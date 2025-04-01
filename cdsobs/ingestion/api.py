from hashlib import sha1
from importlib import import_module
from pathlib import Path
from pprint import pformat
from typing import List

import numpy
import pandas
from sqlalchemy.orm import Session

from cdsobs.cdm.api import (
    read_cdm_code_table,
)
from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.core import (
    DatasetMetadata,
    DatasetPartition,
    DatasetReaderFunctionCallable,
    TimeSpaceBatch,
)
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.service_definition.service_definition_models import (
    MeltColumns,
    ServiceDefinition,
    SourceDefinition,
    UncertaintyColumn,
    UncertaintyType,
)
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def join_header_and_data(
    header: pandas.DataFrame,
    data: pandas.DataFrame,
) -> pandas.DataFrame:
    """Join the header and data tables."""
    data_joined = header.merge(
        data,
        left_on="report_id",
        right_on="report_id",
        how="inner",
        suffixes=("_HEADERTABLE", "_DATATABLE"),
    )
    header_duplicated = data_joined.columns[
        data_joined.columns.str.contains("_HEADERTABLE")
    ]
    # Handle columns that appear both in the header and in the data table
    for headercol in header_duplicated:
        datacol = headercol.replace("_HEADERTABLE", "_DATATABLE")
        original_col = headercol.replace("_HEADERTABLE", "")
        if data_joined[headercol].equals(data_joined[datacol]):
            data_joined = data_joined.rename({headercol: original_col}, axis=1).drop(
                datacol, axis=1
            )
        else:
            raise RuntimeError(
                f"Different fields with the same name found in header and data "
                f"({original_col})"
            )
    return data_joined


def validate_and_homogenise(
    data: pandas.DataFrame,
    service_definition: ServiceDefinition,
    source: str,
) -> pandas.DataFrame:
    """Validate and homogeneise the input data.

    Validates and homogeneises the input data according to what's defined in the
    service definition file.

    Parameters
    ----------
    data :
      Data table.
    service_definition :
      Service definition object with the dataset configuration.
    source :
      Source of data type from this dataset to process.

    Returns
    -------
      Table with the header and data tables joined.
    """
    source_definition = service_definition.sources[source]
    # The first step is renaming the variables and the data types
    if source_definition.cdm_mapping.rename is not None:
        data_renamed = data.rename(
            source_definition.cdm_mapping.rename, axis=1, copy=False
        )
    else:
        data_renamed = data
    # Add z coordinate if needed
    if (
        "z_coordinate" not in data_renamed.columns
        and source_definition.space_columns is not None
        and source_definition.space_columns.z is not None
    ):
        z_column = source_definition.space_columns.z
        logger.info(f"Using {z_column} to define z_coordinate")
        # We copy it so the original can still be melted as a main_variable.
        data_renamed["z_coordinate"] = data_renamed.loc[:, z_column].copy()
        zcol2zcoordtype = dict(altitude=0, pressure=1)
        data_renamed["z_coordinate_type"] = zcol2zcoordtype[z_column]
        data_renamed["z_coordinate_type"] = data_renamed["z_coordinate_type"].astype(
            "int"
        )

        # Check mandatory columns are present
    check_mandatory_columns(data_renamed, source_definition)
    # Cast data types to those specified in Service Definition file.
    cast_to_descriptions(data_renamed, source_definition)
    # It is possible to fill a certain variable with a single value, for example to
    # specify the z_coordinate_type
    if source_definition.cdm_mapping.add is not None:
        for name, value in source_definition.cdm_mapping.add.items():
            data_renamed[name] = value
    return data_renamed


def check_mandatory_columns(
    data_renamed: pandas.DataFrame, source_definition: SourceDefinition
):
    if source_definition.is_multitable():
        join_ids = source_definition.join_ids
        assert join_ids is not None
        indices = [join_ids.header, join_ids.data]
        mandatory_columns = set(
            filter(
                lambda col: col not in indices,
                source_definition.mandatory_columns,
            )
        )
    else:
        mandatory_columns = set(source_definition.mandatory_columns)
    all_columns = set(data_renamed.columns)
    missing_mandatory_columns = mandatory_columns.difference(all_columns)
    if len(missing_mandatory_columns) > 0:
        logger.warning(
            f"Mandatory columns {pformat(missing_mandatory_columns)} are missing"
        )


def cast_to_descriptions(
    data_renamed: pandas.DataFrame, source_definition: SourceDefinition
) -> pandas.DataFrame:
    # Finally cast according to the descriptions
    for colname, desc in source_definition.descriptions.items():
        if colname in data_renamed and desc.dtype is not None:
            final_dtype = desc.dtype
            if final_dtype == "object":
                # pandas dtypes behave in annoying ways. Here we have to compare
                # with object but cast to string so we do not end up with
                # "integer" objects that will be casted back to int64 undexpectedly
                # when calling reset_index().
                data_renamed[colname] = (
                    data_renamed[colname].astype("string").fillna("null")
                )
            else:
                if str(data_renamed[colname].dtype) != final_dtype:
                    try:
                        data_renamed[colname] = data_renamed[colname].astype(
                            final_dtype
                        )
                    except ValueError:
                        logger.error(f"Cannot cast {colname} to {final_dtype}")
                        raise
    return data_renamed


def sort(partition: DatasetPartition) -> DatasetPartition:
    """Sort data of a partition."""
    logger.info("Sorting partition data")
    space_columns = partition.dataset_metadata.space_columns
    latname = space_columns.y
    lonname = space_columns.x
    partition.data.sort_values(
        by=["report_timestamp", latname, lonname], kind="mergesort", inplace=True
    )
    return partition


def read_batch_data(
    config: CDSObsConfig,
    dataset_params: DatasetMetadata,
    service_definition: ServiceDefinition,
    time_space_batch: TimeSpaceBatch,
) -> pandas.DataFrame:
    """
    Read the data as a single big table with the names remmaped from service_definition.

    Different readers can be used for each dataset. These are configured in the
    configuration yaml.

    Parameters
    ----------
    config :
      Configuration of the CDSOBS catalogue manager
    dataset_params :
      Object containing information about the dataset.
    service_definition :
      Object produced parsing the service_definition.json.
    time_space_batch:
      Read data only for one year and month.

    Returns
    -------
    A single, big table containing all the data found for this time_batch.

    """
    dataset_name = dataset_params.name
    source = dataset_params.dataset_source
    logger.info(f"Reading ingestion tables for {dataset_name=} {source=}")
    # Read the data as a flat table
    main_reader_function = _get_reader(config, dataset_name, source)
    reader_extra_args = config.get_dataset(dataset_name).reader_extra_args
    reader_extra_args = reader_extra_args if reader_extra_args is not None else {}
    data_table = main_reader_function(
        dataset_name,
        config,
        service_definition,
        source,
        time_space_batch,
        **reader_extra_args,
    )
    # Check of there is data for this time batch
    if len(data_table) == 0:
        raise EmptyBatchException
    logger.info("Validating and homogenising data tables")
    homogenised_data = validate_and_homogenise(data_table, service_definition, source)
    # Explicitly remove this reference to reduce memory usage
    del data_table
    source_definition = service_definition.sources[source]
    if source_definition.cdm_mapping.melt_columns is not None:
        logger.info("Melting variable columns as requested")
        homogenised_data = _melt_variables(
            homogenised_data,
            dataset_params.variables,
            source_definition.cdm_mapping.melt_columns,
            config.cdm_tables_location,
        )
    return homogenised_data


class EmptyBatchException(Exception):
    pass


def _entry_exists(
    dataset_name: str,
    session: Session,
    source: str,
    time_space_batch: TimeSpaceBatch,
    version: str = "1.0.0",
) -> bool:
    """Return True if any data exists in the catalogue for a given time_batch."""
    time_start, time_end = time_space_batch.get_time_coverage()
    lon_start, lon_end, lat_start, lat_end = time_space_batch.get_spatial_coverage()
    entry_exists = CatalogueRepository(session).entry_exists(
        dataset_name,
        source,
        time_start,
        time_end,
        lon_start,
        lon_end,
        lat_start,
        lat_end,
        version,
    )
    return entry_exists


def _get_reader(
    config: CDSObsConfig, dataset_name: str, source: str
) -> DatasetReaderFunctionCallable:
    """Return the reader function by parsing it from the config file."""
    reader_conf = config.get_dataset(dataset_name).reader
    # Support a different reader for each source (for IGRA and IGRA_H).
    if isinstance(reader_conf, str):
        import_str = reader_conf
    else:
        import_str = reader_conf[source]
    import_str_list = import_str.split(".")
    module_str = ".".join(import_str_list[:-1])
    function_name = import_str_list[-1]
    module = import_module(module_str)
    return getattr(module, function_name)


def hash_string(value):
    return sha1(str(value).encode("utf-8")).hexdigest()


def _melt_variables(
    homogenised_data: pandas.DataFrame,
    variables: List[str],
    melt_columns: MeltColumns,
    cdm_tables_location: Path,
) -> pandas.DataFrame:
    """Melt variables if necessary (like WOUDC dataset).

    This will collapse the variable colums to two columns, one containing the data and
    another containing the variables. Then the variables are replaced by the variable
    codes defined in the observed_variable CDM table. This will fail if the variable is
    not in the table.
    """
    id_vars = [col for col in homogenised_data.columns if col not in variables]
    homogenised_data_melted = homogenised_data.melt(
        id_vars=id_vars,
        value_vars=variables,
        var_name="observed_variable",
        value_name="observation_value",
    )
    # New observation id unique for each observation value
    if "observation_id" not in homogenised_data_melted:
        logger.info("Adding new observation id (only unique for this chunk)")
        homogenised_data_melted["observation_id"] = homogenised_data_melted.index
    # Handle auxiliary variables
    homogenised_data_melted = _handle_aux_variables(
        melt_columns, cdm_tables_location, homogenised_data_melted
    )

    # Encode observed_variables
    logger.info("Encoding observed variables using the CDM variable codes.")
    code_table = read_cdm_code_table(cdm_tables_location, "observed_variable").table
    # strip to remove extra spaces
    code_dict = pandas.Series(
        index=code_table["name"].str.strip().str.replace(" ", "_"),
        data=code_table.index,
    ).to_dict()
    # Check for variables not in the code table
    cdm_vars = set(code_dict)
    not_found = set(homogenised_data_melted["observed_variable"].unique()) - cdm_vars
    if len(not_found) > 0:
        logger.warning(f"Some variables were not found in the CDM: {not_found}")
    return homogenised_data_melted


def _handle_aux_variables(
    melt_columns: MeltColumns,
    cdm_tables_location: Path,
    homogenised_data_melted: pandas.DataFrame,
) -> pandas.DataFrame:
    uncertainty_type_table = read_cdm_code_table(
        cdm_tables_location, "uncertainty_type"
    ).table
    logger.info("Aligning auxiliary variables with melted ones")
    if melt_columns.uncertainty is not None:
        homogenised_data_melted = _add_uncertainty_fields(
            melt_columns.uncertainty, homogenised_data_melted, uncertainty_type_table
        )
    # Add quality flags
    if melt_columns.quality_flag is not None:
        homogenised_data_melted["quality_flag"] = 3
        for qf_col in melt_columns.quality_flag["quality_flag"]:
            var_mask = (
                homogenised_data_melted["observed_variable"] == qf_col.main_variable
            )
            var_quality_flag = homogenised_data_melted.loc[var_mask, qf_col.name]
            homogenised_data_melted.loc[var_mask, "quality_flag"] = var_quality_flag
            homogenised_data_melted = homogenised_data_melted.drop(qf_col.name, axis=1)
            # Ensure is int and fill nans with 3 (missing according to the CDM)
        homogenised_data_melted["quality_flag"] = (
            homogenised_data_melted["quality_flag"].fillna(3).astype("uint8")
        )
    # Add processing level
    if melt_columns.processing_level:
        homogenised_data_melted["processing_level"] = 6
        for pl_col in melt_columns.processing_level["processing_level"]:
            var_mask = (
                homogenised_data_melted["observed_variable"] == pl_col.main_variable
            )
            var_processing_level = homogenised_data_melted.loc[var_mask, pl_col.name]
            homogenised_data_melted.loc[
                var_mask, "processing_level"
            ] = var_processing_level
            homogenised_data_melted = homogenised_data_melted.drop(pl_col.name, axis=1)
        homogenised_data_melted["processing_level"] = homogenised_data_melted[
            "processing_level"
        ].astype("uint8")
    return homogenised_data_melted


def _add_uncertainty_fields(
    uncertainty_fields: dict[UncertaintyType, list[UncertaintyColumn]],
    homogenised_data_melted,
    uncertainty_type_table,
):
    for unc_type, unc_cols in uncertainty_fields.items():
        unc_type_code = uncertainty_type_table.loc[
            uncertainty_type_table.loc[:, "name"]
            == unc_type.replace("_uncertainty", "").replace("_", " ")
        ].index.item()
        uncertainty_value_name = f"uncertainty_value{unc_type_code}"
        uncertainty_type_name = f"uncertainty_type{unc_type_code}"
        uncertainty_units_name = f"uncertainty_units{unc_type_code}"
        homogenised_data_melted[uncertainty_value_name] = numpy.nan
        homogenised_data_melted[uncertainty_type_name] = unc_type_code
        homogenised_data_melted[uncertainty_type_name] = homogenised_data_melted[
            uncertainty_type_name
        ].astype("uint8")
        homogenised_data_melted[uncertainty_units_name] = "NA"

        for unc_col in unc_cols:
            var = unc_col.main_variable
            var_mask = homogenised_data_melted["observed_variable"] == var
            # Fill the columns
            homogenised_data_melted.loc[
                var_mask, uncertainty_value_name
            ] = homogenised_data_melted.loc[var_mask, unc_col.name]
            homogenised_data_melted.loc[var_mask, uncertainty_type_name] = unc_type_code
            homogenised_data_melted.loc[
                var_mask, uncertainty_units_name
            ] = unc_col.units
            homogenised_data_melted = homogenised_data_melted.drop(unc_col.name, axis=1)

        homogenised_data_melted[uncertainty_value_name] = homogenised_data_melted[
            uncertainty_value_name
        ].astype("float32")
    return homogenised_data_melted
