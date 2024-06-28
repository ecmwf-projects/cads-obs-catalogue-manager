from importlib import import_module
from pathlib import Path
from typing import List, Tuple

import numpy
import pandas
import pandas as pd
from sqlalchemy.orm import Session

from cdsobs.cdm.api import (
    AuxFields,
    get_aux_fields_mapping_from_service_definition,
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
    ServiceDefinition,
    SourceDefinition,
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
        logger.warning(f"Mandatory columns {missing_mandatory_columns} are missing")


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


def _get_latlon_names(data: pandas.DataFrame) -> Tuple[str, str]:
    if "longitude" in data:
        latname = "latitude"
        lonname = "longitude"
    else:
        latname = "latitude|station_configuration"
        lonname = "longitude|station_configuration"
    return latname, lonname


def sort(partition: DatasetPartition) -> DatasetPartition:
    """Sort data of a partition."""
    logger.info("Sorting partition data")
    latname, lonname = _get_latlon_names(partition.data)
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
    logger.info("Reading auxiliary fields from configuration.")
    aux_variables = get_aux_fields_mapping_from_service_definition(
        source_definition, dataset_params.variables
    )
    logger.info(f"The following auxiliary fields were found {aux_variables}")
    if source_definition.cdm_mapping.melt_columns:
        logger.info("Melting variable columns as requested")
        homogenised_data = _melt_variables(
            homogenised_data,
            dataset_params.variables,
            aux_variables,
            config.cdm_tables_location,
        )
    return homogenised_data


class EmptyBatchException(Exception):
    pass


def _entry_exists(
    dataset_name: str, session: Session, source: str, time_space_batch: TimeSpaceBatch
) -> bool:
    """Return True if any data exists in the catalogue for a given time_batch."""
    entry_exists = CatalogueRepository(session).entry_exists(
        dataset_name,
        source,
        *time_space_batch.get_time_coverage(),
        *time_space_batch.get_spatial_coverage(),
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


def _melt_variables(
    homogenised_data: pandas.DataFrame,
    variables: List[str],
    aux_fields: AuxFields,
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
    ).rename(dict(observation_id="original_observation_id"), copy=False)
    # New observation id unique for each observation value
    logger.info("Adding new observation id (only unique for this chunk)")
    homogenised_data_melted = homogenised_data_melted.assign(
        observation_id=pd.RangeIndex(0, len(homogenised_data_melted))
    )
    # Handle auxiliary variables
    dataset_has_aux_vars = len(aux_fields) > 0
    if dataset_has_aux_vars:
        logger.info("Aligning auxiliary variables with melted ones")
        for var in aux_fields.vars_with_uncertainty_field:
            var_mask = homogenised_data_melted["observed_variable"] == var
            for unc_field in aux_fields.get_var_uncertainty_field_names(var):
                # We cannot assume that the auxiliary field is {var}_{uncertainty_type}
                # We do it right and extract it from the description
                uncertainty_type = aux_fields.auxfield2metadata_name(var, unc_field)
                if uncertainty_type not in homogenised_data_melted.columns:
                    homogenised_data_melted[uncertainty_type] = numpy.nan
                homogenised_data_melted.loc[
                    var_mask, uncertainty_type
                ] = homogenised_data_melted.loc[var_mask, unc_field]
                homogenised_data_melted = homogenised_data_melted.drop(
                    unc_field, axis=1
                )

        # Add quality flags
        homogenised_data_melted["quality_flag"] = 3.0
        vars_with_qf = aux_fields.vars_with_quality_field
        for var in vars_with_qf:
            var_mask = homogenised_data_melted["observed_variable"] == var
            flag_name = aux_fields.get_var_quality_flag_field_name(var)
            var_quality_flag = homogenised_data_melted.loc[var_mask, flag_name]
            homogenised_data_melted.loc[var_mask, "quality_flag"] = var_quality_flag
            homogenised_data_melted = homogenised_data_melted.drop(flag_name, axis=1)

        # Ensure is int and fill nans with 3 (missing according to the CDM)
        homogenised_data_melted["quality_flag"] = (
            homogenised_data_melted["quality_flag"].fillna(3).astype("int")
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
    logger.warning(f"Some variables were not found in the CDM: {not_found}")
    return homogenised_data_melted
