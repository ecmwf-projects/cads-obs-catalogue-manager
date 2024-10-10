from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import List, Optional

import fsspec
import pandas
import xarray

from cdsobs.cdm.check import (
    CdmTableFieldsMapping,
    check_for_ambiguous_fields,
    check_table_cdm_compliance,
)
from cdsobs.cdm.code_tables import CDMCodeTable, CDMCodeTables
from cdsobs.cdm.tables import CDMTables
from cdsobs.ingestion.core import (
    DatasetMetadata,
    DatasetPartition,
    PartitionParams,
    SerializedPartition,
)
from cdsobs.service_definition.service_definition_models import (
    SourceDefinition,
)
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.utils import get_code_mapping, unique

logger = get_logger(__name__)


@dataclass
class CdmDataset:
    """In memory representation of the Partition data."""

    dataset: pandas.DataFrame
    partition_params: PartitionParams
    dataset_params: DatasetMetadata


@dataclass
class CdmDatasetNormalized:
    """In memory representation of the Partition data."""

    dataset: dict[str, pandas.DataFrame]
    partition_params: PartitionParams
    dataset_metadata: DatasetMetadata


def to_cdm_dataset(partition: DatasetPartition) -> CdmDataset:
    """
    Map a partition to a python object.

    Maps a partition to a python object
    representing the Common Data Model for observations.

    Parameters
    ----------
    partition :
      Input partition to map.

    cdm_tables :
      Mapping with the tables of the CDM-OBS. Use read_cdm_tables() to get it.

    Returns
    -------
    Dict mapping CDM tables to pandas.DataFrame objects.
    """
    cdm_tables = partition.dataset_metadata.cdm_tables
    cdm_variables = get_cdm_fields(cdm_tables)

    cdm_variables = unique([v for v in cdm_variables if v in partition.data])
    data = partition.data.loc[:, cdm_variables].set_index("observation_id")
    original_variables = set(partition.data.columns)
    removed_variables = original_variables - set(cdm_variables)
    if len(removed_variables) > 0:
        logger.warning(
            "The following variables where read but are not in the CDM and "
            f"are going to be dropped: {removed_variables}"
        )
    return CdmDataset(data, partition.partition_params, partition.dataset_metadata)


def get_cdm_fields(cdm_tables: CDMTables) -> list[str]:
    """Return a list with all CDM fields.

    Do not confuse with variables, whish are defined in the observed_variable.csv
    """
    cdm_variables = list(
        chain.from_iterable([tobj.fields for tname, tobj in cdm_tables.items()])
    )
    cdm_variables_with_table_names = list(
        chain.from_iterable(
            [
                [f + "|" + tname for f in tobj.fields]
                for tname, tobj in cdm_tables.items()
            ]
        )
    )
    cdm_variables += cdm_variables_with_table_names
    if "uncertainty_table" in cdm_tables:
        n_uncertainties = 17
        vars_supporting_numbers = [
            "uncertainty_type",
            "uncertainty_units",
            "uncertainty_value",
        ]
        numbered_fields = [
            v + str(n)
            for v in vars_supporting_numbers
            for n in range(1, n_uncertainties + 1)
        ]
        cdm_variables += numbered_fields
    return cdm_variables


def to_cdm_dataset_normalized(
    partition: DatasetPartition, cdm_tables: CDMTables
) -> CdmDatasetNormalized:
    """Map a partition to a python object.

     Maps a partition to a python object
    representing the Common Data Model for observations.

    Parameters
    ----------
    partition :
      Input partition to map.

    cdm_tables :
      Mapping with the tables of the CDM-OBS. Use read_cdm_tables() to get it.

    Returns
    -------
    Dict mapping CDM tables to pandas.DataFrame objects.
    """
    # Get the variables
    dataset_dict: dict[str, pandas.DataFrame] = {
        name: _extract_cdm_table_fields(name, cdm_tables, partition.data)
        for name in cdm_tables
    }
    # Filter empty tables
    dataset_dict_filtered = {
        name: dataset for name, dataset in dataset_dict.items() if len(dataset) >= 1
    }
    return CdmDatasetNormalized(
        dataset_dict_filtered, partition.partition_params, partition.dataset_metadata
    )


def _extract_cdm_table_fields(
    name: str, cdm_tables: CDMTables, partition_data: pandas.DataFrame
) -> pandas.DataFrame:
    cdm_table = cdm_tables[name]
    # Get the variables corresponding to this table. Exclude names that are not unique
    # in the CDM, like "type".
    vars_in_data = [v for v in partition_data if v in cdm_table.fields]
    vars_with_slash = [
        v for v in partition_data if "|" in str(v) and str(v).split("|")[1] == name
    ]
    table_variables = vars_in_data + vars_with_slash
    # Skip empty tables or tables with only primary keys and not data
    if len(table_variables) > 0 and not set(table_variables).issubset(
        cdm_table.primary_keys
    ):
        table_data = partition_data[table_variables]
        table_data = table_data.rename(
            {v: v.split("|")[0] for v in vars_with_slash}, axis=1
        )
        table_data = _add_foreign_keys(name, table_data, cdm_tables, partition_data)
        table_data = _add_external_names(name, table_data, cdm_tables, partition_data)
        table_data = _drop_duplicates_and_set_index(name, table_data, cdm_tables)
    else:
        table_data = pandas.DataFrame()
    return table_data


def _add_foreign_keys(name, table_data, cdm_tables, partition_data):
    # Check if the external names of the foreign keys in this table are available and
    # add them
    table_definition = cdm_tables[name]
    for foreign_key in table_definition.foreign_keys:
        if foreign_key.external_name in partition_data and (
            foreign_key.name not in table_data
        ):
            table_data = table_data.assign(
                **{foreign_key.name: partition_data[foreign_key.external_name]}
            )
    return table_data


def _add_external_names(
    name: str,
    table_data: pandas.DataFrame,
    cdm_tables: CDMTables,
    partition_data: pandas.DataFrame,
) -> pandas.DataFrame:
    # Check if the fields in this table are the external names in foreign keys of other
    # tables and are available, then add them.
    table_definition = cdm_tables[name]
    for foreign_key in cdm_tables.all_foreign_keys:
        if (
            (foreign_key.external_name in table_definition.fields)
            and (foreign_key.name in partition_data)
            and (foreign_key.external_name not in table_data)
        ):
            table_data = table_data.assign(
                **{foreign_key.external_name: partition_data[foreign_key.name]}
            )
    return table_data


def _drop_duplicates_and_set_index(
    name: str, table_data: pandas.DataFrame, cdm_tables: CDMTables
) -> pandas.DataFrame:
    index = cdm_tables[name].primary_keys[0]
    if index in table_data:
        cdm_table_dataset = table_data.drop_duplicates().set_index(index)
    else:
        # If primary key is not present, return empry frame.
        cdm_table_dataset = pandas.DataFrame()
    return cdm_table_dataset


def open_serialized_partition(serialized_partition: SerializedPartition) -> CdmDataset:
    dataset_dict = open_netcdf(
        serialized_partition.file_params.local_temp_path, decode_variables=True
    )
    cdm_dataset = CdmDataset(
        dataset_dict,
        serialized_partition.partition_params,
        serialized_partition.dataset_metadata,
    )
    return cdm_dataset


def open_netcdf(
    cdm_netcdf: Path | str, decode_variables: bool = False
) -> pandas.DataFrame:
    with xarray.open_dataset(cdm_netcdf, decode_times=True) as dataset:
        data = dataset.load().to_dataframe()
        if decode_variables:
            code2var = get_code_mapping(dataset, inverse=True)
            data["observed_variable"] = data["observed_variable"].map(code2var)
    return data


def open_asset(cdm_netcdf: str, decode_variables: bool = False) -> xarray.Dataset:
    logger.debug(f"Downloading {cdm_netcdf} to memory.")
    fs = fsspec.filesystem("https")
    fobj = fs.open(cdm_netcdf)
    logger.debug("Reading file with xarray.")
    # xarray won't read bytes object directly with netCDF4
    asset_data = xarray.open_dataset(fobj, decode_times=True, engine="h5netcdf")
    if decode_variables:
        code2var = get_code_mapping(asset_data, inverse=True)
        asset_data["observed_variable"] = (
            asset_data["observed_variable"].to_pandas().map(code2var)
        )
    return asset_data


def read_cdm_code_table(cdm_tables_location: Path, name: str) -> CDMCodeTable:
    table_path = Path(cdm_tables_location, f"common_data_model/tables/{name}.dat")
    table_data = pandas.read_csv(
        table_path,
        delimiter="\t",
        quoting=3,
        dtype=str,
        na_filter=False,
        comment="#",
        index_col=0,
    )
    return CDMCodeTable(name, table_data)


def read_cdm_code_tables(
    cdm_tables_location: Path, tables_to_use: Optional[List[str]] = None
) -> CDMCodeTables:
    # These are the tables we are using for the moment by default. Other list can
    # be optinally specified
    default_cdm_tables_to_use = [
        "crs",
        "station_type",
        "observed_variable",
        "units",
        "z_coordinate_type",
    ]
    if tables_to_use is None:
        cdm_tables_to_use = default_cdm_tables_to_use
    else:
        cdm_tables_to_use = tables_to_use
    cdm_tables_dict = {
        table: read_cdm_code_table(cdm_tables_location, table)
        for table in cdm_tables_to_use
    }
    cdm_tables = CDMCodeTables(cdm_tables_dict)
    return cdm_tables


def check_cdm_compliance(
    homogenised_data: pandas.DataFrame,
    cdm_tables: CDMTables,
):
    """Run a set of sanity checks on the input data.

    Run a set of sanity checks on the input data to ensure it is compliant with
    the Observations Common Data Model. Currently it will print warnings, but in the
    future it will be more strict and raise exceptions.

    Parameters
    ----------
    homogenised_data :
      Data table with all the input data.
    cdm_tables :
      CDM tables loaded from their definitions (use read_cdm_tables for this).

    """
    logger.info("Checking for compliance with the Observations CDM.")
    check_for_ambiguous_fields(cdm_tables, homogenised_data)
    table_field_mappings: dict[str, CdmTableFieldsMapping] = {}
    for table_name, table_def in cdm_tables.items():
        table_field_mapping = check_table_cdm_compliance(
            cdm_tables, homogenised_data, table_def
        )
        table_field_mappings[table_name] = table_field_mapping
    return table_field_mappings


def apply_unit_changes(
    homogenised_data: pandas.DataFrame,
    source_definition: SourceDefinition,
    cdm_variable_table: CDMCodeTable,
):
    """Apply unit changes defined in the service_definition.json.

    It also compares them against those defined in the CDM observed_variable table.
    """
    variable_table = cdm_variable_table.table.copy()
    variable_table["name"] = variable_table["name"].str.strip().str.replace(" ", "_")
    varname2units = variable_table[["name", "units"]].set_index("name")
    unit_changes = source_definition.cdm_mapping.unit_changes
    homogenised_data["units"] = ""
    homogenised_data["original_units"] = ""
    for variable in homogenised_data.observed_variable.unique():
        _extract_variable_units_change(
            homogenised_data, source_definition, unit_changes, variable, varname2units
        )

    return homogenised_data


def _extract_variable_units_change(
    homogenised_data, source_definition, unit_changes, variable, varname2units
):
    observed_variable = homogenised_data.loc[:, "observed_variable"]
    description_units = source_definition.descriptions[variable].units
    observed_variable_mask = observed_variable == variable
    if unit_changes is None or variable not in unit_changes:
        # Do not change units, set both units columns to be equal.
        new_units = description_units
        original_units = description_units
    else:
        unit_change = unit_changes[variable]
        new_units = list(unit_change.names.values())[0]
        if new_units != description_units:
            raise RuntimeError(
                "New units set in CDM mapping section must agree"
                "with the ones in the description section."
            )
        try:
            _check_cdm_units(new_units, variable, varname2units)
            logger.info(
                f"Changing units for variable {variable} according to service definition."
            )
        except KeyError:
            logger.warning(
                f"Variable {variable} is not defined in the CDM so we"
                f"cannot check if the variables used are CDM compliant."
            )
        homogenised_data.loc[observed_variable_mask, "observation_value"] = (
            homogenised_data.loc[observed_variable_mask, "observation_value"]
            * unit_change.scale
            + unit_change.offset
        )
        original_units = list(unit_change.names.keys())[0]
    # Assign to the units columns
    homogenised_data.loc[observed_variable_mask, "units"] = new_units
    homogenised_data.loc[observed_variable_mask, "original_units"] = original_units


def _check_cdm_units(new_units, variable, varname2units):
    cdm_units = varname2units.loc[variable, "units"]
    if isinstance(cdm_units, pandas.Series):
        # This is for the case when there are two enties with the same name
        # (air temperatura)
        cdm_units = cdm_units.unique().tolist()
    else:
        cdm_units = [
            cdm_units,
        ]
    if new_units not in cdm_units:
        logger.warning(
            f"New units ({new_units}) are different to the units"
            f"defined in the CDM table ({cdm_units})"
        )
