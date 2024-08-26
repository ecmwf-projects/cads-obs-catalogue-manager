from functools import partial

import pandas
from dask.dataframe import DataFrame as DaskDataFrame

from cdsobs.cdm.tables import CDMTable, CDMTables
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def denormalize_tables(
    cdm_tables: CDMTables,
    dataset_cdm: dict[str, pandas.DataFrame],
    tables_to_use: list[str],
    ignore_errors: bool = False,
) -> pandas.DataFrame:
    """De-normalize joining the tables using pandas."""
    # Reset indices
    dataset_cdm = {name: data.reset_index() for name, data in dataset_cdm.items()}
    # De-normalize joining the tables
    # We only use tables in figure 1 of the CDM specification
    _join_table_pair = partial(join_table_pair, ignore_errors=ignore_errors)
    # We start with the optional tables (we don't have any to test right now)
    for table_name in cdm_tables.optional_tables:
        if table_name in tables_to_use:
            # We join the optional table with the table with the same name without
            # _optional
            parent_table_name = table_name.replace("_optional", "")
            child_table_def = cdm_tables[table_name]
            joined_table = _join_table_pair(
                dataset_cdm, table_name, parent_table_name, child_table_def
            )
            dataset_cdm[parent_table_name] = joined_table
    # Here we merge the tables that have a one to one relationship with the
    # observations table
    for table_name in [
        "era5fb_table",
        "advanced_homogenisation",
        "advanced_uncertainty",
    ]:
        if table_name in tables_to_use:
            if len(dataset_cdm["observations_table"]) != len(dataset_cdm[table_name]):
                logger.warning(
                    f"{table_name} table and observations table have different lengths."
                )
            child_table_def = cdm_tables[table_name]
            dataset_cdm["observations_table"] = join_table_pair(
                dataset_cdm, table_name, "observations_table", child_table_def
            )
    # We continue merging sensor configuration with observations
    if "sensor_configuration" in dataset_cdm:
        observations_table_def = cdm_tables["observations_table"]
        dataset_cdm["observations_table"] = _join_table_pair(
            dataset_cdm,
            "observations_table",
            "sensor_configuration",
            observations_table_def,
        )
    # And then the rest of configuration tables, which are parents of the header
    for config_table_name in cdm_tables.header_configuration_tables:
        if config_table_name in dataset_cdm:
            header_table_def = cdm_tables["header_table"]
            dataset_cdm["header_table"] = _join_table_pair(
                dataset_cdm, "header_table", config_table_name, header_table_def
            )
    # "Fields" tables that are childs of observations table
    for field_table_name in cdm_tables.fields_tables:
        if field_table_name in dataset_cdm:
            field_table_def = cdm_tables[field_table_name]
            dataset_cdm["observations_table"] = _join_table_pair(
                dataset_cdm, field_table_name, "observations_table", field_table_def
            )
    # Finally we merge header (parent) and observations (child)
    denormalized_table = _join_table_pair(
        dataset_cdm,
        "observations_table",
        "header_table",
        cdm_tables["observations_table"],
    )
    return denormalized_table


def join_table_pair(
    dataset_cdm: dict[str, pandas.DataFrame | DaskDataFrame],
    child_table: str,
    parent_table: str,
    child_table_def: CDMTable,
    ignore_errors: bool = False,
) -> pandas.DataFrame:
    """
    Join two CDM tables using pandas.merge.

    Parameters
    ----------
    dataset_cdm :
      A dict with the data tables as pandas dataframes
    child_table :
      Table that references parent table is a foreign key(s).
    parent_table :
      Table with primary key(s) referenced by child table
    child_table_def :
      CDMTable where the relations are defined
    ignore_errors:
      If there is an error, print a warning and return the child table. Default is False.

    Returns
    -------
    Joined table (can be large)

    """
    logger.info(f"Joining {child_table} with {parent_table}")
    parent_table_data = dataset_cdm[parent_table]
    child_table_data = dataset_cdm[child_table]
    # Check if we are using dask dataframes
    if isinstance(parent_table_data, DaskDataFrame):
        merge_funct = DaskDataFrame.merge
    else:
        merge_funct = pandas.merge
    # Filter other foreign keys pointing to other tables
    foreign_keys = [
        fk for fk in child_table_def.foreign_keys if fk.external_table == parent_table
    ]
    left_on = [fk.name for fk in foreign_keys]
    right_on = [fk.external_name for fk in foreign_keys]
    try:
        joined_table = merge_funct(
            child_table_data,
            parent_table_data,
            left_on=left_on,
            right_on=right_on,
            how="inner",
            suffixes=(f"|{child_table}", f"|{parent_table}"),
        )
    except KeyError as e:
        if ignore_errors:
            logger.warning(f"Joining tables failed, missing key {e}")
            return child_table_data
        else:
            raise
    if len(joined_table) == 0:
        logger.warning("Joined table has length 0")
    return joined_table
