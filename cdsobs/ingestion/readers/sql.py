import inspect
from typing import Any, Callable, Protocol, Tuple

import connectorx as cx
import pandas
import pandas as pd

from cdsobs.config import CDSObsConfig, DBConfig
from cdsobs.ingestion.api import join_header_and_data
from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.service_definition.service_definition_models import (
    ServiceDefinition,
    SourceDefinition,
)
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)

REPORT_TIMESTAMP = "report_timestamp"


def read_time_partitioned_tables(
    config: DBConfig,
    source_definition: SourceDefinition,
    time_batch: TimeBatch,
) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    return read_sql_tables(
        config, source_definition, time_batch, time_is_in_data_table=True
    )


def read_sql_tables(
    config: DBConfig,
    source_definition: SourceDefinition,
    time_batch: TimeBatch,
    time_is_in_data_table: bool = False,
) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    """Read data from the SQL tables."""
    # Get the URL to the database
    db_url = config.get_url()

    # Define the time_batch specifics in case it exist
    start, end = time_batch.get_time_coverage()
    time_field, time_field_in_header = get_time_field(source_definition)

    if source_definition.is_multitable():
        join_ids = source_definition.join_ids
        assert join_ids is not None
        # Get the header data
        header_table = source_definition.header_table
        header_querystr = f"SELECT * FROM {header_table}"
        # Time filter
        # Closed left, open right
        header_time_filter_query_str = (
            f" WHERE {time_field} >= '{start}' AND {time_field} < '{end}'"
        )
        if time_field_in_header:
            # Append filters to the simple select query
            header_querystr += header_time_filter_query_str
        # We need sortby to the result to be deterministic
        header_querystr += f" ORDER BY {join_ids.header}"
        # This is to use only one thread in order to no overwhelm the database
        header_data = cx.read_sql(
            db_url, header_querystr, return_type="pandas", partition_num=1
        )

        # Get the data data
        data_table = source_definition.data_table
        data_querystr = f"SELECT d.* FROM {data_table} d"
        # Time filter for data
        if time_is_in_data_table:
            data_time_filter_query_str = header_time_filter_query_str
        else:
            data_time_filter_query_str = (
                f" INNER JOIN {header_table} h "
                f"ON d.{join_ids.data}=h.{join_ids.header} "
                f"where h.{time_field} >= '{start}' and h.{time_field} < '{end}'"
            )

        # Append filters to the simple select query
        data_querystr += data_time_filter_query_str
        # We need order by to the result to be deterministic
        data_querystr += " ORDER BY id"
        # This is to use only one thread in order to no overwhelm the database
        data_data = cx.read_sql(
            db_url, data_querystr, return_type="pandas", partition_num=1
        )
        result = (header_data, data_data)
    else:
        data_table = source_definition.data_table
        data_querystr = f"SELECT * FROM {data_table}"
        # Time filter
        data_time_filter_query_str = (
            f" WHERE {time_field} >= '{start}' AND {time_field} < '{end}'"
        )

        # Append filters to the simple select query
        # Closed left, open right
        data_querystr += data_time_filter_query_str
        # We need order by the result to be deterministic
        order_by_str = _get_order_by(data_table, db_url, source_definition)
        data_querystr += order_by_str
        # This is to use only one thread in order to no overwhelm the database
        data_data = cx.read_sql(
            db_url, data_querystr, return_type="pandas", partition_num=1
        )
        # Return an empty dataframe as header.
        result = (pd.DataFrame(), data_data)
    return result


def _get_order_by(data_table, db_url, source_definition):
    columns = cx.read_sql(db_url, f"select * FROM {data_table} LIMIT 1").columns
    if "id" in columns:
        sort_cols = "id"
    else:
        sort_cols = ", ".join(source_definition.order_by)
    order_by_str = f" ORDER BY {sort_cols}"
    return order_by_str


class SQLReaderFunctionCallable(Protocol):
    def __call__(
        self,
        config: DBConfig,
        source_definition: SourceDefinition,
        time_batch: TimeBatch,
    ) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
        ...


dataset2sqlreader_function: dict[str, SQLReaderFunctionCallable] = {
    "insitu-observations-igra-baseline-network": read_time_partitioned_tables,
    "insitu-observations-gruan-reference-network": read_time_partitioned_tables,
    "insitu-observations-near-surface-temperature-us-climate-reference-network": read_time_partitioned_tables,
    "insitu-observations-gnss": read_time_partitioned_tables,
    "insitu-observations-woudc-ozone-total-column-and-profiles": read_time_partitioned_tables,
    "insitu-observations-ndacc": read_time_partitioned_tables,
}


def read_singletable_data(
    dataset_name: str,
    config: CDSObsConfig,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
) -> pandas.DataFrame:
    """Read datasets formatted as one single table."""
    source_definition = service_definition.sources[source]
    # Get the reader function for this dataset
    ingestion_db_config = config.ingestion_databases[service_definition.ingestion_db]
    sql_reader_function = dataset2sqlreader_function[dataset_name]
    _, data_table = read_ingestion_tables(
        ingestion_db_config,
        source_definition=source_definition,
        sql_reader_function=sql_reader_function,
        time_batch=time_space_batch.time_batch,
    )
    return data_table


def read_header_and_data_tables(
    dataset_name: str,
    config: CDSObsConfig,
    service_definition: ServiceDefinition,
    source: str,
    time_batch: TimeBatch,
) -> pandas.DataFrame:
    """Read datasets formatted as two related header and data tables."""
    source_definition = service_definition.sources[source]
    if not source_definition.is_multitable():
        raise RuntimeError(
            f"This function is for reading header and data tables, "
            f"for {type(source_definition)} use another reader (e.g. read_singletable_data)."
        )
    # Get the reader function for this dataset
    sql_reader_function = dataset2sqlreader_function[dataset_name]
    ingestion_db_config = config.ingestion_databases[service_definition.ingestion_db]
    header_table, data_table = read_ingestion_tables(
        ingestion_db_config,
        source_definition,
        sql_reader_function,
        time_batch,
    )
    # Filter unwanted fields
    header_columns = source_definition.get_raw_header_columns()
    mandatory_columns = source_definition.get_raw_mandatory_columns()
    join_ids = source_definition.join_ids
    assert join_ids is not None
    if source == "Dobson_O3":
        header_table = header_table.drop("date_of_observation", axis=1)
    fields_header = header_columns + [join_ids.header] + mandatory_columns
    fields_header = list(set([f for f in fields_header if f in header_table.columns]))
    header_table = header_table[fields_header]
    # Rename indices
    header_table = header_table.rename({join_ids.header: "report_id"}, axis=1)
    data_table = data_table.rename(
        {"id": "observation_id", join_ids.data: "report_id"}, axis=1
    )
    # Remove some offending fields
    if source == "IGRA" and "version" in data_table:
        # Is all nans and conflicts with the header version
        logger.warning(
            "Deleted version from data table as it is all nans and conflicts"
            "with the header version."
        )
        data_table = data_table.drop("version", axis=1)
    # Join header and data
    if source in ["IGS", "EPN", "IGS_R3"]:
        logger.warning(
            "Deleted idstation from data table as it conflicts with the"
            "idstation in the header table."
        )
        data_table = data_table.drop(columns="idstation")
    if source in ["Brewer_O3"]:
        logger.warning(
            "Deleted date_of_observation from header table as it conflicts with the"
            "idstation in the header table."
        )
        header_table = header_table.drop(columns="date_of_observation")
    data_joined = join_header_and_data(header_table, data_table)
    return data_joined


def read_ingestion_tables(
    config: DBConfig,
    source_definition: SourceDefinition,
    sql_reader_function: SQLReaderFunctionCallable,
    time_batch: TimeBatch,
) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    """
    Read ingestion tables into pandas dataframes.

    A reader function can be injected to read data in different formats. By default it
    read data from SQL databases.

    Parameters
    ----------
    config :
      Configuration of the ingestion database.
    source_definition :
      Section of the service configuration referring to the "source" or data type we
      are reading from this dataset.
    sql_reader_function :
      A function that returns the header and data pandas dataframes
    time_batch:
      For the data table, read data for only this month and year

    Returns
    -------
    A tuple with the header and data tables
    """
    # Read tables
    function_file = get_function_reference(sql_reader_function)
    logger.info(f"Reading ingestion tables with {function_file}")
    if not source_definition.is_multitable():
        header_table_name = None
    else:
        header_table_name = source_definition.header_table
    data_table_name = source_definition.data_table
    header, data = sql_reader_function(config, source_definition, time_batch)
    logger.debug("Fixing data types of the input fields")
    sql_data_types = get_sql_data_types(config, data_table_name, header_table_name)
    header = cast_to_right_types(header, sql_data_types)
    data = cast_to_right_types(data, sql_data_types)
    return header, data


def get_function_reference(sql_reader_function: Callable) -> str:
    """Return function reference as module.name."""
    module = inspect.getmodule(sql_reader_function).__name__  # type: ignore
    name = sql_reader_function.__name__
    return module + "." + name


def get_sql_data_types(
    config: DBConfig, data_table_name: str, header_table_name: str | None
) -> pandas.Series:
    # Ids are renamed, so they fit with the result of the merge. pandas merge renames
    # conflicting colums appending "_x" and "_y" to them.
    header_data_types = pandas.read_sql(
        f"select column_name, data_type from information_schema.columns where "
        f"table_name='{header_table_name}' ORDER BY column_name",
        config.get_url(),
        index_col="column_name",
    )
    data_data_types = pandas.read_sql(
        f"select column_name, data_type from information_schema.columns where "
        f"table_name='{data_table_name}' ORDER BY column_name",
        config.get_url(),
        index_col="column_name",
    )
    data_types_sql = pandas.concat([header_data_types, data_data_types])
    data_types_sql = data_types_sql[~data_types_sql.index.duplicated(keep="first")]
    # if any(~data_types_sql.data_type.isin(sqltype2numpytypes)):
    #     raise RuntimeError("Not all SQL types found can be mapped to numpy")
    # data_types = data_types_sql.replace(dict(data_type=sqltype2numpytypes))
    return data_types_sql.squeeze()


sqltype2numpytypes = {
    "real": "float32",
    "double precision": "float64",
    "date": "datetime64[ns]",
    "integer": "int64",
    "timestamp with time zone": "datetime64[ns]",
    "timestamp without time zone": "datetime64[ns]",
    "character": "object",
    "character varying": "object",
    "text": "object",
    "bigint": "int64",
    "uuid": "object",
    "numeric": "float64",
    "ARRAY": "object",
}


def is_unique(x: Any) -> bool:
    return len(x) == len(set(x))


def invert_dict(idict: dict) -> dict:
    """Return the inverse of a dictionary.

    It must be bijective, this is, keys and values need to be unique
    """
    try:
        assert is_unique(idict)
        assert is_unique(idict.values())
    except AssertionError:
        raise AssertionError("Dict must be bijective (keys and values must be unique.)")
    return {v: k for k, v in idict.items()}


def get_time_field(source_definition: SourceDefinition) -> Tuple[str, bool]:
    rename_dict = source_definition.cdm_mapping.rename
    descriptions = source_definition.descriptions
    time_field_in_header = (
        source_definition.is_multitable()
        and source_definition.header_columns is not None
        and REPORT_TIMESTAMP in source_definition.header_columns
    )

    if REPORT_TIMESTAMP in descriptions:
        if rename_dict is not None and REPORT_TIMESTAMP in rename_dict.values():
            inverse_rename_dict = invert_dict(rename_dict)
            time_field = inverse_rename_dict[REPORT_TIMESTAMP]
        else:
            time_field = REPORT_TIMESTAMP
    else:
        raise RuntimeError("Time field not found in service definition")
    return time_field, time_field_in_header


def cast_to_right_types(
    data_from_sql: pandas.DataFrame, sql_types: pandas.Series
) -> pandas.DataFrame:
    """
    Cast to the right types according to our sql to pandas mapping.

    By default, we get too large types (float64 and int64) and also nulleble types that
    do not have an equivalent in numpy. We fix that here.
    """
    input_dtypes = data_from_sql.dtypes

    for field_name in data_from_sql.columns.tolist():
        # field_name = cast(str, colname)  # Workaround for bug in pandas-stubs
        input_dtype = input_dtypes.loc[field_name]
        sql_type = sql_types.loc[field_name]
        numpy_dtype = sqltype2numpytypes[sql_type]
        if str(input_dtype) != numpy_dtype:
            # Check nullable integers and convert to int64 without nulls
            if (str(input_dtype) == "Int64") and data_from_sql[
                field_name
            ].isnull().any():
                logger.info(
                    f"{field_name} is an integer with nulls, converting nulls to -9999"
                )
                data_from_sql[field_name] = data_from_sql[field_name].fillna(-9999)
            # Cast to the data type that maps better with the original sql type
            logger.debug(
                f"Casting {input_dtype} to {numpy_dtype}  for {field_name} as it is the original data type"
            )
            data_from_sql[field_name] = data_from_sql[field_name].astype(numpy_dtype)
    return data_from_sql
