from pathlib import Path

import duckdb
import pandas

from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.api import EmptyBatchException
from cdsobs.ingestion.core import TimeSpaceBatch
from cdsobs.retrieve.filter_datasets import get_var_code_dict
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def read_flat_parquet(
    dataset_name: str,
    config: CDSObsConfig,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
    input_path: str,
    filename_pattern: str | None = None,
) -> pandas.DataFrame:
    """
    Read parquet files.

    Parameters
    ----------
    dataset_name: str
      Name of the dataset.
    config: CDSObsConfig
      Main configuration object.
    service_definition: ServiceDefinition
      Service Definition object.
    source: str
      Dataset source
    time_space_batch: TimeSpaceBatch
      Time and space slice to read.
    input_path: str
      Directory where the input files are located. Expansion patterns can be used, for
      example /data/*.pq.
    filename_pattern: optional, str, default is None
      If provided, it will be expanded replacing the following keys: dataset_name,
      source, year and month. For example {dataset_name}_{source}_{year}_{month}.nc.

    Returns
    -------
    pandas.DataFrame

    """
    if time_space_batch.space_batch != "global":
        logger.warning("This reader does not support subsetting in space.")
    time_batch = time_space_batch.time_batch
    if filename_pattern is not None:
        input_files_pattern = Path(
            input_path,
            input_path.format(
                dataset_name=dataset_name,
                source=source,
                year=time_batch.year,
                month=time_batch.month,
            ),
        )
    else:
        input_files_pattern = Path(input_path)
    start, end = time_batch.get_time_coverage()
    data = duckdb.sql(
        f"""
        SET TimeZone = 'UTC';
        SELECT * FROM read_parquet('{input_files_pattern}')
        WHERE
          report_timestamp >= '{start}' AND report_timestamp < '{end}'
        """
    ).fetchdf()

    if len(data) == 0:
        raise EmptyBatchException
    # Decode variable names
    code_dict = get_var_code_dict(config.cdm_tables_location)
    data["observed_variable"] = data["observed_variable"].map(code_dict)
    # Remove timezone information, that gives problems, all input must be in UTC
    for col in data:
        if data[col].dtype.kind == "M":
            data[col] = data[col].dt.tz_localize(None)
    return data
