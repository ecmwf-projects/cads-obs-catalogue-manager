from pathlib import Path

import pandas

from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.parquet import read_flat_parquet
from cdsobs.service_definition.api import get_service_definition


def test_read_flat_parquet(test_config, tmp_path):
    dataset_name = "insitu-observations-surface-land"
    service_definition = get_service_definition(test_config, dataset_name)
    input_path_csv = "tests/data/csv_data/cdm_core_r7.0_ZIM00067976.psv"
    parquet_path = Path(tmp_path, "test.pq")
    pandas.read_csv(input_path_csv, sep="|").to_parquet(parquet_path)
    df = read_flat_parquet(
        dataset_name,
        test_config,
        service_definition,
        "hourly",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_path=f"{tmp_path}/*.pq",
    )
    assert df.size > 0
    # Test input pattern
    parquet_path_pattern = Path(tmp_path, f"test_{dataset_name}_hourly_2001_9.parquet")
    parquet_path.rename(parquet_path_pattern)
    df = read_flat_parquet(
        dataset_name,
        test_config,
        service_definition,
        "hourly",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_path=f"{tmp_path}",
        filename_pattern="test_{dataset_name}_{source}_{year}_{month}.parquet",
    )
    assert df.size > 0
