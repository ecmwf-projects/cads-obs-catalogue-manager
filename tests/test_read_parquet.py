import importlib
from pathlib import Path

from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.parquet import read_flat_parquet
from cdsobs.service_definition.api import get_service_definition


def test_read_flat_parquet(test_config, tmp_path):
    dataset_name = "insitu-observations-surface-land"
    service_definition = get_service_definition(test_config, dataset_name)
    test_data_path = Path(
        str(importlib.resources.files("tests")), "data", "parquet_data"
    )
    df = read_flat_parquet(
        dataset_name,
        test_config,
        service_definition,
        "hourly",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_path=f"{test_data_path}/*.parquet",
    )
    assert df.size > 0
    # Test input pattern
    df = read_flat_parquet(
        dataset_name,
        test_config,
        service_definition,
        "hourly",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_path=f"{test_data_path}",
        filename_pattern="test_{dataset_name}_{source}_{year}_{month}.parquet",
    )
    assert df.size > 0
