from pathlib import Path

import pandas

from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.parquet import read_flat_parquet
from cdsobs.service_definition.api import get_service_definition


def test_read_flat_parquet(test_config, tmp_path):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = get_service_definition(test_config, dataset_name)
    input_path_csv = "tests/data/csv_data/cdm_core_r7.0_ZIM00067976.psv"
    pandas.read_csv(input_path_csv, sep="|").to_parquet(Path(tmp_path, "test.pq"))
    df = read_flat_parquet(
        dataset_name,
        test_config,
        service_definition,
        "CUON",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_path=f"{tmp_path}/*.pq",
    )
    print(df)
