from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.csv import read_flat_csvs
from cdsobs.service_definition.api import get_service_definition


def test_read_flat_csvs(test_config):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = get_service_definition(test_config, dataset_name)
    df = read_flat_csvs(
        dataset_name,
        test_config,
        service_definition,
        "CUON",
        time_space_batch=TimeSpaceBatch(TimeBatch(2001, 9), "global"),
        input_dir="data/csv_data/*",
        separator="|",
    )
    print(df)
