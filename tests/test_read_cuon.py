import os

from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.cuon import read_cuon_netcdfs
from cdsobs.service_definition.api import get_service_definition


def test_read_cuon(test_config):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    dataset_config = [d for d in test_config.datasets if d.name == dataset_name][0]
    service_definition = get_service_definition(dataset_name)
    time_space_batch = TimeSpaceBatch(TimeBatch(1960, 1))
    os.environ["CADSOBS_AVOID_MULTIPROCESS"] = "True"
    cuon_data = read_cuon_netcdfs(
        dataset_name,
        test_config,
        service_definition,
        "CUON",
        time_space_batch,
        **dataset_config.reader_extra_args,
    )
    assert len(cuon_data) > 1
