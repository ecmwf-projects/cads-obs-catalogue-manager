import os
from pathlib import Path

from cdsobs.ingestion.core import SpaceBatch, TimeBatch, TimeSpaceBatch
from cdsobs.ingestion.readers.cuon import (
    filter_batch_stations,
    get_cuon_stations,
    read_cuon_netcdfs,
)


def test_read_cuon(test_config, test_sds):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = test_sds[dataset_name]
    time_space_batch = TimeSpaceBatch(TimeBatch(1960, 1))
    os.environ["CADSOBS_AVOID_MULTIPROCESS"] = "True"
    cuon_data = read_cuon_netcdfs(
        dataset_name,
        test_config,
        service_definition,
        "CUON",
        time_space_batch,
        **service_definition.reader_extra_args,
    )
    assert len(cuon_data) > 1


def test_filter_batch_stations(test_config, test_sds):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = test_sds[dataset_name]
    active_json = service_definition.reader_extra_args["active_json"]
    time_space_batch = TimeSpaceBatch(
        TimeBatch(1960, 1),
        SpaceBatch(
            latitude_start=0, longitude_start=100, latitude_end=80, longitude_end=180
        ),
    )
    station_metadata = get_cuon_stations(active_json)
    files = [Path(f) for f in station_metadata["file path"].tolist()]
    files_filtered = filter_batch_stations(files, time_space_batch, active_json)
    assert len(files_filtered) == 411
