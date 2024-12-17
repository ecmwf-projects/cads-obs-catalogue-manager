from cdsobs.ingestion.core import (
    TimeBatch,
    TimeSpaceBatch,
)
from cdsobs.ingestion.serialize import batch_to_netcdf
from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.api import get_service_definition


def test_batch_to_netcdf(test_config, tmp_path):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    new_dataset_name = "insitu-observations-woudc-netcdfs"
    source = "OzoneSonde"
    year = 1969
    month = 1
    output_dir = tmp_path
    service_definition = get_service_definition(test_config, dataset_name)
    dataset_config = test_config.get_dataset(dataset_name)
    dataset_metadata = get_dataset_metadata(
        test_config, dataset_config, service_definition, source
    )
    time_space_batch = TimeSpaceBatch(TimeBatch(year, month))
    netcdf_path = batch_to_netcdf(
        dataset_metadata,
        output_dir,
        service_definition,
        test_config,
        time_space_batch,
        new_dataset_name,
    )
    assert netcdf_path.exists() and netcdf_path.stat().st_size > 0
