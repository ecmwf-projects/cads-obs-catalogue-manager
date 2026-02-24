from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.core import (
    IngestionRunParams,
    TimeBatch,
    TimeSpaceBatch,
)
from cdsobs.ingestion.serialize import batch_to_netcdf
from cdsobs.metadata import get_dataset_metadata


def test_batch_to_netcdf(test_config, tmp_path, test_sds):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    new_dataset_name = "insitu-observations-woudc-netcdfs"
    year = 1969
    month = 1
    output_dir = tmp_path
    service_definition = test_sds[dataset_name]
    run_params = IngestionRunParams(
        dataset_name, "OzoneSonde", DEFAULT_VERSION, test_config, service_definition
    )
    dataset_metadata = get_dataset_metadata(run_params)
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
