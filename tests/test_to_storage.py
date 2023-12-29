from cdsobs.cdm.api import to_cdm_dataset
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.ingestion.serialize import to_netcdf, to_storage


def test_to_storage(tmp_path, test_partition, test_s3_client, test_config):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    read_cdm_tables(test_config.cdm_tables_location)
    cdm_dataset = to_cdm_dataset(test_partition)
    temp_ofile = to_netcdf(cdm_dataset, tmp_path)
    actual = to_storage(test_s3_client, dataset_name, temp_ofile)
    expected = (
        "cds2-obs-dev-insitu-observations-woudc-ozone-total-column-and-p/"
        "insitu-observations-woudc-ozone-total-column-and-profiles_OzoneSonde_1969_0.0_0.0.nc"
    )
    assert actual == expected
