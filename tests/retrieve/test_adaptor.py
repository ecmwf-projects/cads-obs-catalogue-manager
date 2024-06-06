from pathlib import Path

import pytest
import xarray


def get_woudc_adaptor_config(test_config):
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-woudc-ozone-total-column-and-profiles",
        "catalogue_url": test_config.catalogue_db.get_url(),
        "storage_url": "http://"
        + test_config.s3config.host
        + ":"
        + str(test_config.s3config.port),
        "mapping": {
            "remap": {
                "observation_type": {
                    "total_column": "TotalOzone",
                    "vertical_profile": "OzoneSonde",
                }
            },
            "rename": {"observation_type": "dataset_source", "variable": "variables"},
            "force": {},
        },
    }
    return test_adaptor_config


def get_gnss_adaptor_config(test_config):
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-gnss",
        "catalogue_url": test_config.catalogue_db.get_url(),
        "storage_url": "http://"
        + test_config.s3config.host
        + ":"
        + str(test_config.s3config.port),
        "mapping": {
            "remap": {
                "network_type": {
                    "epn_repro2": "EPN",
                    "igs_daily": "IGS",
                    "igs3": "IGS_R3",
                },
            },
            "rename": {"network_type": "dataset_source", "variable": "variables"},
            "force": {},
        },
    }
    return test_adaptor_config


test_request_woudc = {
    "observation_type": ["vertical_profile"],
    "format": "netCDF",
    "variable": ["air_temperature"],
    "year": ["1969"],
    "month": ["01"],
    "day": [
        "01",
        "02",
        "03",
    ],
}


test_request_gnss = {
    "network_type": ["igs3"],
    "format": "netCDF",
    "variable": ["total_column_water_vapour"],
    "year": ["2000"],
    "month": ["10"],
    "day": [
        "22",
    ],
}


@pytest.mark.skip("Depends on cads_adaptors")
def test_adaptor(test_config, test_repository, tmp_path):
    from cads_adaptors import ObservationsAdaptor

    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test as it works with MiniIO.
    test_adaptor_config = get_woudc_adaptor_config(test_config)
    adaptor = ObservationsAdaptor(test_form, **test_adaptor_config)
    result = adaptor.retrieve(test_request_woudc)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    assert xarray.open_dataset(tempfile).observation_id.size > 0
