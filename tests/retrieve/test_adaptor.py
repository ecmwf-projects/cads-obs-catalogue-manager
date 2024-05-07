from pathlib import Path

import pytest
import xarray


@pytest.mark.skip("Depends on cads_adaptors")
def test_adaptor(test_config, test_repository, tmp_path):
    from cads_adaptors import ObservationsAdaptor

    test_request = {
        "observation_type": ["vertical_profile"],
        "format": "csv",
        "variable": ["air_temperature"],
        "year": ["1969"],
        "month": ["01"],
        "day": [
            "01",
            "02",
            "03",
        ],
    }
    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test as it works with MiniIO.
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
    adaptor = ObservationsAdaptor(test_form, **test_adaptor_config)
    result = adaptor.retrieve(test_request)
    tempfile = Path(tmp_path, "test_adaptor.nc")
    with tempfile.open("wb") as tmpf:
        tmpf.write(result.read())
    assert tempfile.stat().st_size > 0
    assert xarray.open_dataset(tempfile).observation_id.size > 0
