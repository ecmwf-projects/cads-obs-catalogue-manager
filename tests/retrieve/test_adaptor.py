import os
from pathlib import Path

import xarray

from cdsobs.constants import CONFIG_YML


def test_adaptor(test_repository, tmp_path, test_api_server):
    """Full test with a local instance of the HTTP API."""
    os.environ["CDSOBS_CONFIG"] = str(CONFIG_YML)
    from cads_adaptors import ObservationsAdaptor

    test_request = {
        "observation_type": ["total_column"],
        "format": "netCDF",
        "variable": ["total_ozone_column"],
        "year": ["2011"],
        "month": ["02"],
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
        "obs_api_url": "http://localhost:8000",
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
