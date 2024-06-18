from pathlib import Path

import xarray


def test_adaptor(tmp_path):
    """Full test with a local instance of the HTTP API."""
    from cads_adaptors import ObservationsAdaptor

    test_request = {
        "observation_type": ["vertical_profile"],
        "format": "netCDF",
        "variable": ["air_temperature", "geopotential_height"],
        "year": ["1999"],
        "month": ["01", "02"],
        "day": [f"{i:02d}" for i in range(1, 32)],
    }
    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test as it works with MiniIO.
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-woudc-ozone-total-column-and-profiles",
        "obs_api_url": "http://obscatalogue.cads-obs.compute.cci2.ecmwf.int",
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


def test_adaptor_gnss(tmp_path):
    """Full test with a local instance of the HTTP API."""
    from cads_adaptors import ObservationsAdaptor

    test_request = {
        "network_type": ["igs_r3"],
        "format": "netCDF",
        "variable": [
            "precipitable_water_column",
            "precipitable_water_column_total_uncertainty",
        ],
        "year": ["2000"],
        "month": ["10"],
        "day": [f"{i:02d}" for i in range(1, 32)],
    }
    test_form = {}
    # + "/v1/AUTH_{public_user}" will be needed to work with S3 ceph public urls, but it
    # is not needed for this test as it works with MiniIO.
    test_adaptor_config = {
        "entry_point": "cads_adaptors:ObservationsAdaptor",
        "collection_id": "insitu-observations-gnss",
        "obs_api_url": "http://localhost:8000",
        "mapping": {
            "remap": {
                "network_type": {
                    "igs_r3": "IGS_R3",
                    "epn_repro2": "EPN",
                    "igs_daily": "IGS",
                }
            },
            "rename": {"network_type": "dataset_source", "variable": "variables"},
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
