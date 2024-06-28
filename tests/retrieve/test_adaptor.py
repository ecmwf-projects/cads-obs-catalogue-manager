from pathlib import Path

import pytest
import xarray


# @pytest.mark.skip("API needs to be updated.")
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


@pytest.mark.skip("By hand only.")
def test_adaptor_uscrn(tmp_path):
    """Full test with a local instance of the HTTP API."""
    from cads_adaptors import ObservationsAdaptor

    test_request = {
        "time_aggregation": "daily",
        "format": "netCDF",
        "variable": [
            "maximum_air_temperature",
            "maximum_air_temperature_negative_total_uncertainty",
            "maximum_air_temperature_positive_total_uncertainty",
        ],
        "year": ["2007"],
        "month": ["11"],
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
        "collection_id": "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "obs_api_url": "http://localhost:8000",
        "mapping": {
            "remap": {
                "time_aggregation": {
                    "daily": "USCRN_DAILY",
                    "hourly": "USCRN_HOURLY",
                    "monthly": "USCRN_MONTHLY",
                    "sub_hourly": "USCRN_SUBHOURLY",
                },
                "variable": {
                    "maximum_air_temperature": "daily_maximum_air_temperature",
                    "maximum_air_temperature_negative_total_uncertainty": "air_temperature_max_negative_total_uncertainty",  # noqa E501
                    "maximum_air_temperature_positive_total_uncertainty": "air_temperature_max_positive_total_uncertainty",  # noqa E501
                    "maximum_relative_humidity": "daily_maximum_relative_humidity",
                    "maximum_soil_temperature": "hourly_maximum_soil_temperature",
                    "maximum_soil_temperature_flag": "hourly_maximum_soil_temperature_flag",  # noqa E501
                    "maximum_solar_irradiance": "hourly_maximum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                    "maximum_solar_irradiance_quality_flag": "hourly_maximum_downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                    "mean_air_temperature_negative_total_uncertainty": "air_temperature_mean_negative_total_uncertainty",  # noqa E501
                    "mean_air_temperature_positive_total_uncertainty": "air_temperature_mean_positive_total_uncertainty",  # noqa E501
                    "minimum_air_temperature": "daily_minimum_air_temperature",
                    "minimum_air_temperature_negative_total_uncertainty": "air_temperature_min_negative_total_uncertainty",  # noqa E501
                    "minimum_air_temperature_positive_total_uncertainty": "air_temperature_min_positive_total_uncertainty",  # noqa E501
                    "minimum_relative_humidity": "daily_minimum_relative_humidity",
                    "minimum_soil_temperature": "hourly_minimum_soil_temperature",
                    "minimum_soil_temperature_quality_flag": "hourly_minimum_soil_temperature_quality_flag",  # noqa E501
                    "minimum_solar_irradiance": "hourly_minimum_downward_shortwave_irradiance_at_earth_surface",  # noqa E501
                    "minimum_solar_irradiance_quality_flag": "hourly_minimum_downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                    "solar_irradiance": "downward_shortwave_irradiance_at_earth_surface",
                    "solar_irradiance_quality_flag": "downward_shortwave_irradiance_at_earth_surface_quality_flag",  # noqa E501
                },
            },
            "format": {"netcdf": "netCDF"},
            "rename": {"time_aggregation": "dataset_source", "variable": "variables"},
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


@pytest.mark.skip("By hand only.")
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
