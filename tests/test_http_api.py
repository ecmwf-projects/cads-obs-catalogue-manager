from starlette.testclient import TestClient

from cdsobs.api_rest.app import app
from cdsobs.api_rest.endpoints import HttpAPISession, session_gen
from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.service_definition.api import get_service_definition

client = TestClient(app)


def test_read_main(test_repository, test_config, tmp_path):
    # We define a test session callable and use it to override session_gen for the test
    def test_session() -> HttpAPISession:
        return HttpAPISession(test_config, test_repository.catalogue_repository.session)

    # Note that the  key here is the callable itself, not the callable name.
    app.dependency_overrides[session_gen] = test_session

    payload = {
        "retrieve_args": {
            "dataset": "insitu-observations-gnss",
            "params": {
                "dataset_source": "IGS_R3",
                "stations": ["AREQ00PER"],
                "latitude_coverage": (-90.0, 0.0),
                "longitude_coverage": (-180.0, 0.0),
                "format": "netCDF",
                "variables": [
                    "precipitable_water_column",
                    "precipitable_water_column_total_uncertainty",
                ],
                "year": ["2000"],
                "month": ["10"],
                "day": [f"{i:02d}" for i in range(1, 32)],
            },
        },
        "config": {"size_limit": 1000000},
    }

    response = client.post("/get_object_urls_and_check_size", json=payload)
    assert response.status_code == 200
    assert response.json() == [
        "http://127.0.0.1:9000/cds2-obs-dev-insitu-observations-gnss/"
        "insitu-observations-gnss_IGS_R3_200010_-30.0_-80.0.nc"
    ]


def test_service_definition():
    dataset = "insitu-observations-gnss"
    actual = client.get(f"/{dataset}/service_definition").json()
    expected = get_service_definition(dataset).dict()
    assert actual == expected


def test_capabilities_datasets(test_config, test_repository):
    # We define a test session callable and use it to override session_gen for the test
    def test_session() -> HttpAPISession:
        return HttpAPISession(test_config, test_repository.catalogue_repository.session)

    # Note that the  key here is the callable itself, not the callable name.
    app.dependency_overrides[session_gen] = test_session
    actual = client.get("/capabilities/datasets").json()
    expected = [
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "insitu-observations-gnss",
    ]
    assert actual == expected


def test_capabilities_sources():
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"
    actual = client.get(f"capabilities/{dataset}/sources").json()
    expected = ["OzoneSonde", "TotalOzone"]
    assert actual == expected


def test_get_cdm_lite():
    actual = client.get("cdm/lite_variables").json()
    expected = cdm_lite_variables
    assert actual == expected


def test_get_dataset_auxiliary_variables_mapping():
    dataset = (
        "insitu-observations-near-surface-temperature-us-climate-reference-network"
    )
    actual = client.get(f"{dataset}/USCRN_DAILY/aux_variables_mapping").json()
    expected = {
        "accumulated_precipitation": [],
        "air_temperature": [
            {
                "auxvar": "air_temperature_mean_positive_total_uncertainty",
                "metadata_name": "positive_total_uncertainty",
            },
            {
                "auxvar": "air_temperature_mean_negative_total_uncertainty",
                "metadata_name": "negative_total_uncertainty",
            },
        ],
        "daily_maximum_air_temperature": [
            {
                "auxvar": "air_temperature_max_positive_total_uncertainty",
                "metadata_name": "positive_total_uncertainty",
            },
            {
                "auxvar": "air_temperature_max_negative_total_uncertainty",
                "metadata_name": "negative_total_uncertainty",
            },
        ],
        "daily_maximum_relative_humidity": [],
        "daily_minimum_air_temperature": [
            {
                "auxvar": "air_temperature_min_positive_total_uncertainty",
                "metadata_name": "positive_total_uncertainty",
            },
            {
                "auxvar": "air_temperature_min_negative_total_uncertainty",
                "metadata_name": "negative_total_uncertainty",
            },
        ],
        "daily_minimum_relative_humidity": [],
        "relative_humidity": [],
        "soil_moisture_100cm_from_earth_surface": [],
        "soil_moisture_10cm_from_earth_surface": [],
        "soil_moisture_20cm_from_earth_surface": [],
        "soil_moisture_50cm_from_earth_surface": [],
        "soil_moisture_5cm_from_earth_surface": [],
        "soil_temperature": [],
        "soil_temperature_100cm_from_earth_surface": [],
        "soil_temperature_10cm_from_earth_surface": [],
        "soil_temperature_20cm_from_earth_surface": [],
        "soil_temperature_50cm_from_earth_surface": [],
        "soil_temperature_5cm_from_earth_surface": [],
    }
    assert expected == actual
