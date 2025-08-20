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
            "version": "last",
        },
    }

    response = client.post("/get_object_urls", json=payload)
    assert response.status_code == 200
    assert response.json() == [
        "http://127.0.0.1:9000/cds2-obs-dev-insitu-observations-gnss/"
        "insitu-observations-gnss_1.0.0_IGS_R3_200010_-30.0_-80.0.nc"
    ]


def test_service_definition(test_config):
    dataset = "insitu-observations-gnss"
    actual = client.get(f"/{dataset}/service_definition").json()
    expected = get_service_definition(test_config, dataset).model_dump()
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
        "insitu-observations-igra-baseline-network",
        "insitu-comprehensive-upper-air-observation-network",
        "insitu-observations-gruan-reference-network",
        "insitu-observations-gnss",
        "insitu-observations-surface-land",
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


def test_get_disabled_fields(test_config, test_repository):
    def test_session() -> HttpAPISession:
        return HttpAPISession(test_config, test_repository.catalogue_repository.session)

    # Note that the  key here is the callable itself, not the callable name.
    app.dependency_overrides[session_gen] = test_session
    dataset = "insitu-comprehensive-upper-air-observation-network"
    source = "CUON"

    actual = client.get(f"/{dataset}/{source}/disabled_fields").json()
    expected = ["report_type", "report_duration", "station_type", "secondary_id"]
    assert actual == expected
