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
            "dataset": "insitu-observations-woudc-ozone-total-column-and-profiles",
            "params": {
                "dataset_source": "OzoneSonde",
                "stations": ["7"],
                "variables": None,
                "latitude_coverage": (0.0, 90.0),
                "longitude_coverage": (0.0, 180.0),
                "time_coverage": ("1969-02-01T00:00:00", "1970-03-01T00:00:00"),
                "year": None,
                "month": None,
                "day": None,
                "format": "netCDF",
            },
        },
        "config": {"size_limit": 1000000},
    }

    response = client.post("/get_object_urls_and_check_size", json=payload)
    assert response.status_code == 200
    assert response.json() == [
        "http://127.0.0.1:9000/"
        "cds2-obs-dev-insitu-observations-woudc-ozone-total-column-and-p/"
        "insitu-observations-woudc-ozone-total-column-and-profiles_OzoneSonde_1969_0.0_0.0.nc"
    ]


def test_service_definition():
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"
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
    expected = ["insitu-observations-woudc-ozone-total-column-and-profiles"]
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
