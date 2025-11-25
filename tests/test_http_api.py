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


def test_service_definition(test_config, test_repository):
    dataset = "insitu-observations-gnss"
    actual = client.get(f"/{dataset}/service_definition").json()
    expected = get_service_definition(test_config, dataset).model_dump(
        exclude_none=True, exclude={"path"}
    )
    expected.pop("path", None)
    assert actual == expected


def test_capabilities_datasets(test_config, test_repository):
    # We define a test session callable and use it to override session_gen for the test
    def test_session() -> HttpAPISession:
        return HttpAPISession(test_config, test_repository.catalogue_repository.session)

    # Note that the  key here is the callable itself, not the callable name.
    app.dependency_overrides[session_gen] = test_session
    actual = client.get("/capabilities/datasets").json()
    expected = [
        "insitu-observations-surface-land",
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "insitu-observations-igra-baseline-network",
        "insitu-comprehensive-upper-air-observation-network",
        "insitu-observations-gruan-reference-network",
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


def test_get_form(test_config, test_repository):
    def test_session() -> HttpAPISession:
        return HttpAPISession(test_config, test_repository.catalogue_repository.session)

    # Note that the  key here is the callable itself, not the callable name.
    app.dependency_overrides[session_gen] = test_session
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"

    actual = client.get(f"/{dataset}/forms/variables.json").json()
    expected = {
        "OzoneSonde": {
            "air_temperature": {
                "description": "Level temperature Kelvin.",
                "dtype": "float32",
                "units": "Kelvin",
            },
            "altitude": {
                "description": "Geometric altitude above sea level calculated from air "
                "pressure and GPS altitude",
                "dtype": "float32",
                "units": "m",
            },
            "date_time": {
                "description": "The mean time of observations.",
                "dtype": "float32",
                "units": "decimal hours, UTC",
            },
            "geopotential_height": {
                "description": "Geopotential height in meters.",
                "dtype": "float32",
                "units": "m",
            },
            "height_of_station_above_sea_level": {
                "description": "Height is defined as the altitude, elevation, or height "
                "of the defined platform + instrument above sea level.",
                "dtype": "float32",
                "units": "meters above sea level",
            },
            "latitude|header_table": {
                "description": "Latitude of the instrument.",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "latitude|observations_table": {
                "description": "Geographical latitude (for example from GPS).",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "longitude|header_table": {
                "description": "Longitude of the instrument.",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "longitude|observations_table": {
                "description": "Geographical longitude (for example from GPS).",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "ozone_partial_pressure": {
                "description": "Level partial pressure of ozone in Pascals.",
                "dtype": "float32",
                "units": "Pa",
            },
            "platform_type": {
                "description": "Type of observing platform.",
                "dtype": "object",
                "units": None,
            },
            "pressure": {
                "description": "Atmospheric pressure of each level in Pascals.",
                "dtype": "float32",
                "units": "Pa",
            },
            "primary_station_id": {
                "description": "Unique station or flight ID assigned by the WOUDC to "
                "each registered platform.",
                "dtype": "object",
                "units": None,
            },
            "relative_humidity": {
                "description": "Percentage of water vapour relative to the saturation amount.",
                "dtype": "float32",
                "units": "%",
            },
            "report_timestamp": {
                "description": "Timestamp with time zone.",
                "dtype": "datetime64[ns]",
                "units": None,
            },
            "sensor_id": {
                "description": "Model ID where applicable.",
                "dtype": "object",
                "units": None,
            },
            "sensor_model": {
                "description": "Radiosonde model.",
                "dtype": "object",
                "units": None,
            },
            "time_since_launch": {
                "description": "Elapsed flight time since released as primary variable.",
                "dtype": "float32",
                "units": "s",
            },
            "wind_from_direction": {
                "description": "Wind direction in degrees.",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "wind_speed": {
                "description": "Wind speed in meters per second.",
                "dtype": "float32",
                "units": "m s^-1",
            },
        },
        "TotalOzone": {
            "column_sulphur_dioxide": {
                "description": "The daily total column sulphur dioxide (SO2) amount "
                "calculated as the mean of the individual SO2 amounts "
                "from the same observation used for the O3 amount.",
                "dtype": "float32",
                "units": "Dobson-units",
            },
            "height_of_station_above_sea_level": {
                "description": "Height is defined as the altitude, elevation, or height "
                "of the defined platform + instrument above sea level.",
                "dtype": "float32",
                "units": "meters above sea level",
            },
            "latitude|header_table": {
                "description": "Latitude of the measurement station (used when differs "
                "from the one of the instrument).",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "longitude|header_table": {
                "description": "Longitude of the measurement station (used when differs "
                "from the one of the instrument).",
                "dtype": "float32",
                "units": "decimal degrees",
            },
            "monthly_total_ozone_column_number_of_points": {
                "description": "The number of points (typically this is the number of "
                "daily averages) used to estimate the monthly mean ozone value.",
                "dtype": "float32",
                "units": None,
            },
            "number_of_observations": {
                "description": "Number of observations used to calculate the total "
                "column ozone value.",
                "dtype": "float32",
                "units": None,
            },
            "platform_type": {
                "description": "Type of observing platform.",
                "dtype": "object",
                "units": None,
            },
            "primary_station_id": {
                "description": "Unique station or flight ID assigned by the WOUDC to "
                "each registered platform.",
                "dtype": "object",
                "units": None,
            },
            "report_timestamp": {
                "description": "timestamp datetime first day.",
                "dtype": "datetime64[ns]",
                "units": "Datetime",
            },
            "sensor_id": {
                "description": "Model ID where applicable.",
                "dtype": "object",
                "units": None,
            },
            "standard_deviation_ozone": {
                "description": "Estimated population standard deviation of the total "
                "column ozone measurements used for the daily value.",
                "dtype": "float32",
                "units": None,
            },
            "time_begin": {
                "description": "The starting time of observations.",
                "dtype": "float32",
                "units": "decimal hours, UTC",
            },
            "time_end": {
                "description": "The ending time of observations.",
                "dtype": "float32",
                "units": "decimal hours, UTC",
            },
            "time_mean": {
                "description": "The mean time of observations.",
                "dtype": "float32",
                "units": "decimal hours, UTC",
            },
            "total_ozone_column": {
                "description": "Daily value of total column ozone amount defined as the "
                "'best representative value' in order of Direct Sun (DS),"
                " Zenith Cloud (ZS) and Focused Moon (FM).",
                "dtype": "float32",
                "units": "Dobson-units",
            },
        },
    }

    assert actual == expected
