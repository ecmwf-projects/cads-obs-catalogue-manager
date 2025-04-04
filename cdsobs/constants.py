import importlib.resources
import typing
from datetime import datetime
from pathlib import Path

from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.schemas.cads_dataset import CadsDatasetSchema

# Time units for all
# Note that the data type must be always int64, smaller ints will overflow.
TIME_UNITS_REFERENCE_DATE = "1900-01-01 00:00:00"
TIME_UNITS = f"seconds since {TIME_UNITS_REFERENCE_DATE}"

# From here, all constants are for the tests
cdsobs_path = typing.cast(Path, importlib.resources.files("cdsobs"))

TEST_VAR_OUT = "air_temperature"

DATE_FORMAT = "%Y-%m-%d, %H:%M:%S"

CONFIG_YML = Path(Path(cdsobs_path).parent, "tests", "data", "cdsobs_config_test.yml")

DS_TEST_NAME = "insitu-observations-woudc-ozone-total-column-and-profiles"

SOURCE_TEST_NAME = "OzoneSonde"

DEFAULT_VERSION = "1.0.0"

CATALOGUE_ENTRY = Catalogue(
    dataset=CadsDatasetSchema(name=DS_TEST_NAME),
    dataset_source=SOURCE_TEST_NAME,
    time_coverage_start=datetime(1998, 1, 1),
    time_coverage_end=datetime(1998, 2, 1),
    latitude_coverage_start=30,
    latitude_coverage_end=35,
    longitude_coverage_start=130,
    longitude_coverage_end=135,
    variables=[TEST_VAR_OUT, "geopotential_height"],
    stations=["7"],
    sources=[],
    asset=(
        "insitu-observations-woudc-ozone-total-column-and-profiles/"
        "insitu-observations-woudc-ozone-total-column-and-profiles_"
        "OzoneSonde_1999_1_30.0_130.0.nc"
    ),
    file_size=1,
    data_size=1,
    file_checksum="95458a832fbf2616fd4180319a4f8ccba94770b2c4b9552479407cf8293483c1",
    constraints={
        "dims": ["stations", "time"],
        "time": [
            datetime(1998, 1, 2).strftime(DATE_FORMAT),
            datetime(1998, 1, 4).strftime(DATE_FORMAT),
        ],
        "variable_constraints": {"air_pressure": [0, 1]},
    },
    version=DEFAULT_VERSION,
)


TEST_YEARS = dict(
    OzoneSonde=1969,
    TotalOzone=2011,
    IGRA=1981,
    IGRA_H=1981,
    CUON=1924,
    GRUAN=2010,
    uscrn_subhourly=2007,
    uscrn_hourly=2006,
    uscrn_daily=2007,
    uscrn_monthly=2006,
    IGS=2001,
    EPN=1996,
)
TEST_DATASETS = [
    "insitu-observations-woudc-ozone-total-column-and-profiles",
    "insitu-observations-igra-baseline-network",
    "insitu-comprehensive-upper-air-observation-network",
    "insitu-observations-gruan-reference-network",
    "insitu-observations-near-surface-temperature-us-climate-reference-network",
    "insitu-observations-gnss",
]
VARS2DATASET = {
    "insitu-observations-woudc-ozone-total-column-and-profiles": {
        "OzoneSonde": ["wind_speed", "relative_humidity"],
        "TotalOzone": ["total_ozone_column", "column_sulphur_dioxide"],
    },
    "insitu-observations-igra-baseline-network": {
        "IGRA": ["wind_speed", "geopotential_height"],
        "IGRA_H": ["wind_speed", "geopotential_height"],
    },
    "insitu-comprehensive-upper-air-observation-network": {
        "CUON": ["geopotential_height", "air_temperature"]
    },
    "insitu-observations-gruan-reference-network": {
        "GRUAN": ["wind_speed", "air_temperature"]
    },
    "insitu-observations-near-surface-temperature-us-climate-reference-network": {
        "uscrn_monthly": ["daily_maximum_air_temperature", "accumulated_precipitation"],
        "uscrn_daily": ["relative_humidity", "air_temperature"],
        "uscrn_hourly": ["relative_humidity", "air_temperature"],
        "uscrn_subhourly": ["relative_humidity", "air_temperature"],
    },
    "insitu-observations-gnss": {
        "IGS": ["precipitable_water_column_era5"],
        "EPN": ["precipitable_water_column"],
    },
}
