import importlib.resources
import typing
from datetime import datetime
from pathlib import Path

from cdsobs.observation_catalogue.models import CadsDataset, Catalogue

# Time units for all
# Note that the data type must be always int64, smaller ints will overflow.
TIME_UNITS_REFERENCE_DATE = "1900-01-01 00:00:00"
TIME_UNITS = f"seconds since {TIME_UNITS_REFERENCE_DATE}"

# From here, all constants are for the tests
cdsobs_path = typing.cast(Path, importlib.resources.files("cdsobs"))
SERVICE_DEFINITION_YML = Path(
    cdsobs_path,
    "data/insitu-observations-woudc-ozone-total-column-and-profiles/service_definition.yml",
)

TEST_VAR_OUT = "air_temperature"

DATE_FORMAT = "%Y-%m-%d, %H:%M:%S"

CONFIG_YML = Path(cdsobs_path, "data/cdsobs_config_template.yml")

DS_TEST_NAME = "insitu-observations-woudc-ozone-total-column-and-profiles"

SOURCE_TEST_NAME = "OzoneSonde"

CATALOGUE_ENTRY = Catalogue(
    dataset=CadsDataset(name=DS_TEST_NAME, version=1),
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
)


TEST_YEARS = dict(
    OzoneSonde=1969,
    TotalOzone=2011,
    IGRA=1981,
    IGRA_H=1981,
    CUON=1924,
    GRUAN=2010,
    USCRN_SUBHOURLY=2007,
    USCRN_HOURLY=2006,
    USCRN_DAILY=2007,
    USCRN_MONTHLY=2006,
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
        "USCRN_MONTHLY": ["daily_maximum_air_temperature", "accumulated_precipitation"],
        "USCRN_DAILY": ["relative_humidity", "air_temperature"],
        "USCRN_HOURLY": ["relative_humidity", "air_temperature"],
        "USCRN_SUBHOURLY": ["relative_humidity", "air_temperature"],
    },
    "insitu-observations-gnss": {
        "IGS": ["precipitable_water_column_era5"],
        "EPN": ["precipitable_water_column"],
    },
}
AUX_FIELDS = [
    "total_uncertainty",
    "positive_total_uncertainty",
    "negative_total_uncertainty",
    "max_positive_total_uncertainty",
    "max_negative_total_uncertainty",
    "min_positive_total_uncertainty",
    "min_negative_total_uncertainty",
    "random_uncertainty",
    "positive_systematic_uncertainty",
    "negative_systematic_uncertainty",
    "quasisystematic_uncertainty",
    "positive_quasisystematic_uncertainty",
    "negative_quasisystematic_uncertainty",
    "flag",
]
