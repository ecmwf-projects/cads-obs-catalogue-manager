from datetime import datetime
from pathlib import Path

import pandas
import pytest
import xarray

from cdsobs.config import CDSObsConfig
from cdsobs.constants import CONFIG_YML
from cdsobs.observation_catalogue.database import get_session
from cdsobs.retrieve.api import retrieve_observations
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.storage import S3Client
from tests.utils import get_test_years

PARAMETRIZE_VALUES = (
    ("netCDF", "OzoneSonde", True),
    ("netCDF", "OzoneSonde", False),
    ("netCDF", "TotalOzone", True),
    ("csv", "OzoneSonde", True),
    ("csv", "TotalOzone", True),
)


@pytest.mark.parametrize("oformat,dataset_source,time_coverage", PARAMETRIZE_VALUES)
def test_retrieve(test_repository, tmp_path, oformat, dataset_source, time_coverage):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    start_year, end_year = get_test_years(dataset_source)
    if dataset_source == "OzoneSonde":
        stations = [
            "7",
        ]
    else:
        stations = [
            "2",
        ]
    # Full test
    params = dict(
        dataset_source=dataset_source,
        variables=None,
        latitude_coverage=(0.0, 90.0),
        longitude_coverage=(0.0, 180.0),
        format=oformat,
        stations=stations,
    )
    if time_coverage:
        params["time_coverage"] = (datetime(start_year, 2, 1), datetime(end_year, 3, 1))
    else:
        params["year"] = [start_year]
        params["month"] = [2, 3]
        params["day"] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
    start = datetime.now()
    output_file = retrieve_observations(
        test_repository.catalogue_repository.session,
        test_repository.s3_client.base,
        retrieve_args,
        tmp_path,
        size_limit=1000000000000,
    )
    end = datetime.now()
    print(f"Retrieve took {(end - start).total_seconds()}")
    assert output_file.exists() and output_file.stat().st_size > 0
    if oformat == "netCDF":
        retrieved_dataset = xarray.open_dataset(output_file)
        assert retrieved_dataset.observation_id.size > 1
    elif oformat == "csv":
        retrieved_dataset = pandas.read_csv(output_file, comment="#")
        assert len(retrieved_dataset) > 1 and len(retrieved_dataset.columns) > 1
    else:
        raise RuntimeError


@pytest.mark.skip("Too slow")
def test_retrieve_cuon():
    test_config = CDSObsConfig.from_yaml(CONFIG_YML)
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    params = {
        "dataset_source": "CUON",
        "time_coverage": ["2000-01-01 00:00:00", "2000-12-31 00:00:00"],
        "variables": [
            "aerosol_absorption_optical_depth",
            "air_temperature",
            "relative_humidity",
            "geopotential_height",
        ],
    }
    retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
    session = get_session(test_config.catalogue_db)
    s3_client = S3Client.from_config(test_config.s3config)
    output_file = retrieve_observations(
        session,
        s3_client.base,
        retrieve_args,
        Path("/tmp"),
        size_limit=1000000000000,
    )
    print(output_file)


if __name__ == "__main__":
    test_retrieve_cuon()
