import copy
import importlib
import os
import pickle
import time
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

import pytest
import requests
import uvicorn

from cdsobs.api import run_ingestion_pipeline
from cdsobs.cli._deprecate_version import deprecate_dataset_version
from cdsobs.config import CDSObsConfig
from cdsobs.constants import CATALOGUE_ENTRY, CONFIG_YML, DATE_FORMAT, DS_TEST_NAME
from cdsobs.ingestion.core import DatasetPartition, SerializedPartition
from cdsobs.ingestion.serialize import serialize_partition
from cdsobs.observation_catalogue.database import Base, get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.storage import S3Client, StorageClient
from tests.utils import get_test_years

TEST_API_PARAMETERS = [
    ("insitu-observations-surface-land", "sub_daily"),
    ("insitu-observations-woudc-ozone-total-column-and-profiles", "OzoneSonde"),
    ("insitu-observations-woudc-ozone-total-column-and-profiles", "TotalOzone"),
    (
        "insitu-observations-igra-baseline-network",
        "IGRA",
    ),
    (
        "insitu-observations-igra-baseline-network",
        "IGRA_H",
    ),
    (
        "insitu-comprehensive-upper-air-observation-network",
        "CUON",
    ),
    (
        "insitu-observations-gruan-reference-network",
        "GRUAN",
    ),
    (
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "uscrn_subhourly",
    ),
    (
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "uscrn_hourly",
    ),
    (
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "uscrn_daily",
    ),
    (
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
        "uscrn_monthly",
    ),
    (
        "insitu-observations-gnss",
        "IGS",
    ),
    (
        "insitu-observations-gnss",
        "EPN",
    ),
    (
        "insitu-observations-gnss",
        "IGS_R3",
    ),
    (
        "insitu-observations-ndacc",
        "Brewer_O3",
    ),
    (
        "insitu-observations-ndacc",
        "CH4",
    ),
    ("insitu-observations-ndacc", "CO"),
    ("insitu-observations-ndacc", "Dobson_O3"),
    ("insitu-observations-ndacc", "Ftir_profile_O3"),
    ("insitu-observations-ndacc", "Lidar_profile_O3"),
    ("insitu-observations-ndacc", "Mwr_profile_O3"),
    ("insitu-observations-ndacc", "OzoneSonde_O3"),
    ("insitu-observations-ndacc", "Uvvis_profile_O3"),
]


@pytest.fixture(scope="module")
def test_config():
    config = CDSObsConfig.from_yaml(CONFIG_YML)
    # For cuon, add the test dir with the test files
    cuon_config = config.get_dataset(
        "insitu-comprehensive-upper-air-observation-network"
    )
    tests_path = importlib.resources.files("tests")
    input_dir = Path(
        tests_path, "data/cuon_data/0-20500-0-94829_CEUAS_merged_v3.nc"
    ).parent.absolute()

    cuon_config.reader_extra_args["input_dir"] = str(input_dir)
    cuon_config.reader_extra_args["active_json"] = str(Path(input_dir, "active.json"))
    # Do the same for WOUDC test netcdfs
    woudc_netcdfs_config = config.get_dataset("insitu-observations-woudc-netcdfs")
    example_filename = "insitu-observations-woudc-ozone-total-column-and-profiles_OzoneSonde_1969_01.nc"
    input_dir = Path(
        tests_path, f"data/woudc_netcdfs/{example_filename}"
    ).parent.absolute()
    woudc_netcdfs_config.reader_extra_args["input_dir"] = str(input_dir)
    surface_config = config.get_dataset("insitu-observations-surface-land")
    input_dir = Path(Path(tests_path, "data/csv_data/").absolute(), "*.psv")
    surface_config.reader_extra_args["input_path"] = str(input_dir)
    return config


@pytest.fixture(scope="module")
def test_session(test_config):
    session = get_session(test_config.catalogue_db, reset=True)
    yield session
    # To reset database. Comment  this line to see test results.
    engine = session.get_bind()
    session.commit()
    Base.metadata.drop_all(engine)
    session.close()
    engine.dispose()


@pytest.fixture()
def test_session_pertest(test_config):
    session = get_session(test_config.catalogue_db, reset=True)
    engine = session.get_bind()
    yield session
    # To reset database. Comment  this line to see test results.
    session.commit()
    Base.metadata.drop_all(session.get_bind())
    session.close()
    engine.dispose()


@pytest.fixture
def test_catalogue_repository(test_session):
    return CatalogueRepository(test_session)


@pytest.fixture(scope="module")
def test_s3_client(test_config):
    s3client = S3Client.from_config(test_config.s3config)
    _clean_storage(s3client)
    yield s3client
    # To clean storage. Comment this line to see test results.
    _clean_storage(s3client)


@pytest.fixture()
def test_s3_client_pertest(test_config):
    s3client = S3Client.from_config(test_config.s3config)
    _clean_storage(s3client)
    yield s3client
    # To clean storage. Comment this line to see test results.
    _clean_storage(s3client)


@dataclass
class TestRepository:
    catalogue_repository: CatalogueRepository
    s3_client: StorageClient
    config: CDSObsConfig


@pytest.fixture(scope="module")
def test_repository(test_session, test_s3_client, test_config):
    """The whole thing, session to the catalogue DB and storage client."""
    for dataset_name, dataset_source in TEST_API_PARAMETERS:
        start_year, end_year = get_test_years(dataset_source)
        run_ingestion_pipeline(
            dataset_name,
            dataset_source,
            test_session,
            test_config,
            start_year,
            end_year,
        )

    # Gruan version 2.0.0
    dataset_name = "insitu-observations-gruan-reference-network"
    dataset_source = "GRUAN"
    start_year, end_year = get_test_years(dataset_source)
    run_ingestion_pipeline(
        dataset_name,
        dataset_source,
        test_session,
        test_config,
        start_year,
        end_year,
        version="2.0.0",
    )
    deprecate_dataset_version(
        CONFIG_YML,
        "insitu-observations-gruan-reference-network",
        version="1.0.0",
    )
    catalogue_repository = CatalogueRepository(test_session)
    test_repository = TestRepository(catalogue_repository, test_s3_client, test_config)
    yield test_repository
    test_repository.catalogue_repository.session.close()


@pytest.fixture
def test_partition() -> DatasetPartition:
    partition_file = Path(str(importlib.resources.files("tests")), "data/partition.pkl")
    with partition_file.open("rb") as fileobj:
        return pickle.load(fileobj)


@pytest.fixture
def test_serialized_partition(
    test_partition, tmp_path, test_config
) -> SerializedPartition:
    serialized_partition = serialize_partition(test_partition, tmp_path)
    return serialized_partition


def _clean_bucket(bucket_name: str, test_s3_client: S3Client):
    """Remove all objects in a bucket for teardown after tests."""
    for object_name in test_s3_client.list_directory_objects(bucket_name):
        test_s3_client.delete_file(bucket_name, object_name)


def _clean_storage(test_s3_client: S3Client):
    """Clean all storage for teardown after tests."""
    for bucket in test_s3_client.list_buckets():
        _clean_bucket(bucket, test_s3_client)
        test_s3_client.delete_bucket(bucket)


@pytest.fixture
def mock_entries():
    catalogue_entry_1 = copy.deepcopy(CATALOGUE_ENTRY)
    catalogue_entry_2 = copy.deepcopy(CATALOGUE_ENTRY)
    catalogue_entry_2.stations = ["7", "8"]
    catalogue_entry_2.constraints["time"] = [datetime(1998, 1, 2).strftime(DATE_FORMAT)]
    catalogue_entry_2.constraints["variable_constraints"] = {
        "air_pressure": [1],
        "column_burden": [0],
    }
    return [catalogue_entry_1, catalogue_entry_2]


@pytest.fixture
def mock_retrieve_args():
    params = dict(
        dataset_source="OzoneSonde",
        stations=["7"],
        variables=["air_pressure", "geopotential_height"],
        latitude_coverage=(0.0, 90.0),
        longitude_coverage=(120.0, 140.0),
        time_coverage=(datetime(1998, 1, 1), datetime(1998, 2, 1)),
    )
    return RetrieveArgs(dataset=DS_TEST_NAME, params=params)


def run_server():
    os.environ["CDSOBS_CONFIG"] = str(CONFIG_YML)
    uvicorn.run(
        "cdsobs.api_rest.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1,
    )


def wait_until_api_is_ready(timeout: int, period=0.25):
    mustend = time.time() + timeout
    while time.time() < mustend:
        try:
            res = requests.get("http://localhost:8000/cdm/lite_variables")
            res.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            time.sleep(period)
        except requests.exceptions.ConnectionError:
            time.sleep(period)
    return False


@pytest.fixture()
def test_api_server():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start()
    wait_until_api_is_ready(5)
    yield proc
    # Cleanup after test
    proc.terminate()
