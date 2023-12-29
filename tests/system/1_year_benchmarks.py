# Benchmark with one year
# To be run in a VM with acces to the system, not with test DB and storage
import contextlib
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pandas
import sqlalchemy as sa

from cdsobs.api import run_ingestion_pipeline
from cdsobs.cli._delete_dataset import delete_from_catalogue
from cdsobs.cli._utils import read_and_validate_config
from cdsobs.constants import TEST_DATASETS, TEST_YEARS, VARS2DATASET
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.retrieve.api import retrieve_observations
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.service_definition.api import get_service_definition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def mocked_funct(*args, **kwargs):
    time.sleep(0.1)


def mocked_get_session(*args):
    return contextlib.nullcontext()


def main():
    cdsobs_config_yml = Path(os.environ.get("CDSOBS_CONFIG"))
    delete = False
    mock = False
    config = read_and_validate_config(cdsobs_config_yml)
    results_index = pandas.MultiIndex.from_frame(
        pandas.DataFrame(columns=["dataset_name", "source"])
    )
    results = pandas.DataFrame(
        index=results_index,
        columns=[
            "ingestion",
            "retrieve_global",
            "retrieve_station",
            "retrieve_variable",
        ],
    )
    results.columns.name = "times"
    if mock:
        ingestion_funct = mocked_funct  # run_ingestion_pipeline
        retrieve_funct = mocked_funct  # retrieve_observations
        get_session_funct = mocked_get_session  # get_session
    else:
        ingestion_funct = run_ingestion_pipeline
        retrieve_funct = retrieve_observations
        get_session_funct = get_session

    with get_session_funct(
        config.catalogue_db
    ) as session, tempfile.TemporaryDirectory() as tmpdir:
        for dataset_name in TEST_DATASETS:
            service_definition = get_service_definition(dataset_name)
            sources = service_definition.sources

            for dataset_source in sources:
                year = TEST_YEARS[dataset_source]
                logger.info(f"Loading {dataset_name=}, {dataset_source=}, {year=}")
                start_time = time.perf_counter()
                ingestion_funct(
                    dataset_name,
                    service_definition,
                    dataset_source,
                    session,
                    config,
                    start_year=year,
                    end_year=year,
                    update=False,
                )
                end_time = time.perf_counter()
                total_time = end_time - start_time
                results.loc[(dataset_name, dataset_source), "ingestion"] = total_time
                # Retrieve all stations, one month
                params = dict(
                    dataset_source=dataset_source,
                    latitude_coverage=(-90.0, 90.0),
                    longitude_coverage=(-180, 180.0),
                    time_coverage=(datetime(year, 1, 1), datetime(year, 2, 1)),
                    format="netCDF",
                )
                retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
                s3_client = S3Client.from_config(config.s3config)
                start_time = time.perf_counter()
                retrieve_funct(
                    session,
                    s3_client.public_url_base,
                    retrieve_args,
                    tmpdir,
                    size_limit=1000000000000,
                )
                end_time = time.perf_counter()
                total_time = end_time - start_time
                results.loc[
                    (dataset_name, dataset_source), "retrieve_global"
                ] = total_time
                # Retrieve one station for all the times
                if session is None:
                    station = "1"
                else:
                    station = [
                        session.scalar(
                            sa.select(Catalogue.stations).filter(
                                Catalogue.dataset == dataset_name,
                                Catalogue.dataset_source == dataset_source,
                            )
                        )[0]
                    ]
                params["stations"] = station
                params["time_coverage"] = (
                    datetime(year, 1, 1),
                    datetime(year + 1, 1, 1),
                )
                retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
                start_time = time.perf_counter()
                retrieve_funct(
                    session,
                    s3_client.public_url_base,
                    retrieve_args,
                    tmpdir,
                    size_limit=1000000000000,
                )
                end_time = time.perf_counter()
                total_time = end_time - start_time
                results.loc[
                    (dataset_name, dataset_source), "retrieve_station"
                ] = total_time
                # Select one of few variables for all stations one month
                variables = VARS2DATASET[dataset_name][dataset_source]
                params = dict(
                    dataset_source=dataset_source,
                    latitude_coverage=(-90.0, 90.0),
                    longitude_coverage=(-180, 180.0),
                    time_coverage=(datetime(year, 1, 1), datetime(year, 1, 31)),
                    format="netCDF",
                    variables=variables,
                )
                retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
                start_time = time.perf_counter()
                retrieve_funct(
                    session,
                    s3_client.public_url_base,
                    retrieve_args,
                    tmpdir,
                    size_limit=1000000000000,
                )
                end_time = time.perf_counter()
                total_time = end_time - start_time
                results.loc[
                    (dataset_name, dataset_source), "retrieve_variable"
                ] = total_time
                # Delete if asked
                if delete:
                    delete_from_catalogue(session, dataset_name, dataset_source, "")

    ofile = Path("results_1_year_benchmark.csv")
    logger.info(f"Writing results to {ofile}")
    results.to_csv(ofile)


if __name__ == "__main__":
    main()
