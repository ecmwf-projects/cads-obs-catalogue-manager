import tempfile
from datetime import datetime
from pathlib import Path

import xarray

from cdsobs.cli._object_storage import check_if_missing_in_object_storage
from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.core import get_variables_from_service_definition
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.api import retrieve_observations
from cdsobs.retrieve.filter_datasets import between
from cdsobs.retrieve.models import RetrieveArgs, RetrieveParams
from cdsobs.service_definition.api import get_service_definition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def run_sanity_checks(
    config: CDSObsConfig,
    datasets_to_check: list[str],
    years_to_check: dict[str, int],
    test: bool = False,
):
    for dataset_name in datasets_to_check:
        service_definition = get_service_definition(dataset_name)
        if test:
            sources = ["OzoneSonde"]
        else:
            sources = list(service_definition.sources)

        for dataset_source in sources:
            variables = get_variables_from_service_definition(
                service_definition, dataset_source
            )
            _sanity_check_dataset(
                config, dataset_name, dataset_source, variables, years_to_check
            )


def _sanity_check_dataset(
    config: CDSObsConfig,
    dataset_name: str,
    dataset_source: str,
    variables_from_service_definition: list[str],
    years_to_check: dict[str, int],
):
    year = years_to_check[dataset_source]
    # Retrieve all stations, one month
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 1, 31)
    latitude_coverage = (-10.0, 80.0)
    longitude_coverage = (-170, 180.0)
    params = RetrieveParams(
        dataset_source=dataset_source,
        latitude_coverage=latitude_coverage,
        longitude_coverage=longitude_coverage,
        time_coverage=(start_date, end_date),
        format="netCDF",
        variables=variables_from_service_definition,
    )
    retrieve_args = RetrieveArgs(dataset=dataset_name, params=params)
    s3_client = S3Client.from_config(config.s3config)
    with get_session(
        config.catalogue_db
    ) as session, tempfile.TemporaryDirectory() as tmpdir:
        # Check consistency between the catalogue and the storage
        catalogue_repo = CatalogueRepository(session)
        check_if_missing_in_object_storage(catalogue_repo, s3_client, dataset_name)
        # Retrieve and check output
        output_path = retrieve_observations(
            config.catalogue_db.get_url(),
            s3_client.public_url_base,
            retrieve_args,
            Path(tmpdir),
            size_limit=1000000000000,
        )
        # Check the file is not empty
        if output_path.stat().st_size == 0:
            logger.warning("Retrieved file is zero size")
        # Open and check the main coordinates and variables are OK
        output_dataset = xarray.open_dataset(output_path)
        check_retrieved_dataset(
            output_dataset,
            start_date,
            end_date,
            latitude_coverage,
            longitude_coverage,
            variables_from_service_definition,
        )


def check_retrieved_dataset(
    output_dataset: xarray.Dataset,
    start_date: datetime,
    end_date: datetime,
    latitude_coverage: tuple[float, float],
    longitude_coverage: tuple[float, float],
    variables_from_service_definition: list[str],
):
    # Check times
    times_index = output_dataset.report_timestamp.to_index()
    times_are_in_bounds = between(times_index, start_date, end_date).all()
    if not times_are_in_bounds:
        logger.warning("report_timestamp file has dates outside the expected interval")
    if times_index.isnull().any():
        logger.warning("Null values found in report_timestamp")
    # Check observed_variables
    observed_variables = output_dataset.observed_variable
    if observed_variables.dtype.kind != "S":
        logger.warning("Observed variables have not been properly decoded")
    if observed_variables.isnull().any():
        logger.warning("Null values found in observed_variable")
    if (
        not observed_variables.str.decode("utf-8")
        .isin(variables_from_service_definition)
        .all()
    ):
        logger.warning("Found variables not in service definition")
    # Check latitude and longitude
    for coord in ("latitude", "longitude"):
        coverage = latitude_coverage if coord == "latitude" else longitude_coverage
        coord_data = output_dataset[coord]
        coord_is_in_bounds = between(coord_data, coverage[0], coverage[1]).all()
        if not coord_is_in_bounds:
            logger.warning(f"{coord} has values outside the expected interval")
        if coord_data.isnull().any():
            logger.warning(f"Null values foung in {coord}")
    # Check station ids
    if output_dataset.primary_station_id.isnull().any():
        logger.warning("Null values foung in primary_station_id")
