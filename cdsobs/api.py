"""Main python API."""
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Iterator

import pandas
from sqlalchemy.orm import Session

from cdsobs.cdm.api import (
    check_cdm_compliance,
    define_units,
)
from cdsobs.config import CDSObsConfig, DatasetConfig
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.api import (
    EmptyBatchException,
    _entry_exists,
    read_batch_data,
    sort,
)
from cdsobs.ingestion.core import (
    DatasetPartition,
    SpaceBatch,
    TimeBatch,
    TimeSpaceBatch,
)
from cdsobs.ingestion.partition import get_partitions, save_partitions
from cdsobs.ingestion.serialize import serialize_partition
from cdsobs.metadata import get_dataset_metadata
from cdsobs.observation_catalogue.repositories.dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)
from cdsobs.retrieve.filter_datasets import between
from cdsobs.service_definition.api import get_service_definition
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def run_ingestion_pipeline(
    dataset_name: str,
    source: str,
    session: Session,
    config: CDSObsConfig,
    start_year: int,
    end_year: int,
    start_month: int = 1,
    version: str = DEFAULT_VERSION,
):
    """
    Ingest the data to the CADS observation repository.

    In order to do this, the data goes through several steps:
    - Read the ingestion tables.
    - Validate and homogeneise these tables.
    - Partition and sorting.
    - Saving the partitions to the catalogue and the storage.

    Parameters
    ----------
    dataset_name :
      Name of the dataset, for example insitu-observations-woudc-ozone-total-column-and-profiles
    source :
      Name of the data type to read from the dataset. For example "OzoneSonde".
    session :
      Session on the catalogue database
    config :
      Configuration of the CDSOBS catalogue manager
    start_year:
      Year to start reading the data.
    end_year:
      Year to finish reading the data.
    start_month:
      Month to start reading the data. It only applies to the first year of the interval.
      Default is 1.
    version:
      Semantic version to use for the data being uploaded.
    """
    logger.info("----------------------------------------------------------------")
    logger.info("Running ingestion pipeline")
    logger.info("----------------------------------------------------------------")
    service_definition = get_service_definition(config, dataset_name)

    def _run_for_batch(time_space_batch):
        try:
            _run_ingestion_pipeline_for_batch(
                dataset_name,
                service_definition,
                source,
                session,
                config,
                time_space_batch,
                version=version,
            )
        except EmptyBatchException:
            logger.warning(f"Data not found for {time_space_batch=}")

    main_iterator = _get_main_iterator(
        config, dataset_name, source, start_year, end_year, start_month=start_month
    )

    for time_space_batch in main_iterator:
        logger.info(f"Running ingestion pipeline for {time_space_batch}")
        _run_for_batch(time_space_batch)


def run_make_cdm(
    dataset_name: str,
    source: str,
    config: CDSObsConfig,
    start_year: int,
    end_year: int,
    output_dir: Path,
    save_data: bool = False,
    version: str = DEFAULT_VERSION,
):
    """
    Run the first steps of the ingestion pileline.

    This is equivalent to running make_production without uploading anything to the
    catalogue or the storage. It is to be used to check that the data can be successfully
    parsed in the Observations CDM, and that the resulting files are correct.

    Parameters
    ----------
    dataset_name :
      Name of the dataset, for example insitu-observations-woudc-ozone-total-column-and-profiles
    source
     Name of the data type to read from the dataset. For example "OzoneSonde".
    config
      Configuration of the CDSOBS catalogue manager.
    start_year:
      Year to start reading the data.
    end_year:
      Year to finish reading the data.
    output_dir:
      Directory where to save the files produced, in case save_data is True.
    save_data:
      Whether to produce the netCDFs as they would be uploaded to the storage by
      make_production. If False, the data only will be loaded and checked for CDM
    version:
      Semantic version to use for the data. Is written in the file names.
    """
    logger.info("----------------------------------------------------------------")
    logger.info("Running make cdm")
    logger.info("----------------------------------------------------------------")
    service_definition = get_service_definition(config, dataset_name)

    def _run_for_batch(time_batch):
        try:
            _run_make_cdm_for_batch(
                config,
                dataset_name,
                output_dir,
                save_data,
                service_definition,
                source,
                time_batch,
                version=version,
            )
        except EmptyBatchException:
            logger.warning(f"No data found for {time_batch=}")

    main_iterator = _get_main_iterator(
        config, dataset_name, source, start_year, end_year
    )
    for time_space_batch in main_iterator:
        _run_for_batch(time_space_batch)


def _run_ingestion_pipeline_for_batch(
    dataset_name: str,
    service_definition: ServiceDefinition,
    source: str,
    session: Session,
    config: CDSObsConfig,
    time_space_batch: TimeSpaceBatch,
    version: str = "DEFAULT_VERSION",
):
    """
    Ingest the data for a given year and month, specified by TimeBatch.

    Parameters
    ----------
    dataset_name :
      Name of the dataset, for example insitu-observations-woudc-ozone-total-column-and-profiles
    service_definition :
      Object produced parsing the service_definition.json.
    source :
      Name of the data type to read from the dataset. For example "OzoneSonde".
    session :
      Session on the catalogue database
    config :
      Configuration of the CDSOBS catalogue manager
    time_space_batch:
      Optionally read data only for one year and month
    """
    if _entry_exists(dataset_name, session, source, time_space_batch, version):
        logger.warning(
            "A partition with the chosen parameters already exists and update is set to False."
        )
    else:
        sorted_partitions = _read_homogenise_and_partition(
            config, dataset_name, service_definition, source, time_space_batch, version
        )
        # Create dataset if it does not exist
        cads_dataset_repo = CadsDatasetRepository(session)
        cads_dataset_repo.create_dataset(dataset_name=dataset_name)
        # Create dataset version if it does not exist
        cads_dataset_version_repo = CadsDatasetVersionRepository(session)
        cads_dataset_version_repo.create_dataset_version(dataset_name, version=version)
        logger.info("Partitioning data and saving to storage")
        s3_client = S3Client.from_config(config.s3config)
        logger.debug(f"Getting client to S3 storage: {s3_client}")
        save_partitions(session, s3_client, sorted_partitions)


def _get_main_iterator(
    config: CDSObsConfig,
    dataset_name: str,
    source: str,
    start_year: int,
    end_year: int,
    start_month: int = 1,
) -> Iterator[TimeSpaceBatch]:
    """Get the main iterator for the ingestion, over batches of time and space."""
    dataset_config = config.get_dataset(dataset_name)
    time_tile_size = dataset_config.time_tile_size
    if time_tile_size == "month":
        time_iterator = (
            TimeBatch(dt.year, dt.month)
            for dt in pandas.date_range(
                datetime(start_year, start_month, 1),
                datetime(end_year, 12, 31),
                freq="MS",
            )
        )
    elif time_tile_size == "year":
        years = range(start_year, end_year + 1)
        time_iterator = (TimeBatch(yy) for yy in years)
    else:
        raise NotImplementedError(f"Invalid {time_tile_size=}")
    if dataset_config.read_with_spatial_batches:
        logger.info("Reading each tile at a time, both in space and time.")
        main_iterator = _get_space_and_time_iterator(
            dataset_config, time_iterator, source
        )
    else:
        main_iterator = (TimeSpaceBatch(t) for t in time_iterator)
    return main_iterator


def _get_space_and_time_iterator(
    dataset_config: DatasetConfig, time_iterable: Iterator[TimeBatch], source: str
) -> Iterator[TimeSpaceBatch]:
    for time_batch in time_iterable:
        space_iterable = _get_space_iterator(dataset_config, source, time_batch.year)
        for space_batch in space_iterable:
            yield TimeSpaceBatch(time_batch, space_batch)


def _get_space_iterator(
    dataset_config: DatasetConfig, source: str, year: int
) -> Iterator[SpaceBatch]:
    lon_tile_size = dataset_config.get_tile_size("lon", source, year)
    lat_tile_size = dataset_config.get_tile_size("lat", source, year)
    for lon_start, lat_start in product(
        range(-180, 180, lon_tile_size), range(-90, 90, lat_tile_size)
    ):
        yield SpaceBatch(
            lon_start, lon_start + lon_tile_size, lat_start, lat_start + lat_tile_size
        )


def _read_homogenise_and_partition(
    config: CDSObsConfig,
    dataset_name: str,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
    version: str,
) -> Iterator[DatasetPartition]:
    dataset_config = config.get_dataset(dataset_name)
    dataset_metadata = get_dataset_metadata(
        config, dataset_config, service_definition, source, version
    )
    # Get the data as a single big table with the names remmaped from
    # service_definition
    homogenised_data = read_batch_data(
        config, dataset_metadata, service_definition, time_space_batch
    )
    # Validate that times are inside the time batch
    _validate_time_interval(homogenised_data, time_space_batch.time_batch)
    # Check CDM compliance
    check_cdm_compliance(homogenised_data, dataset_metadata.cdm_tables)
    # Define units id not present and apply unit changes
    if "units" not in homogenised_data.columns:
        homogenised_data = define_units(
            homogenised_data,
            service_definition.sources[source],
            dataset_metadata.cdm_code_tables["observed_variable"],
        )
    # If units is present but encoded as integers, decode them
    # Old datasets have all units as strings, so we shall keep it like this.
    unit_codes = dataset_metadata.cdm_code_tables["units"]
    code2unit = unit_codes.table["abbreviation"].to_dict()
    code2unit[0] = "none"
    unit_fields = [f for f in homogenised_data.columns if "units" in f]
    for unit_field in unit_fields:
        if homogenised_data[unit_field].dtype.kind == "i":
            # Decode integers using CDM tables
            decoded_units = homogenised_data[unit_field].map(code2unit)
            # Check for nans
            if decoded_units.isnull().any():
                raise RuntimeError("Not all units were mapped")
            homogenised_data[unit_field] = decoded_units

    year = time_space_batch.time_batch.year
    lon_tile_size = dataset_config.get_tile_size("lon", source, year)
    lat_tile_size = dataset_config.get_tile_size("lat", source, year)
    # Here we use iterators to be memory efficient
    data_partitions = get_partitions(
        dataset_metadata,
        homogenised_data,
        time_space_batch.time_batch,
        lon_tile_size=lon_tile_size,
        lat_tile_size=lat_tile_size,
    )
    sorted_partitions = (sort(dp) for dp in data_partitions)
    return sorted_partitions


def _validate_time_interval(homogenised_data: pandas.DataFrame, time_batch: TimeBatch):
    start_date, end_date = time_batch.get_time_coverage()
    times_check = between(homogenised_data.report_timestamp, start_date, end_date).all()
    if not times_check:
        raise AssertionError(
            f"Thee times read {homogenised_data.report_timestamp} are outside "
            f"{start_date} {end_date}. Something when wrong with the time selection."
        )


def _run_make_cdm_for_batch(
    config: CDSObsConfig,
    dataset_name: str,
    output_dir: Path,
    save_data: bool,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
    version: str = "DEFAULT_VERSION",
):
    sorted_partitions = _read_homogenise_and_partition(
        config, dataset_name, service_definition, source, time_space_batch, version
    )
    if save_data:
        for partition in sorted_partitions:
            serialized_partition = serialize_partition(partition, output_dir)
            logger.info(
                f"Saved partition to "
                f"{serialized_partition.file_params.local_temp_path}"
            )
