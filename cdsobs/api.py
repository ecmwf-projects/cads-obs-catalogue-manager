"""Main python API."""
import socket
import tempfile
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Iterator, Literal

import h5netcdf
import pandas
from sqlalchemy.orm import Session

from cdsobs import warning_tracker
from cdsobs.cdm.api import (
    _check_cdm_units,
    check_cdm_compliance,
    define_units,
    get_cdm_repo_current_tag,
    get_varname2units,
)
from cdsobs.cli._catalogue_explorer import stats_summary
from cdsobs.config import CDSObsConfig, DatasetConfig
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.api import (
    EmptyBatchException,
    entry_exists,
    read_batch_data,
    sort,
)
from cdsobs.ingestion.core import (
    DatasetMetadata,
    DatasetPartition,
    IngestionRunParams,
    SpaceBatch,
    TimeBatch,
    TimeSpaceBatch,
)
from cdsobs.ingestion.notify import notify_to_slack
from cdsobs.ingestion.partition import get_partitions, save_partitions
from cdsobs.ingestion.serialize import serialize_partition
from cdsobs.metadata import get_dataset_metadata
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
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
    disable_cdm_tag_check: bool = False,
    slack_notify: bool = False,
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
    disable_cdm_tag_check:
      For testing purposes only. Do not check if the CDM repo is in a tag.
    slack_notify:
        Notify to slack channel defined by CADSOBS_SLACK_CHANNEL and CADSOBS_SLACK_HOOK
        environment variables.
    """
    hostname = socket.gethostname()
    logger.info("----------------------------------------------------------------")
    logger.info(f"Running ingestion pipeline in {hostname}")
    logger.info("----------------------------------------------------------------")
    service_definition = get_service_definition(config, dataset_name)
    _maybe_check_cdm_tag(config, disable_cdm_tag_check)
    run_params = IngestionRunParams(
        dataset_name, source, version, config, service_definition
    )

    def _run_for_batch(time_space_batch):
        try:
            _run_ingestion_pipeline_for_batch(
                run_params,
                session,
                time_space_batch,
            )
        except EmptyBatchException:
            logger.warning(f"Data not found for {time_space_batch=}")
        except (KeyboardInterrupt, MemoryError) as e:
            message = (
                f"Ingestion pipeline for {run_params} {start_year=} {end_year=} "
                f"running at {hostname} as been canceled at {time_space_batch}"
                f"with {e}"
            )
            logger.error(message)
            if slack_notify:
                notify_to_slack(message)
            raise
        except Exception as e:
            message = (
                f"Ingestion pipeline for {run_params} {start_year=} {end_year=} "
                f"running at {hostname} as failed {time_space_batch} with {e}"
            )
            logger.error(message)
            if slack_notify:
                notify_to_slack(message)
            raise

    main_iterator = _get_main_iterator(
        config, dataset_name, source, start_year, end_year, start_month=start_month
    )

    for time_space_batch in main_iterator:
        logger.info(f"Running ingestion pipeline for {time_space_batch}")
        _run_for_batch(time_space_batch)

    # Run sanity check
    _run_sanity_check(
        config, dataset_name, service_definition, session, source, version
    )
    # Log successful, taking the warnings into account
    final_message = _print_final_message(
        dataset_name, source, start_year, end_year, "ingestion pipeline"
    )
    if slack_notify:
        notify_to_slack(final_message)


def _print_final_message(
    dataset_name: str,
    source: str,
    start_year: int,
    end_year: int,
    function_name: Literal["ingestion pipeline", "make cdm"],
) -> str:
    final_message = f"Finished {function_name} for {dataset_name=} {source=} {start_year=} {end_year=} "
    if warning_tracker.warning_logged:
        final_message += "with warnings, please check the log."
    else:
        final_message += "successfully."
    logger.info(
        "--------------------------------------------------------------------------------"
    )
    logger.info(final_message)
    return final_message


def _run_sanity_check(
    config: CDSObsConfig,
    dataset_name: str,
    service_definition: ServiceDefinition,
    session: Session,
    source: str,
    version: str,
):
    catalogue_repo = CatalogueRepository(session)
    entries = catalogue_repo.get_by_dataset_and_source_and_version(
        dataset_name, source, version
    )
    # Check summary
    # The problem is that we don't know beforehand the number of stations or partitions
    # So we cannot check it, just if there are any.
    stats = stats_summary(entries)  # type: ignore
    print(stats)
    assert stats["number of partitions"] >= 1
    assert stats["number of stations"] >= 1
    # This may fail is the make production is partial, and a variable is missing for
    # the years that are being read.
    assert set(stats["available variables"]) == set(
        service_definition.sources[source].main_variables
    )
    # Now we will check the metadata in the largest partition uploaded
    largest_entry = max(entries, key=lambda x: x.data_size)
    s3_client = S3Client.from_config(config.s3config)
    temp_file = tempfile.NamedTemporaryFile()
    bucket_name, object_name = largest_entry.asset.split("/")
    s3_client.download_file(
        bucket_name=bucket_name, object_name=object_name, ofile=temp_file.name
    )
    validate_storage_file(service_definition, source, temp_file.name)


def validate_storage_file(
    service_definition: ServiceDefinition, source: str, file_path: Path | str
):
    dataset = h5netcdf.File(file_path)
    fields = set(dataset.variables)
    expected_fields = set(service_definition.sources[source].descriptions)
    expected_fields = expected_fields - set(
        service_definition.sources[source].main_variables
    )
    expected_fields = expected_fields.union({"observation_value", "observed_variable"})
    if not expected_fields.issubset(fields):
        logger.warning(
            f"{expected_fields - fields} are missing in the uploaded file but"
            f" defined in the service definition."
        )


def compare_sets(a: set, b: set):
    assert a == b, f"Sets are not equal:\n  Only in a: {a - b}\n  Only in b: {b - a}"


def run_make_cdm(
    dataset_name: str,
    source: str,
    config: CDSObsConfig,
    start_year: int,
    end_year: int,
    output_dir: Path,
    save_data: bool = False,
    version: str = DEFAULT_VERSION,
    disable_cdm_tag_check: bool = False,
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
    disable_cdm_tag_check:
      For testing purposes only. Do not check if the CDM repo is in a tag.
    """
    logger.info("----------------------------------------------------------------")
    logger.info("Running make cdm")
    logger.info("----------------------------------------------------------------")
    service_definition = get_service_definition(config, dataset_name)
    _maybe_check_cdm_tag(config, disable_cdm_tag_check)
    run_params = IngestionRunParams(
        dataset_name, source, version, config, service_definition
    )

    def _run_for_batch(time_batch):
        try:
            _run_make_cdm_for_batch(
                run_params,
                output_dir,
                save_data,
                time_batch,
            )
        except EmptyBatchException:
            logger.warning(f"No data found for {time_batch=}")

    main_iterator = _get_main_iterator(
        config, dataset_name, source, start_year, end_year
    )
    for time_space_batch in main_iterator:
        _run_for_batch(time_space_batch)
    _print_final_message(dataset_name, source, start_year, end_year, "make cdm")


def _run_ingestion_pipeline_for_batch(
    run_params: IngestionRunParams,
    session: Session,
    time_space_batch: TimeSpaceBatch,
):
    """
    Ingest the data for a given year and month, specified by TimeBatch.

    Parameters
    ----------
    run_params:
      IngestionRunParams object containing the relevant parameters for the ingestion run:
      dataset_name, source, version and the configuration (service definition and main
      config).
    session :
      Session on the catalogue database
    time_space_batch:
      Optionally read data only for one year and month
    """
    dataset_name = run_params.dataset_name
    source = run_params.source
    version = run_params.version
    config = run_params.config
    if entry_exists(dataset_name, session, source, time_space_batch, version):
        logger.warning(
            "A partition with the chosen parameters already exists and update is set to False."
        )
    else:
        sorted_partitions = _read_homogenise_and_partition(run_params, time_space_batch)
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
    run_params: IngestionRunParams,
    time_space_batch: TimeSpaceBatch,
) -> Iterator[DatasetPartition]:
    dataset_name = run_params.dataset_name
    config = run_params.config
    service_definition = run_params.service_definition
    dataset_config = config.get_dataset(dataset_name)
    source = run_params.source
    dataset_metadata = get_dataset_metadata(run_params)
    # Get the data as a single big table with the names remmaped from
    # service_definition
    homogenised_data = read_batch_data(
        config, dataset_metadata, service_definition, time_space_batch
    )
    # Validate that times are inside the time batch
    _validate_time_interval(homogenised_data, time_space_batch.time_batch)
    # Check CDM compliance
    check_cdm_compliance(homogenised_data, dataset_metadata.cdm_tables)
    # Map and check the units
    homogenised_data = _handle_units(
        homogenised_data, dataset_metadata, service_definition, source
    )
    # Get tile sizes
    year = time_space_batch.time_batch.year
    lon_tile_size = dataset_config.get_tile_size("lon", source, year)
    lat_tile_size = dataset_config.get_tile_size("lat", source, year)
    # Partition and sort the data.
    data_partitions = get_partitions(
        dataset_metadata,
        homogenised_data,
        time_space_batch.time_batch,
        lon_tile_size=lon_tile_size,
        lat_tile_size=lat_tile_size,
    )
    sorted_partitions = (sort(dp) for dp in data_partitions)
    return sorted_partitions


def _handle_units(
    homogenised_data: pandas.DataFrame,
    dataset_metadata: DatasetMetadata,
    service_definition: ServiceDefinition,
    source: str,
) -> pandas.DataFrame:
    # Define units id not present and apply unit changes
    source_definition = service_definition.sources[source]
    if "units" not in homogenised_data.columns:
        homogenised_data = define_units(
            homogenised_data,
            source_definition,
            dataset_metadata.cdm_code_tables["observed_variable"],
        )
    # If units is present but encoded as integers, decode them
    # Old datasets have all units as strings, so we shall keep it like this.
    unit_codes = dataset_metadata.cdm_code_tables["units"]
    code2unit = unit_codes.table["abbreviation"].to_dict()
    code2unit[0] = "none"
    unit_fields = [f for f in homogenised_data.columns if "units" in f]
    for unit_field in unit_fields:
        # Convert if we get units as integers
        if homogenised_data[unit_field].dtype.kind == "i":
            # Decode integers using CDM tables
            decoded_units = homogenised_data[unit_field].map(code2unit)
            # Check for nans
            if decoded_units.isnull().any():
                raise RuntimeError("Not all units were mapped")
            homogenised_data[unit_field] = decoded_units
        # Check the units agains he CDM
        varname2units = get_varname2units(
            dataset_metadata.cdm_code_tables["observed_variable"]
        )
        for variable in source_definition.main_variables:
            mask = homogenised_data["observed_variable"] == variable
            # Check if the variable is available, not all partitions contain all
            # variables
            if mask.any():
                units = homogenised_data.loc[mask, unit_field].iloc[0]
                _check_cdm_units(units, variable, varname2units)
    return homogenised_data


def _validate_time_interval(homogenised_data: pandas.DataFrame, time_batch: TimeBatch):
    start_date, end_date = time_batch.get_time_coverage()
    times_check = between(homogenised_data.report_timestamp, start_date, end_date).all()
    if not times_check:
        raise AssertionError(
            f"Thee times read {homogenised_data.report_timestamp} are outside "
            f"{start_date} {end_date}. Something when wrong with the time selection."
        )


def _run_make_cdm_for_batch(
    run_params: IngestionRunParams,
    output_dir: Path,
    save_data: bool,
    time_space_batch: TimeSpaceBatch,
):
    sorted_partitions = _read_homogenise_and_partition(run_params, time_space_batch)
    if save_data:
        for partition in sorted_partitions:
            serialized_partition = serialize_partition(partition, output_dir)
            logger.info(
                f"Saved partition to "
                f"{serialized_partition.file_params.local_temp_path}"
            )
            logger.info("Validating file")
            validate_storage_file(
                run_params.service_definition,
                run_params.source,
                serialized_partition.file_params.local_temp_path,
            )


def _maybe_check_cdm_tag(config: CDSObsConfig, disable_cdm_tag_check: bool):
    cdm_repo_location = Path(config.cdm_tables_location, "cdm-obs")
    if disable_cdm_tag_check:
        logger.warning(
            f"Using CDM tables from {cdm_repo_location}, which are not in a"
            f"git tag. Tag checking is disabled."
        )
    else:
        cdm_tag = get_cdm_repo_current_tag(cdm_repo_location)
        logger.info(f"Using CDM tables from {cdm_repo_location}, tag is {cdm_tag}")


def set_version_status(
    config: CDSObsConfig, dataset: str, version: str, deprecated: bool
):
    """Change the status of a version for a given dataset."""
    with get_session(config.catalogue_db) as session:
        cads_dataset_version_repo = CadsDatasetVersionRepository(session)
        dataset_version = cads_dataset_version_repo.get_dataset(
            dataset_name=dataset, version=version
        )
        if dataset_version is None:
            raise RuntimeError(f"{dataset=} {version=} not found in the catalogue")
        dataset_version.deprecated = deprecated
        session.commit()
        verb = "Deprecated" if deprecated else "Enabled"
        logger.info(f"{verb} {dataset=} {version=}")
