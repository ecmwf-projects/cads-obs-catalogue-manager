import tempfile
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Tuple, cast

import pandas
import sqlalchemy as sa
from sqlalchemy.orm import Session

from cdsobs.cdm.tables import STATION_COLUMN
from cdsobs.ingestion.core import (
    DatasetMetadata,
    DatasetPartition,
    FileParams,
    PartitionParams,
    SerializedPartition,
    TimeBatch,
    to_catalogue_record,
)
from cdsobs.ingestion.merge import merge_with_existing_partition
from cdsobs.ingestion.serialize import serialize_partition, to_storage
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.constraints import get_partition_constraints
from cdsobs.storage import StorageClient
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.types import BoundedLat, BoundedLon

logger = get_logger(__name__)


def get_partitions(
    dataset_params: DatasetMetadata,
    data: pandas.DataFrame,
    time_batch: TimeBatch,
    lon_tile_size: int = 5,
    lat_tile_size: int = 5,
) -> Iterator[DatasetPartition]:
    """
    Partition the data in regular tiles in time and space.

    Parameters
    ----------
    dataset_params :
      Contains some metadata to identify the dataset
    data :
      Main data table from the previous step (validate_and_homogenise).
    time_batch:
      Defines the time interval of this data.
    lon_tile_size :
      Partition size along the longitude axis in degrees.
    lat_tile_size :
      Partition size along the latitude axis in degrees.

    Yields
    ------
    Partition object with the data and several useful attributes.

    """
    # Group by month and year and sort
    time_column = "report_timestamp"
    # Check which coordinate names is this dataset using
    latlon_names = dataset_params.space_columns
    latname, lonname = latlon_names.y, latlon_names.x
    # We avoid negative values so we can support 180 and 360 (global) as tile sizes
    data["latitude_coverage_start"] = (
        (data[latname] + 90) // lat_tile_size
    ) * lat_tile_size - 90
    data["longitude_coverage_start"] = (
        (data[lonname] + 180) // lon_tile_size
    ) * lon_tile_size - 180
    group_columns: Any = [
        "latitude_coverage_start",
        "longitude_coverage_start",
    ]
    grouped = data.groupby(group_columns, sort=False)
    for n, (group_name, group_data) in enumerate(grouped, start=1):
        logger.info(
            f"Processing partition {n} of {grouped.ngroups} first lat: {group_name[0]} "
            f"first_lon: ${group_name[1]}"
        )
        group_name_typed = cast(Tuple[BoundedLat, BoundedLon], group_name)
        station_ids = sorted(group_data[STATION_COLUMN].unique().astype("str").tolist())
        logger.info("Computing constraints.")
        constraints = get_partition_constraints(group_data, time_column=time_column)
        sources: list[str] = []
        partition_params = PartitionParams(
            time_batch,
            group_name_typed[0],
            group_name_typed[1],
            lat_tile_size,
            lon_tile_size,
            station_ids,
            sources,
        )
        yield DatasetPartition(
            dataset_params, partition_params, group_data, constraints
        )


def save_partitions(
    db_session: Session,
    storage_client: StorageClient,
    partitions: Iterable[DatasetPartition],
):
    """
    Save partitions to storage and catalogue.

    Loops over the partitions iterable uploading them to the cataloge and the storage.

    Parameters
    ----------
    db_session :
      Session to the catalogue database.
    storage_client :
      Client providing an interface to the storage.
    partitions :
      Partitions yielded from the previous ingestion steps.
    """
    logger.info("Reading Observations Common Data Model tables")
    with tempfile.TemporaryDirectory() as tempdir:
        for partition in partitions:
            _save_partition(db_session, partition, storage_client, tempdir)


def _save_partition(
    db_session: Session,
    partition: DatasetPartition,
    storage_client: StorageClient,
    tempdir: str,
):
    """Save one partition to storage.

    If the partition already exists in the storage and it is different, a merge is
    carried out.
    """
    serialized_partition = serialize_partition(partition, Path(tempdir))
    # Check the status of the partition in the storage & catalogue
    # Can be "new", "exists_identical" or "exists_different".
    partition_status = get_partition_status(
        db_session,
        storage_client,
        partition.dataset_metadata.name,
        serialized_partition.file_params,
    )
    # Handle update logic
    match partition_status:
        case "exists_identical":
            logger.info("An identical partition has been already uploaded, skipping.")
            return None
        case "new":
            partition_to_upload = serialized_partition
        case "exists_different":
            partition_to_upload, has_conflicts = merge_with_existing_partition(
                serialized_partition, storage_client
            )
            if has_conflicts:
                # This means that we are updating already stored data records.
                # This is a new version of the dataset
                cads_dataset_repository = CadsDatasetRepository(db_session)
                dataset_name = partition.dataset_metadata.name
                cads_dataset_repository.bump_dataset_version(dataset_name)
        case _:
            raise RuntimeError(f"{partition_status} is an invalid status for partition")
    # Upload
    upload_partition(db_session, partition_to_upload, storage_client)


def get_partition_status(
    db_session: Session,
    storage_client: StorageClient,
    dataset_name: str,
    file_params: FileParams,
) -> Literal["exists_identical", "new", "exists_different"]:
    """Return wether the partition already exists in the storage and if it is different."""
    logger.info("Getting partition status")
    bucket_name = storage_client.get_bucket_name(dataset_name)
    partition_asset = storage_client.get_asset(
        bucket_name, file_params.local_temp_path.name
    )
    partition_catalogue_record = db_session.scalars(
        sa.select(Catalogue).filter(Catalogue.asset == partition_asset).limit(1)
    ).first()
    if partition_catalogue_record:
        # Partition exists
        if file_params.file_checksum == partition_catalogue_record.file_checksum:
            logger.info(
                "This partition already exists and files are identical, skipping"
            )
            return "exists_identical"
        else:
            logger.info(
                "This partition already exists and files are different, merging"
            )
            return "exists_different"
    else:
        logger.info("This partition is new, uploading")
        return "new"


def upload_partition(
    db_session: Session,
    partition: SerializedPartition,
    storage_client: StorageClient,
):
    """Upload data to storage and catalogue database."""
    logger.debug("Uploading to object storage")
    # Upload to S3
    asset = to_storage(
        storage_client,
        partition.dataset_metadata.name,
        partition.file_params.local_temp_path,
    )
    logger.debug(f"Uploaded file {asset}")
    try:
        # Save to catalogue
        catalogue_record = to_catalogue_record(partition, asset)
        catalogue_repository = CatalogueRepository(session=db_session)
        logger.debug(f"Commited {catalogue_record=} to catalogue database")
        catalogue_repository.create(obj_in=catalogue_record)
    except (Exception, KeyboardInterrupt):
        # rollback
        bucket_name = storage_client.get_bucket_name(partition.dataset_metadata.name)
        storage_client.delete_file(
            bucket_name, partition.file_params.local_temp_path.name
        )
        logger.error(
            "Error when uploading to Catalogue/Storage. Changes have been "
            "rolled back to ensure consistency."
        )
        raise
