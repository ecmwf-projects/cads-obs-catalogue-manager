import logging
from copy import copy
from dataclasses import asdict
from typing import Tuple

import pandas

from cdsobs.cdm.api import CdmDataset, open_asset, open_netcdf
from cdsobs.ingestion.core import PartitionParams, SerializedPartition
from cdsobs.ingestion.serialize import get_file_params, to_netcdf
from cdsobs.observation_catalogue.schemas.constraints import (
    ConstraintsSchema,
    get_partition_constraints,
)
from cdsobs.storage import StorageClient


def update_constraints(
    merged_dataset: pandas.DataFrame,
    partition_params: PartitionParams,
) -> ConstraintsSchema:
    """
    Update the constraints of a partition from a new, modified dataset.

    Parameters
    ----------
    merged_dataset :
      New data tables.
    partition_params :
      Parameters of the partition.

    Returns
    -------
    New updated constraints as a ConstraintsSchema object.
    """
    group_columns = [
        "year",
        "month",
        "latitude_coverage_start",
        "longitude_coverage_start",
    ]
    {k: v for k, v in asdict(partition_params).items() if k in group_columns}
    merged_dataset["year"] = merged_dataset.report_timestamp.dt.year
    merged_dataset["month"] = merged_dataset.report_timestamp.dt.month
    merged_dataset["latitude_coverage_start"] = partition_params.latitude_coverage_start
    merged_dataset[
        "longitude_coverage_start"
    ] = partition_params.longitude_coverage_start
    updated_constraints = get_partition_constraints(merged_dataset)
    return updated_constraints


def merge_with_existing_partition(
    new_partition: SerializedPartition,
    storage_client: StorageClient,
) -> Tuple[SerializedPartition, bool]:
    """
    Merge the data from a new partition with the data already existing in the storage.

    Parameters
    ----------
    new_partition :
      New partition data file and parameters.
    storage_client :
      Storage client to download the old data from the storage.
    cdm_tables :
      CDM tables definition.

    Returns
    -------
    A tuple containing the new partition data file and parameters, and a boolean telling
    if conflicts have been found (i.e. if old data has been ovewritten by new).
    """
    # Path to the file with the new/updated data
    new_local_file = new_partition.file_params.local_temp_path
    # Url of the asset of the partition
    bucket = storage_client.get_bucket_name(new_partition.dataset_metadata.name)
    partition_asset = storage_client.get_object_url(
        bucket,
        new_partition.file_params.local_temp_path.name,
    )
    new_dataset = open_netcdf(new_local_file, decode_variables=True)
    dataset_in_storage = open_asset(partition_asset, decode_variables=True)
    dataset_in_storage = dataset_in_storage.to_pandas()
    new_index = new_dataset.index
    old_index = dataset_in_storage.index
    intersection = old_index.intersection(new_index)
    new_indices = new_index.difference(old_index)
    indexes_overlap = len(intersection) > 1

    if indexes_overlap:
        if dataset_in_storage.loc[intersection].equals(new_dataset.loc[intersection]):
            # overlapping indexes are equal, concat the new ones
            logging.info("Overlapping data is equal, there are no conflicts")
            merged_data = pandas.concat(
                [dataset_in_storage, new_dataset.loc[new_indices]]
            )
            has_conflicts = False
        else:
            # We fix the conflict replacing the values in the intersection by the
            # new ones, then concat
            dataset_in_storage.loc[intersection] = new_dataset.loc[
                intersection, dataset_in_storage.columns
            ]  # type: ignore
            merged_data = pandas.concat(
                [dataset_in_storage, new_dataset.loc[new_indices]]
            )
            has_conflicts = True
    else:
        merged_data = pandas.concat([dataset_in_storage, new_dataset])
        has_conflicts = False

    # Write the new merged file to the same path as the old
    new_cdm_dataset = CdmDataset(
        new_dataset, new_partition.partition_params, new_partition.dataset_metadata
    )
    to_netcdf(new_cdm_dataset, new_local_file.parent)
    # Check relational integrity of the new partition dataset
    # check_relational_integrity(cdm_tables, new_cdm_dataset)
    # Update file params, stations and constraints
    new_file_params = get_file_params(new_local_file, new_cdm_dataset)
    new_partition_params = copy(new_partition.partition_params)
    if merged_data.primary_station_id.dtype.kind == "S":
        new_station_ids = merged_data.primary_station_id.str.decode("UTF-8")
    else:
        new_station_ids = merged_data.primary_station_id.index.astype(str)
    new_station_ids = new_station_ids.unique().tolist()
    new_partition_params.stations_ids = new_station_ids
    new_dataset_params = copy(new_partition.dataset_metadata)
    new_constraints = update_constraints(merged_data, new_partition.partition_params)
    # Return the merged partition
    merged_partition = SerializedPartition(
        new_file_params,
        new_partition_params,
        new_dataset_params,
        new_constraints,
    )
    return merged_partition, has_conflicts
