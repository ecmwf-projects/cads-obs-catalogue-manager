import pytest

from cdsobs.cdm.api import open_serialized_partition
from cdsobs.ingestion.merge import merge_with_existing_partition
from cdsobs.ingestion.partition import upload_partition
from cdsobs.ingestion.serialize import to_netcdf
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.utils.utils import compute_hash, get_file_size


@pytest.mark.parametrize("add_conflict", [True, False])
def test_merge_with_existing_partition(
    test_serialized_partition, test_catalogue_repository, test_s3_client, add_conflict
):
    cads_dataset_repo = CadsDatasetRepository(test_catalogue_repository.session)
    cads_dataset_repo.create_dataset(test_serialized_partition.dataset_metadata.name)
    upload_partition(
        test_catalogue_repository.session,
        test_serialized_partition,
        test_s3_client,
    )
    # Open the partition dataset
    cdm_dataset = open_serialized_partition(test_serialized_partition)
    # Add a new record in the observations table
    data = cdm_dataset.dataset
    # We just copy the last record and add one to the primary key
    data.iloc[-1]
    last_index = data.index[-1]
    last_index + 1
    data_modified = data.copy()
    if add_conflict:
        # Add a conflict too
        data_modified.loc[last_index, "observation_value"] = 5.2
    # Cast back to float32 to avoid annoying warning
    data_modified["observation_value"] = data_modified["observation_value"].astype(
        "float32"
    )
    cdm_dataset.dataset = data_modified
    # Write the modified netcdf
    tempdir = test_serialized_partition.file_params.local_temp_path.parent
    to_netcdf(cdm_dataset, tempdir, encode_variables=True)
    # Test merge
    merged_partition, has_conflict = merge_with_existing_partition(
        test_serialized_partition, test_s3_client
    )
    assert merged_partition.file_params.file_size == get_file_size(
        merged_partition.file_params.local_temp_path
    )
    assert merged_partition.file_params.file_checksum == compute_hash(
        merged_partition.file_params.local_temp_path
    )
    assert has_conflict == add_conflict
