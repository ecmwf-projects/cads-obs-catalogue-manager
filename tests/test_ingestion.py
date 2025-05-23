import pytest
import pytest_mock.plugin

from cdsobs.ingestion.partition import (
    get_partition_status,
    save_partitions,
    upload_partition,
)
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.repositories.dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)


def test_save_partitions(
    test_session_pertest, test_s3_client, test_partition, test_config
):
    cads_dataset_repo = CadsDatasetRepository(test_session_pertest)
    cads_dataset_repo.create_dataset(test_partition.dataset_metadata.name)
    cads_dataset_version_repo = CadsDatasetVersionRepository(test_session_pertest)
    cads_dataset_version_repo.create_dataset_version(
        test_partition.dataset_metadata.name, test_partition.dataset_metadata.version
    )
    save_partitions(test_session_pertest, test_s3_client, [test_partition])
    result = CatalogueRepository(test_session_pertest).get_all()
    assert result[0].dataset == test_partition.dataset_metadata.name
    assert result[0].dataset_source == test_partition.dataset_metadata.dataset_source
    assert (
        result[0].time_coverage_start
        == test_partition.partition_params.time_coverage_start
    )
    assert (
        result[0].time_coverage_end == test_partition.partition_params.time_coverage_end
    )
    assert (
        result[0].latitude_coverage_start
        == test_partition.partition_params.latitude_coverage_start
    )
    assert (
        result[0].longitude_coverage_start
        == test_partition.partition_params.longitude_coverage_start
    )
    assert result[0].variables == test_partition.dataset_metadata.variables
    assert "insitu-observations" in result[0].asset


def test_get_partition_status(
    test_session_pertest, test_s3_client_pertest, test_serialized_partition, tmp_path
):
    test_s3_client = test_s3_client_pertest
    status = get_partition_status(
        test_session_pertest,
        test_s3_client,
        test_serialized_partition.dataset_metadata.name,
        test_serialized_partition.file_params,
    )
    assert status == "new"
    # Upload and check again
    cads_dataset_repo = CadsDatasetRepository(test_session_pertest)
    cads_dataset_repo.create_dataset(test_serialized_partition.dataset_metadata.name)
    cads_dataset_repo = CadsDatasetVersionRepository(test_session_pertest)
    cads_dataset_repo.create_dataset_version(
        test_serialized_partition.dataset_metadata.name,
        test_serialized_partition.dataset_metadata.version,
    )
    upload_partition(test_session_pertest, test_serialized_partition, test_s3_client)
    status = get_partition_status(
        test_session_pertest,
        test_s3_client,
        test_serialized_partition.dataset_metadata.name,
        test_serialized_partition.file_params,
    )
    assert status == "exists_identical"
    # Modify the checksum and check again
    test_serialized_partition.file_params.file_checksum = "sgfadsgfadf"
    status = get_partition_status(
        test_session_pertest,
        test_s3_client,
        test_serialized_partition.dataset_metadata.name,
        test_serialized_partition.file_params,
    )
    assert status == "exists_different"


def test_upload_partition(
    test_session_pertest, test_serialized_partition, test_s3_client_pertest
):
    test_s3_client = test_s3_client_pertest
    cads_dataset_repo = CadsDatasetRepository(test_session_pertest)
    cads_dataset_repo.create_dataset(test_serialized_partition.dataset_metadata.name)
    cads_dataset_repo = CadsDatasetVersionRepository(test_session_pertest)
    cads_dataset_repo.create_dataset_version(
        test_serialized_partition.dataset_metadata.name,
        test_serialized_partition.dataset_metadata.version,
    )
    upload_partition(test_session_pertest, test_serialized_partition, test_s3_client)
    dataset_name = test_serialized_partition.dataset_metadata.name
    bucket_name = test_s3_client.get_bucket_name(dataset_name)
    bucket_objects = list(test_s3_client.list_directory_objects(bucket_name))
    assert len(bucket_objects) == 1
    assert len(list(CatalogueRepository(test_session_pertest).get_all()))


def test_rollback(
    mocker: pytest_mock.plugin.MockerFixture,
    test_session_pertest,
    test_serialized_partition,
    test_s3_client_pertest,
):
    test_s3_client = test_s3_client_pertest

    class MockedException(Exception):
        pass

    # making creation fail and asserting nothing has been inserted into both DBs
    with pytest.raises(MockedException):
        mocker.patch.object(
            CatalogueRepository, "create", side_effect=MockedException("mocked error")
        )
        upload_partition(
            test_session_pertest, test_serialized_partition, test_s3_client
        )
    dataset_name = test_serialized_partition.dataset_metadata.name
    bucket_name = test_s3_client.get_bucket_name(dataset_name)
    bucket_objects = list(test_s3_client.list_directory_objects(bucket_name))
    assert len(list(bucket_objects)) == 0
    assert len(CatalogueRepository(test_session_pertest).get_all()) == 0
