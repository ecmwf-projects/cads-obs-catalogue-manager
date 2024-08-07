import pytest_mock
from structlog.testing import capture_logs
from typer.testing import CliRunner

from cdsobs.cli._object_storage import (
    check_if_missing_in_catalogue,
    check_if_missing_in_object_storage,
)
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from tests.conftest import DS_TEST_NAME

runner = CliRunner()


def test_check_if_missing_in_storage(
    mocker: pytest_mock.plugin.MockerFixture,
    test_session,
    test_s3_client,
    test_repository,
):
    catalogue_repo = CatalogueRepository(test_session)
    mocker.patch.object(test_s3_client, "object_exists", return_value=True)
    with capture_logs() as cap_logs:
        check_if_missing_in_object_storage(catalogue_repo, test_s3_client, DS_TEST_NAME)
    assert cap_logs == [
        {"event": "Found all assets in object storage.", "log_level": "info"}
    ]


def test_check_if_missing_in_catalogue(
    mocker: pytest_mock.plugin.MockerFixture, test_session, test_s3_client, capsys
):
    # missing example
    catalogue_repo = CatalogueRepository(test_session)
    mocker.patch.object(
        test_s3_client, "list_buckets", side_effect=[["test_bucket"], []]
    )
    mocker.patch.object(
        test_s3_client, "list_directory_objects", side_effect=[["test_object"], []]
    )
    with capture_logs() as cap_logs:
        check_if_missing_in_catalogue(catalogue_repo, test_s3_client)
    assert cap_logs[-1] == {
        "event": "Missing test_bucket/test_object entry in catalogue.",
        "log_level": "warning",
    }
