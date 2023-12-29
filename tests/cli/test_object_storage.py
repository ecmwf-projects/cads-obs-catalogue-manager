import pytest_mock
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
    capsys,
):
    catalogue_repo = CatalogueRepository(test_session)
    mocker.patch.object(test_s3_client, "object_exists", return_value=True)
    check_if_missing_in_object_storage(catalogue_repo, test_s3_client, DS_TEST_NAME)
    captured_out = capsys.readouterr().out
    assert "Found all assets in object storage" in captured_out


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
    check_if_missing_in_catalogue(catalogue_repo, test_s3_client)
    captured_out = capsys.readouterr().out
    assert "Missing" in captured_out
    assert "test_bucket/test_object entry in catalogue" in captured_out
