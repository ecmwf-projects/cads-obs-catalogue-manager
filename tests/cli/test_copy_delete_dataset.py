import pytest
from typer.testing import CliRunner

from build.lib.cdsobs.constants import DEFAULT_VERSION
from cdsobs.cli._copy_dataset import s3_export
from cdsobs.cli.app import app
from cdsobs.constants import DS_TEST_NAME, SOURCE_TEST_NAME
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from tests.conftest import CONFIG_YML

runner = CliRunner()


def test_copy_delete_dataset_inside(test_repository, test_config):
    result = runner.invoke(
        app,
        [
            "copy-dataset",
            "-c",
            CONFIG_YML,
            "--dataset",
            DS_TEST_NAME,
            "--dest-dataset",
            "test",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 2
    dest_bucket = test_repository.s3_client.get_bucket_name("test")
    assert len(list(test_repository.s3_client.list_directory_objects(dest_bucket))) == 2
    result = runner.invoke(
        app,
        [
            "delete-dataset",
            "-c",
            CONFIG_YML,
            "--dataset",
            "test",
            "--dataset-source",
            SOURCE_TEST_NAME,
            "--time",
            "1969-01-01,1970-12-31",
            "--version",
            DEFAULT_VERSION,
        ],
        input="test",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 1
    assert len(list(test_repository.s3_client.list_directory_objects(dest_bucket))) == 1


@pytest.mark.skip(reason="this test does get stuck in github CI for some reason")
def test_s3_export(test_repository):
    entries = test_repository.catalogue_repository.get_by_dataset(DS_TEST_NAME)
    s3_export(test_repository.s3_client, test_repository.s3_client, entries, "test")
    s3_client = test_repository.s3_client
    origin_bucket = s3_client.get_bucket_name(DS_TEST_NAME)
    dest_bucket = s3_client.get_bucket_name("test")
    origin_objects = list(
        test_repository.s3_client.list_directory_objects(origin_bucket)
    )
    dest_objects = list(test_repository.s3_client.list_directory_objects(dest_bucket))
    assert len(origin_objects) == len(dest_objects)
