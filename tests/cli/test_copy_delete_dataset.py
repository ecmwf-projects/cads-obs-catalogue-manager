import pytest
from typer.testing import CliRunner

from cdsobs.cli._copy_dataset import s3_export
from cdsobs.cli.app import app
from cdsobs.constants import DEFAULT_VERSION, DS_TEST_NAME, SOURCE_TEST_NAME
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.repositories.dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)
from tests.conftest import CONFIG_YML

runner = CliRunner()


def test_copy_delete_dataset_inside(test_repository, test_config):
    # Copy a dataset to a new one named test
    copy_invoke_params = [
        "copy-dataset",
        "-c",
        CONFIG_YML,
        "--dataset",
        DS_TEST_NAME,
        "--dest-dataset",
        "test",
        "--version",
        DEFAULT_VERSION,
    ]
    # Check dry run
    result = runner.invoke(
        app,
        copy_invoke_params + ["--dry-run"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert not CadsDatasetRepository(test_session).dataset_exists("test")
    # Actual copy
    result = runner.invoke(
        app,
        copy_invoke_params,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 2
    dest_bucket = test_repository.s3_client.get_bucket_name("test")
    assert len(list(test_repository.s3_client.list_directory_objects(dest_bucket))) == 5
    # Copy again to check that existing entries are not copied
    result = runner.invoke(
        app,
        copy_invoke_params,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 2
    dest_bucket = test_repository.s3_client.get_bucket_name("test")
    assert len(list(test_repository.s3_client.list_directory_objects(dest_bucket))) == 5
    # Test delete, with dry run and without
    delete_invoke_params = [
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
    ]
    # Dry run
    result = runner.invoke(
        app,
        delete_invoke_params + ["--dry-run"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 2
    # Actual delete, only one entry
    result = runner.invoke(
        app,
        delete_invoke_params,
        input="test",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert len(CatalogueRepository(test_session).get_by_dataset("test")) == 1
    assert len(list(test_repository.s3_client.list_directory_objects(dest_bucket))) == 4
    # Delete all
    delete_all_invoke_params = [
        "delete-dataset",
        "-c",
        CONFIG_YML,
        "--dataset",
        "test",
        "--dataset-source",
        "TotalOzone",
        "--version",
        DEFAULT_VERSION,
    ]
    result = runner.invoke(
        app,
        delete_all_invoke_params,
        input="test",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        assert not CadsDatasetRepository(test_session).dataset_exists("test")

    # Ensure deleting GRUAN 1.0.0 does not touch GRUAN 2.0.0
    gruan_dataset = "insitu-observations-gruan-reference-network"
    gruan_source = "GRUAN"
    gruan_version_one = DEFAULT_VERSION
    gruan_version_two = "2.0.0"
    with get_session(test_config.catalogue_db) as test_session:
        catalogue_repo = CatalogueRepository(test_session)
        gruan_v1_count = len(
            catalogue_repo.get_by_dataset_and_version(gruan_dataset, gruan_version_one)
        )
        gruan_v2_count = len(
            catalogue_repo.get_by_dataset_and_version(gruan_dataset, gruan_version_two)
        )
        assert gruan_v1_count > 0
        assert gruan_v2_count > 0
        assert (
            CadsDatasetVersionRepository(test_session).get_dataset_version(
                gruan_dataset, gruan_version_two
            )
            is not None
        )
    delete_gruan_invoke_params = [
        "delete-dataset",
        "-c",
        CONFIG_YML,
        "--dataset",
        gruan_dataset,
        "--dataset-source",
        gruan_source,
        "--version",
        gruan_version_one,
    ]
    result = runner.invoke(
        app,
        delete_gruan_invoke_params,
        input=gruan_dataset,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    with get_session(test_config.catalogue_db) as test_session:
        catalogue_repo = CatalogueRepository(test_session)
        assert (
            len(
                catalogue_repo.get_by_dataset_and_version(
                    gruan_dataset, gruan_version_one
                )
            )
            == 0
        )
        assert (
            len(
                catalogue_repo.get_by_dataset_and_version(
                    gruan_dataset, gruan_version_two
                )
            )
            == gruan_v2_count
        )
        assert (
            CadsDatasetVersionRepository(test_session).get_dataset_version(
                gruan_dataset, gruan_version_two
            )
            is not None
        )


@pytest.mark.skip(reason="this test does get stuck in github CI for some reason")
def test_s3_export(test_repository):
    entries = test_repository.catalogue_repository.get_by_dataset(DS_TEST_NAME)
    assets = [e.asset for e in entries]
    s3_export(test_repository.s3_client, test_repository.s3_client, assets, "test")
    s3_client = test_repository.s3_client
    origin_bucket = s3_client.get_bucket_name(DS_TEST_NAME)
    dest_bucket = s3_client.get_bucket_name("test")
    origin_objects = list(
        test_repository.s3_client.list_directory_objects(origin_bucket)
    )
    origin_objects = [
        oo
        for oo in origin_objects
        if ".json" not in origin_objects or ".yml" in origin_objects
    ]
    dest_objects = list(test_repository.s3_client.list_directory_objects(dest_bucket))
    assert len(origin_objects) == len(dest_objects)
