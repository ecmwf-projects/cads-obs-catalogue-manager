from datetime import datetime, timezone

from pydantic_extra_types.semantic_version import SemanticVersion

from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.repositories.dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)
from cdsobs.observation_catalogue.schemas.catalogue import CatalogueSchema

test_catalogue_record = CatalogueSchema(
    dataset="test_dataset",
    version=SemanticVersion.parse("1.0.0"),
    dataset_source="test_dataset_source",
    time_coverage_start=datetime(2022, 1, 1),
    time_coverage_end=datetime(2022, 1, 31),
    latitude_coverage_start=20,  # type: ignore
    latitude_coverage_end=50,  # type: ignore
    longitude_coverage_start=20,  # type: ignore
    longitude_coverage_end=50,  # type: ignore
    variables=["tas"],
    stations=["test_station"],
    sources=["test_source"],
    asset="path_to_asset",
    file_size=1,  # type: ignore
    data_size=1,  # type: ignore
    file_checksum="7dea0c08a12829682e2a581930284858",
    constraints=dict(
        time=[datetime(1969, 1, 30, 0, 0, tzinfo=timezone.utc)],
        variable_constraints={"57": [0], "117": [0], "126": [0], "150": [0]},
        dims=["stations", "time"],
    ),  # type: ignore
)


def test_repos(test_session_pertest):
    # Test the repository objects that we use to interact with the catalogue
    test_session = test_session_pertest
    cads_dataset_repo = CadsDatasetRepository(test_session)
    cads_dataset_repo.create_dataset(test_catalogue_record.dataset)
    cads_dataset_version_repo = CadsDatasetVersionRepository(test_session)
    cads_dataset_version_repo.create_dataset_version(
        test_catalogue_record.dataset, version=str(test_catalogue_record.version)
    )
    catalogue_repo = CatalogueRepository(session=test_session)
    catalogue_repo.create(test_catalogue_record)
    assert len(catalogue_repo.get_all()) == 1
    all_dataset_versions = cads_dataset_version_repo.get_all()
    assert len(all_dataset_versions) == 1
    assert all_dataset_versions[0].name == str(test_catalogue_record.version)
    assert all_dataset_versions[0].dataset == test_catalogue_record.dataset
    all_datasets = cads_dataset_repo.get_all()
    assert not all_dataset_versions[0].deprecated
    assert len(all_datasets) == 1
    assert all_datasets[0].name == test_catalogue_record.dataset
    entry_exists = catalogue_repo.entry_exists(
        test_catalogue_record.dataset,
        test_catalogue_record.dataset_source,
        test_catalogue_record.time_coverage_start,
        test_catalogue_record.time_coverage_end,
        test_catalogue_record.longitude_coverage_start,
        test_catalogue_record.longitude_coverage_end,
        test_catalogue_record.latitude_coverage_start,
        test_catalogue_record.latitude_coverage_end,
        str(test_catalogue_record.version),
    )
    assert entry_exists
    dataset_exists = cads_dataset_repo.dataset_exists(
        dataset_name=test_catalogue_record.dataset
    )
    assert dataset_exists
    dataset_version_exists = cads_dataset_version_repo.dataset_version_exists(
        dataset_name=test_catalogue_record.dataset,
        version=str(test_catalogue_record.version),
    )
    assert dataset_version_exists

    entry_not_exists = catalogue_repo.entry_exists(
        test_catalogue_record.dataset,
        test_catalogue_record.dataset_source,
        test_catalogue_record.time_coverage_start,
        test_catalogue_record.time_coverage_end,
        test_catalogue_record.longitude_coverage_start,
        test_catalogue_record.longitude_coverage_end,
        test_catalogue_record.latitude_coverage_start,
        test_catalogue_record.latitude_coverage_end,
        "3.0.0",
    )
    assert not entry_not_exists
