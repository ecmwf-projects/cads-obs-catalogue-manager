from datetime import datetime, timezone

import sqlalchemy as sa

from cdsobs.observation_catalogue.models import CadsDataset, Catalogue
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.catalogue import CatalogueSchema

test_catalogue_record = CatalogueSchema(
    dataset="test_dataset",
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


def test_raw_insert(test_session_pertest):
    test_session = test_session_pertest
    cads_dataset_repo = CadsDatasetRepository(test_session)
    cads_dataset_repo.create_dataset(test_catalogue_record.dataset)
    catalogue_repo = CatalogueRepository(session=test_session)
    catalogue_repo.create(test_catalogue_record)
    assert test_session.scalar(sa.select(sa.func.count(Catalogue.id))) == 1


def test_cads_dataset(test_session_pertest):
    test_session = test_session_pertest
    cads_dataset_repo = CadsDatasetRepository(test_session)
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    cads_dataset_repo.create_dataset(dataset_name)
    results = test_session.scalars(sa.select(CadsDataset)).all()
    assert len(results) == 1
    cads_dataset_repo.bump_dataset_version(dataset_name)
    assert results[0].version == "2.0"


def test_entry_exists(test_session_pertest):
    test_session = test_session_pertest
    cads_dataset_repo = CadsDatasetRepository(test_session)
    cads_dataset_repo.create_dataset(test_catalogue_record.dataset)
    catalogue_repo = CatalogueRepository(session=test_session)
    catalogue_repo.create(test_catalogue_record)
    exists = catalogue_repo.entry_exists(
        test_catalogue_record.dataset,
        test_catalogue_record.dataset_source,
        test_catalogue_record.time_coverage_start,
        test_catalogue_record.time_coverage_end,
        test_catalogue_record.longitude_coverage_start,
        test_catalogue_record.longitude_coverage_end,
        test_catalogue_record.latitude_coverage_start,
        test_catalogue_record.latitude_coverage_end,
    )
    assert exists


def test_entry_exists_ko(test_session_pertest):
    test_session = test_session_pertest
    exists = CatalogueRepository(test_session).entry_exists(
        test_catalogue_record.dataset,
        test_catalogue_record.dataset_source,
        test_catalogue_record.time_coverage_start,
        test_catalogue_record.time_coverage_end,
        test_catalogue_record.longitude_coverage_start,
        test_catalogue_record.longitude_coverage_end,
        test_catalogue_record.latitude_coverage_start,
        test_catalogue_record.latitude_coverage_end,
    )
    assert not exists
