import pytest
from typer.testing import CliRunner

from cdsobs.cli._catalogue_explorer import list_catalogue_
from cdsobs.cli.app import app
from cdsobs.observation_catalogue.schemas.catalogue import CliCatalogueFilters
from tests.conftest import CONFIG_YML, DS_TEST_NAME

runner = CliRunner()


@pytest.mark.parametrize("print_format", ["table", "json"])
def test_list_catalogue(test_session, test_repository, print_format):
    result = runner.invoke(
        app,
        ["list-catalogue", "-c", CONFIG_YML, "--print-format", print_format],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_catalogue_dataset_info(test_session, test_repository):
    result = runner.invoke(
        app,
        ["catalogue-dataset-info", DS_TEST_NAME, "-c", CONFIG_YML],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


@pytest.mark.parametrize("print_format", ["table", "json"])
def test_list_datasets(test_repository, print_format):
    result = runner.invoke(
        app,
        ["list-datasets", "-c", CONFIG_YML, "--print-format", print_format],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_get_catalogue_list(test_repository):
    # empty filters
    filters = CliCatalogueFilters(
        dataset="",
        dataset_source="",
        time=[],
        latitudes=[],
        longitudes=[],
        variables=[],
        stations=[],
        versions=[],
    )
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results)
    # filled filters
    filters = CliCatalogueFilters(
        dataset="insitu-observations-woudc-ozone-total-column-and-profiles",
        dataset_source="OzoneSonde",
        time=["1969-1-15"],
        latitudes=[45],
        longitudes=[100],
        variables=["air_pressure", "air_temperature"],
        stations=["7"],
        versions=["1.0.0"],
    )
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results) == 1
    # time interval for two partitions (two months)
    filters.time = ["1968-12-31", "1969-3-3"]
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results) == 1
    # list deprecated versions (there is none)
    filters.deprecated = True
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results) == 0
    # list all (deprecated or now) versions
    filters.deprecated = "all"
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results) == 1
    # list non existing version
    filters.versions = ["2.0.0"]
    results = list_catalogue_(test_repository.catalogue_repository.session, filters)
    assert len(results) == 0
