from pathlib import Path
from typing import Sequence

import sqlalchemy
import typer
from rich.console import Console
from rich.table import Table

from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import Base, get_session
from cdsobs.observation_catalogue.models import (
    CadsDataset,
    Catalogue,
    row_to_json,
)
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.catalogue import CliCatalogueFilters
from cdsobs.utils.logutils import ConfigError, get_logger

from ._utils import (
    PAGE_SIZE,
    CliException,
    ConfigNotFound,
    config_yml_typer,
    list_parser,
    print_format_msg,
)

logger = get_logger(__name__)
console = Console()


def print_db_results(results: Sequence[Base], print_format: str, clazz=Catalogue):
    match print_format:
        case "table":
            fields = [c.key for c in clazz.__table__.columns]
            table = Table(*fields)
            for result in results:
                # Exclude the constraints from here as they take too much to load.
                table.add_row(
                    *[str(getattr(result, f)) for f in fields if f != "constraints"]
                )
            console.print(table)
        case "json":
            console.print([row_to_json(r) for r in results])
        case _:
            raise CliException("Invalid format: options are 'table' or 'json'")


def list_catalogue(
    cdsobs_config_yml: Path = config_yml_typer,
    page: int = typer.Option(
        1, help="Results are paginated by 50 entries, choose a page number"
    ),
    dataset: str = typer.Option("", help="filter by dataset name"),
    source: str = typer.Option("", help="filter by source name"),
    time: str = typer.Option(
        "",
        help="Filter by an exact date or by an interval of two dates. For example: "
        "to retrieve all partitions of year 1970: 1970-1-1,1970-12-31",
    ),
    latitudes: str = typer.Option(
        "", help="Filter by an exact latitude or by an interval of two latitudes"
    ),
    longitudes: str = typer.Option(
        "", help="Filter by an exact longitude or by an interval of two longitudes"
    ),
    variables: str = typer.Option(
        "",
        help="Filter by a variable or a list of variables. For example:"
        "to retrieve all partitions that contain variables air_pressure and/or"
        " air_temperature: air_pressure,air_temperature",
    ),
    stations: str = typer.Option("", help="Filter by a station or a list of stations"),
    print_format: str = typer.Option("table", help=print_format_msg),
):
    """List entries in the catalogue. Accepts arguments to filter the output."""
    filters = CliCatalogueFilters(
        dataset=dataset,
        dataset_source=source,
        time=list_parser(time),
        latitudes=[float(lat) for lat in list_parser(latitudes)],
        longitudes=[float(lon) for lon in list_parser(longitudes)],
        variables=list_parser(variables),
        stations=list_parser(stations),
    )
    try:
        config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    except ConfigError:
        raise ConfigNotFound()
    with get_session(config.catalogue_db) as session:
        """List catalogue table data."""
        # with pagination (50 per page)
        results = list_catalogue_(session, filters, page)

    if len(results) == 0:
        raise RuntimeError("No catalogue entries found for these parameters.")

    print_db_results(results, print_format)


def list_catalogue_(
    session: sqlalchemy.orm.Session, filters: CliCatalogueFilters, page: int = 1
):
    if filters.empty:
        results = CatalogueRepository(session).get_all(
            skip=(page - 1) * PAGE_SIZE, limit=PAGE_SIZE
        )
    else:
        results = CatalogueRepository(session).get_by_filters(
            filter_args=filters.to_repository_filters(),
            skip=(page - 1) * PAGE_SIZE,
            limit=PAGE_SIZE,
        )
    return results


def catalogue_dataset_info(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = typer.Argument(..., help="dataset name"),
    source: str = typer.Argument(
        "", help="dataset source, if not provided all dataset sources will be displayed"
    ),
):
    """Get catalogue info for certain dataset."""
    try:
        config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    except ConfigError:
        raise ConfigNotFound()
    with get_session(config.catalogue_db) as session:
        print_catalogue_info(session, source, dataset)


def list_datasets(
    cdsobs_config_yml: Path = config_yml_typer,
    page: int = typer.Option(
        0, help="Results are paginated by 50 entries, choose page"
    ),
    print_format: str = typer.Option("table", help=print_format_msg),
):
    """List all datasets and versions."""
    try:
        config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    except ConfigError:
        raise ConfigNotFound()
    with get_session(config.catalogue_db) as session:
        results = CadsDatasetRepository(session).get_all(
            skip=page * PAGE_SIZE, limit=PAGE_SIZE
        )
        print_db_results(results, print_format, CadsDataset)


def print_catalogue_info(session, source, dataset):
    if len(source):
        results = CatalogueRepository(session).get_by_dataset_and_source(
            dataset=dataset, dataset_source=source
        )
    else:
        results = CatalogueRepository(session).get_by_dataset(dataset)

    if len(results) == 0:
        raise RuntimeError(
            f"{dataset=} and source {source=} not found in the catalogue."
        )

    console.print(stats_summary(results))


def stats_summary(entries: sqlalchemy.engine.result.ScalarResult) -> dict:
    num_of_partitions = 0
    total_size = 0
    stations, variables, time_coverages = set(), set(), set()
    longitudes, latitudes = list(), list()
    dataset_sources = set()
    for entry in entries:
        logger.debug(f"Reading entry {entry.id}")
        num_of_partitions += 1
        total_size += entry.file_size
        stations.update(entry.stations)
        variables.update(entry.variables)
        dataset_sources.add(entry.dataset_source)
        time_coverages.update([entry.time_coverage_start, entry.time_coverage_end])
        longitudes.extend(
            [entry.longitude_coverage_start, entry.longitude_coverage_end]
        )
        latitudes.extend([entry.latitude_coverage_start, entry.latitude_coverage_end])
    station_num = len(stations)
    return {
        "number of partitions": num_of_partitions,
        "total size in disk": str(round(total_size * 1024**-2, 2)) + "MB",
        "number of stations": station_num,
        "available variables": list(variables),
        "total time coverage": (
            min(time_coverages).strftime("%Y-%m-%d"),
            max(time_coverages).strftime("%Y-%m-%d"),
        ),
        "total latitude coverage": (min(latitudes), max(latitudes)),
        "total longitude coverage": (min(longitudes), max(longitudes)),
        "available stations": list(stations),
        "available dataset sources": list(dataset_sources),
    }
