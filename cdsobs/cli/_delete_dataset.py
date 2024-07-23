from pathlib import Path

import sqlalchemy.orm
import typer
from click import prompt
from fastapi.encoders import jsonable_encoder
from rich.console import Console

from cdsobs.cli._utils import (
    CliException,
    ConfigNotFound,
    config_yml_typer,
    list_parser,
)
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.catalogue import (
    CatalogueSchema,
    CliCatalogueFilters,
)
from cdsobs.storage import S3Client
from cdsobs.utils.exceptions import ConfigError

console = Console()


def delete_dataset(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = typer.Option(..., help="dataset to delete", prompt=True),
    dataset_source: str = typer.Option("", help="dataset source to delete"),
    time: str = typer.Option(
        "",
        help="Filter by an exact date or by an interval of two dates. For example: "
        "to delete all partitions of year 1970: 1970-1-1,1970-12-31",
    ),
):
    """Permanently delete the given dataset from the catalogue and the storage."""
    confirm = prompt(
        "This will delete the dataset permanently."
        " This action cannot be undone. "
        "Please type again the name of the dataset to confirm"
    )
    assert confirm == dataset, CliException("Error: The entered value do not match.")
    try:
        init_config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    except ConfigError:
        raise ConfigNotFound

    with get_session(init_config.catalogue_db) as catalogue_session:
        deleted_entries = delete_from_catalogue(
            catalogue_session, dataset, dataset_source, time
        )
        s3_client = S3Client.from_config(init_config.s3config)
        try:
            delete_from_s3(deleted_entries, s3_client)
        except (Exception, KeyboardInterrupt):
            catalogue_rollback(catalogue_session, deleted_entries)
            raise
    if len(deleted_entries):
        console.print(f"[bold green] Dataset {dataset} deleted. [/bold green]")


def delete_from_catalogue(
    catalogue_session: sqlalchemy.orm.Session,
    dataset: str,
    dataset_source: str,
    time: str,
):
    catalogue_repo = CatalogueRepository(catalogue_session)
    filters = CliCatalogueFilters(
        dataset=dataset,
        dataset_source=dataset_source,
        time=list_parser(time),
        longitudes=[],
        latitudes=[],
        variables=[],
        stations=[],
    ).to_repository_filters()
    entries = catalogue_repo.get_by_filters(filters)
    if not len(entries):
        console.print(f"[red] No entries for dataset {dataset} found")
    deleted_entries = []
    try:
        for entry in entries:
            catalogue_repo.remove(record_id=entry.id)
            deleted_entries.append(entry)
    except (Exception, KeyboardInterrupt):
        catalogue_rollback(catalogue_session, deleted_entries)
    return deleted_entries


def delete_from_s3(deleted_entries, s3_client):
    assets = [e.asset for e in deleted_entries]
    for asset in assets:
        bucket, name = asset.split("/")
        s3_client.delete_file(bucket, name)


def catalogue_rollback(catalogue_session, deleted_entries):
    schemas = [CatalogueSchema(**jsonable_encoder(e)) for e in deleted_entries]
    CatalogueRepository(catalogue_session).create_many(schemas)
