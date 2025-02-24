from pathlib import Path

import dask
import sqlalchemy.orm
import typer
from click import prompt
from fastapi.encoders import jsonable_encoder
from rich.console import Console
from sqlalchemy import delete, func, select

from cdsobs.cli._utils import (
    config_yml_typer,
    list_parser,
)
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.catalogue import (
    CatalogueSchema,
    CliCatalogueFilters,
)
from cdsobs.storage import S3Client
from cdsobs.utils.exceptions import CliException, ConfigError, ConfigNotFound

console = Console()


def delete_dataset(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = typer.Option(..., help="dataset to delete", prompt=True),
    dataset_source: str = typer.Option(
        None, help="dataset source to delete. By default it will delete all."
    ),
    time: str = typer.Option(
        "",
        help="Filter by an exact date or by an interval of two dates. For example: "
        "to delete all partitions of year 1970: 1970-1-1,1970-12-31",
    ),
):
    """Permanently delete the given dataset from the catalogue and the storage."""
    confirm = prompt(
        "This will delete the data permanently."
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
        nd = len(deleted_entries)
        console.print(
            f"[bold green] {nd} entries deleted from {dataset}. [/bold green]"
        )
        nremaining = catalogue_session.scalar(
            select(func.count()).select_from(Catalogue)
        )
        if nremaining == 0:
            CadsDatasetRepository(catalogue_session).delete_dataset(dataset)
            console.print(
                f"[bold green] Deleted {dataset} from datasets table as it was left empty. "
                f"[/bold green]"
            )


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
    try:
        catalogue_session.execute(delete(Catalogue).where(*filters))
        catalogue_session.commit()
    except (Exception, KeyboardInterrupt):
        catalogue_rollback(catalogue_session, entries)
    return entries


def delete_from_s3(deleted_entries, s3_client):
    assets = [e.asset for e in deleted_entries]

    def delete_asset(asset_to_delete):
        bucket, name = asset_to_delete.split("/")
        s3_client.delete_file(bucket, name)

    delayed_deletes = []
    for asset in assets:
        delayed_deletes.append(dask.delayed(delete_asset)(asset))

    dask.compute(*delayed_deletes)


def catalogue_rollback(catalogue_session, deleted_entries):
    schemas = [CatalogueSchema(**jsonable_encoder(e)) for e in deleted_entries]
    CatalogueRepository(catalogue_session).create_many(schemas)
