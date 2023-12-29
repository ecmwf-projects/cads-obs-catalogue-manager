from pathlib import Path

import typer
from rich.console import Console

from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.storage import S3Client, StorageClient

console = Console()


def check_consistency(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = typer.Argument(
        ..., help="dataset name. If provided will only check entries for that dataset."
    ),
):
    """
    Check if catalogue db and object storage are consistent.

    That means that every asset has a catalogue entry and vice versa.
    """
    config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    s3client = S3Client.from_config(config.s3config)
    with get_session(config.catalogue_db) as session:
        catalogue_repo = CatalogueRepository(session)
        check_if_missing_in_object_storage(catalogue_repo, s3client, dataset)
        check_if_missing_in_catalogue(catalogue_repo, s3client, dataset)


def check_if_missing_in_object_storage(
    catalogue_repo: CatalogueRepository,
    s3client: StorageClient,
    dataset: str | None = None,
):
    red_flag = False
    if dataset is None:
        console.print(
            "Check if every dataset in the catalogue is in the object storage"
        )
        page = 0
        page_size = 10000
        assets = catalogue_repo.get_all_assets(limit=page_size)
        while len(assets):
            console.print(f"Checking page {page+1} ({page_size} records per page)")
            red_flag = assets_in_s3(assets, s3client)
            page += 1
            assets = catalogue_repo.get_all_assets(
                skip=page * page_size, limit=page_size
            )
    else:
        assets = catalogue_repo.get_dataset_assets(dataset)
        red_flag = assets_in_s3(assets, s3client)
    if not red_flag:
        console.print("[bold green] Found all assets in object storage [/bold green]")


def assets_in_s3(assets, s3client) -> bool:
    red_flag = False
    for asset in assets:
        bucket, name = asset.split("/")
        if not s3client.object_exists(bucket, name):
            console.print(
                f"[bold red] Missing {str(asset)} in object storage [/bold red]"
            )
            red_flag = True
    return red_flag


def check_if_missing_in_catalogue(
    catalogue_repo: CatalogueRepository,
    s3client: StorageClient,
    dataset: str | None = None,
):
    red_flag = False
    if dataset is None:
        console.print(
            "Check if every dataset in the object storage has a catalogue entry"
        )
        buckets = s3client.list_buckets()
        for bucket in buckets:
            object_names = s3client.list_directory_objects(bucket)
            red_flag = objects_in_catalogue(
                object_names, bucket, s3client, catalogue_repo
            )
    else:
        objects = s3client.list_directory_objects(dataset)
        object_names = [o.object_name for o in objects]
        red_flag = objects_in_catalogue(object_names, dataset, s3client, catalogue_repo)
    if not red_flag:
        console.print("[bold green] Found all assets in catalogue [/bold green]")


def objects_in_catalogue(objects, bucket, s3client, catalogue_repo) -> bool:
    red_flag = False
    for obj in objects:
        asset = s3client.get_asset(bucket, obj)
        if not catalogue_repo.exists_asset(asset):
            console.print(f"[bold red] Missing {asset} entry in catalogue [/bold red]")
            red_flag = True
    return red_flag
