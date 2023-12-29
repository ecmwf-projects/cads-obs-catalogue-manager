import tempfile
from pathlib import Path

import typer

from cdsobs.cli._utils import config_yml_typer, read_and_validate_config
from cdsobs.forms_jsons import get_forms_jsons
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.storage import S3Client


def get_forms_jsons_command(
    dataset_name: str = typer.Option(
        ..., "--dataset", "-d", help="Dataset name", show_default=False
    ),
    cdsobs_config_yml: Path = config_yml_typer,
    output_dir: Path = typer.Option(
        tempfile.gettempdir(),
        "--output-dir",
        "-o",
        help="Directory where to write the output if --save-data is enabled.",
    ),
    upload: bool = typer.Option(
        False,
        "--upload",
        "-u",
    ),
    stations_file: bool = typer.Option(
        False,
        "--stations_file",
        "-s",
    ),
):
    """Save the geco output json files in a folder, optionally upload it."""
    config = read_and_validate_config(cdsobs_config_yml)
    storage_client = S3Client.from_config(config.s3config)
    with get_session(config.catalogue_db) as session:
        catalogue_repository = CatalogueRepository(session)
        get_forms_jsons(
            dataset_name,
            catalogue_repository,
            output_dir,
            upload_to_storage=upload,
            storage_client=storage_client,
            get_stations_file=stations_file,
        )
