import datetime
import json
from pathlib import Path

import typer
from rich.console import Console

from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import validate_config
from cdsobs.retrieve.api import retrieve_observations
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.storage import S3Client
from cdsobs.utils.exceptions import CliException, ConfigNotFound


def retrieve(
    cdsobs_config_yml: Path = config_yml_typer,
    retrieve_params_json: Path = typer.Option(
        ...,
        "--retrieve-params",
        "-p",
        help="Path to a JSON file with the retrieve params.",
        show_default=False,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        "-o",
        help="Directory where to write the output file.",
        show_default=False,
    ),
    size_limit: int = typer.Option(
        100000000000000000,
        "--size-limit",
        "-sl",
        help="Specify a size limit for the data size retrieved in bytes.",
        show_default=False,
    ),
):
    """Retrieve datasets from the CADS observation repository."""
    if not retrieve_params_json.exists():
        raise CliException(f"{retrieve_params_json=} file not found")
    if not output_dir.exists():
        raise CliException("output_dir does not exist.")
    with open(retrieve_params_json, "r") as rf:
        retrieve_json = json.load(rf)
    if len(retrieve_json) < 2:
        raise CliException("Invalid retrieve_params format")
    dataset = retrieve_json[0]
    params = retrieve_json[1]
    time_coverage_str = params["time_coverage"]
    if len(time_coverage_str) < 2:
        raise CliException("Invalid retrieve_params format")
    params["time_coverage"][0] = datetime.datetime.fromisoformat(time_coverage_str[0])
    params["time_coverage"][1] = datetime.datetime.fromisoformat(time_coverage_str[1])
    retrieve_args = RetrieveArgs(dataset=dataset, params=params)

    if cdsobs_config_yml is None:
        cdsobs_config_yml = Path.home().joinpath(".cdsobs/cdsobs_config.yml")
    if not cdsobs_config_yml.exists():
        raise ConfigNotFound()
    config = validate_config(cdsobs_config_yml)
    s3_client = S3Client.from_config(config.s3config)
    output_file = retrieve_observations(
        config.catalogue_db.get_url(),
        s3_client.public_url_base,
        retrieve_args,
        output_dir,
        size_limit,
    )
    console = Console()
    console.print(f"[green] Successfully downloaded {output_file} [/green]")
