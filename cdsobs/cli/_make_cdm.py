import tempfile
from pathlib import Path

import typer

from cdsobs.api import run_make_cdm
from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import read_and_validate_config


def make_cdm(
    dataset_name: str = typer.Option(
        ..., "--dataset", "-d", help="Dataset name", show_default=False
    ),
    start_year: int = typer.Option(
        ..., help="Year to start processing the data", show_default=False
    ),
    end_year: int = typer.Option(
        ..., help="Year to stop processing the data", show_default=False
    ),
    cdsobs_config_yml: Path = config_yml_typer,
    source: str = typer.Option(
        ...,
        help="Source to process. Sources are defined in the service definition file,"
        "in the sources mapping.",
    ),
    output_dir: Path = typer.Option(
        tempfile.gettempdir(),
        "--output-dir",
        "-o",
        help="Directory where to write the output if --save-data is enabled.",
    ),
    save_data: bool = typer.Option(
        False,
        "--save-data",
        "-s",
        help="If set, will save netCDF files in --output-dir",
    ),
):
    """Prepare the data to be uploaded without actually uploading it."""
    config = read_and_validate_config(cdsobs_config_yml)
    run_make_cdm(
        dataset_name,
        source,
        config,
        start_year=start_year,
        end_year=end_year,
        output_dir=output_dir,
        save_data=save_data,
    )
