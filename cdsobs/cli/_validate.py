from pathlib import Path

import typer

import cdsobs.service_definition.api
from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import read_and_validate_config
from cdsobs.utils.exceptions import CliException


def validate_service_definition(
    service_definition: str = typer.Argument(
        ..., help="Path to Service definition YAML file"
    ),
    cdsobs_config_yml: Path = config_yml_typer,
):
    """Validate a service definition YAML file."""
    if not Path(service_definition).exists():
        raise CliException("File not found")
    config = read_and_validate_config(cdsobs_config_yml)
    cdm_tables_location = config.cdm_tables_location
    cdsobs.service_definition.api.validate_service_definition(
        service_definition,
        cdm_tables_location,
    )
