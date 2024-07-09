from pathlib import Path

import typer

import cdsobs.service_definition.api
from cdsobs.cli._utils import CliException


def validate_service_definition(
    service_definition: str = typer.Argument(..., help="Path to JSON file")
):
    """Validate a service definition YAML file."""
    if not Path(service_definition).exists():
        raise CliException("File not found")
    cdsobs.service_definition.api.validate_service_definition(service_definition)
