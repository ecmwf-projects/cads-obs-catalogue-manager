from pathlib import Path

import typer

import cdsobs.service_definition.api
from cdsobs.cli._utils import CliException


def validate_service_definition(
    service_definition_json: str = typer.Argument(..., help="Path to JSON file")
):
    """Validate a service definition JSON file."""
    if not Path(service_definition_json).exists():
        raise CliException("File not found")
    cdsobs.service_definition.api.validate_service_definition(service_definition_json)
