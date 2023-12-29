from pathlib import Path

import typer

from cdsobs.cli._utils import CliException
from cdsobs.service_definition.api import validate_service_definition


def validate_service_definition_json(
    service_definition_json: str = typer.Argument(..., help="Path to JSON file")
):
    """Validate a service definition JSON file."""
    if not Path(service_definition_json).exists():
        raise CliException("File not found")
    validate_service_definition(service_definition_json)
