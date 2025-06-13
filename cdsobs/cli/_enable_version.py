from pathlib import Path

import typer

from cdsobs.api import set_version_status
from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import CDSObsConfig


def enable_dataset_version(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = typer.Option(..., help="Dataset to deprecate the version"),
    version: str = typer.Option(..., help="Version to deprecate."),
):
    """
    Deprecate a version for a given dataset.

    This version will not be available for retrieve or returned when inspecting the
    catalogue. Constraints need to be updated after runnin this command.
    """
    config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    set_version_status(config, dataset, version, False)
