from pathlib import Path

import typer

from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)


def deprecate_dataset_version(
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

    with get_session(config.catalogue_db) as session:
        cads_dataset_version_repo = CadsDatasetVersionRepository(session)
        dataset_version = cads_dataset_version_repo.get_dataset(
            dataset_name=dataset, version=version
        )
        if dataset_version is None:
            raise RuntimeError(f"{dataset=} {version=} not found in the catalogue")
        dataset_version.deprecated = True
        session.commit()
        print(f"Deprecated {dataset=} {version=}")
