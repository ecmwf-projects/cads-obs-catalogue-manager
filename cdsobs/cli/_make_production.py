from pathlib import Path

import typer
from pydantic_extra_types.semantic_version import SemanticVersion

from cdsobs.api import run_ingestion_pipeline
from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import read_and_validate_config
from cdsobs.observation_catalogue.database import get_session


def make_production(
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
    start_month: int = typer.Option(
        1,
        "--start-month",
        help="Month to start reading the data. It only applies to the first year of "
        "the interval. Default is 1.",
    ),
    version: str = typer.Option(
        "1.0.0",
        "--version",
        help="Semantic version corresponding to the data to be uploaded. Default is 1.0.0",
        show_default=False,
    ),
    disable_cdm_tag_check: bool = typer.Option(
        False,
        help="Disable CDM tag check, only for testing purposes.",
        show_default=True,
    ),
    slack_notify: bool = typer.Option(
        False,
        help="Notify to slack channel defined by CADSOBS_SLACK_CHANNEL and "
        "CADSOBS_SLACK_HOOK environment variables.",
        show_default=True,
    ),
):
    """
    Upload datasets to the CADS observation repository.

    Read input data for a CADS observations dataset, homogenises it, partitions it and
    uploads it to the observation catalogue and storage.
    """
    config = read_and_validate_config(cdsobs_config_yml)
    # Validate the version
    if not SemanticVersion.is_valid(version):
        raise RuntimeError(f"{version} is not a valid semantic version.")
    # Run the ingestion pipeline
    with get_session(config.catalogue_db) as session:
        run_ingestion_pipeline(
            dataset_name,
            source,
            session,
            config,
            start_year,
            end_year,
            start_month,
            version,
            disable_cdm_tag_check,
            slack_notify,
        )
