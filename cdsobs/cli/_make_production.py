from pathlib import Path

import typer

from cdsobs.api import run_ingestion_pipeline
from cdsobs.cli._utils import config_yml_typer
from cdsobs.config import read_and_validate_config
from cdsobs.observation_catalogue.database import get_session
from cdsobs.service_definition.api import validate_service_definition


def make_production(
    dataset_name: str = typer.Option(
        ..., "--dataset", "-d", help="Dataset name", show_default=False
    ),
    service_definition_json: Path = typer.Option(
        ...,
        "--service-definition",
        "-s",
        help="Path to the service_definition.json",
        show_default=False,
    ),
    start_year: int = typer.Option(
        ..., help="Year to start processing the data", show_default=False
    ),
    end_year: int = typer.Option(
        ..., help="Year to stop processing the data", show_default=False
    ),
    cdsobs_config_yml: Path = config_yml_typer,
    source: str = typer.Option(
        "all", help="Process only a given source, by default it processes all"
    ),
    update: bool = typer.Option(
        False,
        "--update",
        "-u",
        help=(
            "If set, data overlapping in time (year and month) with existing partitions "
            "will be read in order to check if it changed these need to be updated. "
            "By default, these time intervals will be skipped."
        ),
    ),
    start_month: int = typer.Option(
        1,
        "--start-month",
        help="Month to start reading the data. It only applies to the first year of "
        "the interval. Default is 1.",
    ),
):
    """
    Upload datasets to the CADS observation repository.

    Read input data for a CADS observations dataset, homogenises it, partitions it and
    uploads it to the observation catalogue and storage.
    """
    config = read_and_validate_config(cdsobs_config_yml)

    # read and validate service definition
    service_definition = validate_service_definition(
        str(service_definition_json), config.cdm_tables_location
    )[0]
    assert service_definition is not None

    # Check if we selected only one source
    sources = [source] if source != "all" else service_definition.sources.keys()

    # ingestion pipeline per source
    with get_session(config.catalogue_db) as session:
        for source in sources:
            run_ingestion_pipeline(
                dataset_name,
                service_definition,
                source,
                session,
                config,
                start_year,
                end_year,
                update,
                start_month,
            )
