from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import sqlalchemy.orm
from fastapi import APIRouter, Depends, HTTPException

from cdsobs.api_rest import config_helper
from cdsobs.api_rest.models import RetrievePayload
from cdsobs.cli._utils import ConfigNotFound
from cdsobs.config import CDSObsConfig, validate_config
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.api import (
    _get_catalogue_entries,
    get_urls_and_check_size,
)
from cdsobs.service_definition.api import get_service_definition
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.storage import S3Client
from cdsobs.utils.utils import get_database_session

router = APIRouter()


@dataclass
class HttpAPISession:
    cdsobs_config: CDSObsConfig
    catalogue_session: sqlalchemy.orm.Session


def session_gen() -> HttpAPISession:
    cdsobs_config_yml = Path.home().joinpath(".cdsobs/cdsobs_config.yml")
    if not Path(cdsobs_config_yml).exists():
        raise ConfigNotFound()
    cdsobs_config = validate_config(cdsobs_config_yml)
    try:
        catalogue_session = get_database_session(cdsobs_config.catalogue_db.get_url())
        session = HttpAPISession(cdsobs_config, catalogue_session)
        yield session
    finally:
        session.catalogue_session.close()


@router.post("/get_object_urls_and_check_size")
def get_object_urls_and_check_size(
    retrieve_payload: RetrievePayload,
    session: Annotated[HttpAPISession, Depends(session_gen)],
) -> list[str]:
    # Query the storage to get the URLS of the files that contain the data requested
    retrieve_args = retrieve_payload.retrieve_args
    catalogue_repository = CatalogueRepository(session.catalogue_session)
    entries = _get_catalogue_entries(catalogue_repository, retrieve_args)
    s3client = S3Client.from_config(session.cdsobs_config.s3config)
    object_urls = get_urls_and_check_size(
        entries, retrieve_args, retrieve_payload.config.size_limit, s3client.base
    )
    return object_urls


@router.get("/capabilities/datasets")
def get_capabilities() -> list[str]:
    """Get available datasets."""
    return config_helper.datasets_installed()


@router.get("/capabilities/{dataset}/sources")
def get_sources(dataset: str):
    """Get available sources for a given dataset."""
    return config_helper.get_dataset_sources(dataset)


@router.get("/{dataset}/service_definition")
def get_dataset_service_definition(dataset: str) -> ServiceDefinition:
    """Get the service definition for a dataset."""
    try:
        return get_service_definition(dataset)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail=f"Service definition not found for {dataset=}"
        )
