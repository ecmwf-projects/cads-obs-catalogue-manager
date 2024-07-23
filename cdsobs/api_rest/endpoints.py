import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Iterator

import sqlalchemy.orm
from fastapi import APIRouter, Depends, HTTPException

from cdsobs.api_rest.models import RetrievePayload
from cdsobs.cdm.api import get_auxiliary_variables_mapping
from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.cli._utils import ConfigNotFound
from cdsobs.config import CDSObsConfig, validate_config
from cdsobs.ingestion.core import get_variables_from_service_definition
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.retrieve_services import (
    DataNotFoundException,
    _get_catalogue_entries,
    get_urls_and_check_size,
)
from cdsobs.service_definition.api import get_service_definition
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import SizeError
from cdsobs.utils.utils import get_database_session

router = APIRouter()


@dataclass
class HttpAPISession:
    cdsobs_config: CDSObsConfig
    catalogue_session: sqlalchemy.orm.Session


def session_gen() -> Iterator[HttpAPISession]:
    if "CDSOBS_CONFIG" in os.environ:
        cdsobs_config_yml = Path(os.environ["CDSOBS_CONFIG"])
    else:
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
    try:
        catalogue_repository = CatalogueRepository(session.catalogue_session)
        entries = _get_catalogue_entries(catalogue_repository, retrieve_args)
    except DataNotFoundException as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error: Observations API failed: {e}"
        )
    s3client = S3Client.from_config(session.cdsobs_config.s3config)
    try:
        object_urls = get_urls_and_check_size(
            entries, retrieve_args, retrieve_payload.config.size_limit, s3client.base
        )
    except SizeError as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error: Observations API failed: {e}"
        )
    return object_urls


@router.get("/capabilities/datasets")
def get_capabilities(
    session: Annotated[HttpAPISession, Depends(session_gen)]
) -> list[str]:
    """Get available datasets."""
    results = CadsDatasetRepository(session.catalogue_session).get_all()
    return [r.name for r in results]


@router.get("/capabilities/{dataset}/sources")
def get_sources(dataset: str) -> list[str]:
    """Get available sources for a given dataset."""
    service_definition = get_service_definition(dataset)
    return list(service_definition.sources)


@router.get("/{dataset}/service_definition")
def get_dataset_service_definition(dataset: str) -> ServiceDefinition:
    """Get the service definition for a dataset."""
    try:
        return get_service_definition(dataset)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail=f"Service definition not found for {dataset=}"
        )


@router.get("/{dataset}/{source}/aux_variables_mapping")
def get_dataset_auxiliary_variables_mapping(
    dataset: str, source: str
) -> dict[str, list[dict[str, str]]]:
    """Get the service definition for a dataset."""
    service_definition = get_service_definition(dataset)
    source_definition = service_definition.sources[source]
    variables = get_variables_from_service_definition(service_definition, source)
    auxiliary_variables_mapping = get_auxiliary_variables_mapping(
        source_definition, variables
    )
    return auxiliary_variables_mapping


@router.get("/cdm/lite_variables")
def get_cdm_lite_variables() -> dict[str, list[str]]:
    return cdm_lite_variables
