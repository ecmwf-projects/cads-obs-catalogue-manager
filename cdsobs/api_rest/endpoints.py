import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Iterator

import sqlalchemy.orm
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.config import CDSObsConfig, validate_config
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.repositories.dataset import CadsDatasetRepository
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.retrieve.retrieve_services import get_catalogue_entries, get_urls
from cdsobs.service_definition.api import get_service_definition
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.storage import S3Client
from cdsobs.utils.exceptions import ConfigNotFound, DataNotFoundException, SizeError
from cdsobs.utils.utils import get_database_session

router = APIRouter()


@dataclass
class HttpAPISession:
    cdsobs_config: CDSObsConfig
    catalogue_session: sqlalchemy.orm.Session


def session_gen() -> Iterator[HttpAPISession]:
    cdsobs_config = get_config()
    try:
        catalogue_session = get_database_session(cdsobs_config.catalogue_db.get_url())
        session = HttpAPISession(cdsobs_config, catalogue_session)
        yield session
    finally:
        session.catalogue_session.close()


def get_config() -> CDSObsConfig:
    if "CDSOBS_CONFIG" in os.environ:
        cdsobs_config_yml = Path(os.environ["CDSOBS_CONFIG"])
    else:
        cdsobs_config_yml = Path.home().joinpath(".cdsobs/cdsobs_config.yml")
    if not Path(cdsobs_config_yml).exists():
        raise ConfigNotFound()
    cdsobs_config = validate_config(cdsobs_config_yml)
    return cdsobs_config


def make_http_exception(
    status_code: int, message: str, traceback: str | None = None
) -> HTTPException:
    detail = dict(message="Error: Observations API failed.")
    if traceback is not None:
        detail["traceback"] = traceback
    http_exception = HTTPException(
        status_code=status_code, detail=dict(message=message, traceback=traceback)
    )
    return http_exception


@router.post("/get_object_urls")
def get_object_urls(
    retrieve_args: RetrieveArgs,
    session: Annotated[HttpAPISession, Depends(session_gen)],
) -> list[str]:
    # Query the storage to get the URLS of the files that contain the data requested
    try:
        catalogue_repository = CatalogueRepository(session.catalogue_session)
        entries = get_catalogue_entries(catalogue_repository, retrieve_args)
    except DataNotFoundException as e:
        raise make_http_exception(status_code=500, message=f"Error: {e}")
    except Exception as e:
        raise make_http_exception(
            status_code=500,
            message=f"Error: Observations API failed: {e}",
            traceback=repr(e),
        )
    s3client = S3Client.from_config(session.cdsobs_config.s3config)
    try:
        object_urls = get_urls(entries, s3client.base)
    except SizeError as e:
        raise HTTPException(status_code=500, detail=dict(message=f"Error: {e}"))
    except Exception as e:
        raise make_http_exception(
            status_code=500, message="Error: Observations API failed", traceback=repr(e)
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
def get_sources(
    dataset: str,
    session: Annotated[HttpAPISession, Depends(session_gen)],
) -> list[str]:
    """Get available sources for a given dataset."""
    service_definition = get_service_definition(session.cdsobs_config, dataset)
    return list(service_definition.sources)


@router.get("/{dataset}/service_definition.yml")
def get_dataset_service_definition_yaml(
    dataset: str,
    session: Annotated[HttpAPISession, Depends(session_gen)],
) -> StreamingResponse:
    """Get the service definition for a dataset."""
    try:
        s3_client = S3Client.from_config(session.cdsobs_config.s3config)
        bucket_name = s3_client.get_bucket_name(dataset_name=dataset)
        s3_obj = s3_client.s3.Object(bucket_name, "service_definition.yml")
        file_like = s3_obj.get()["Body"]
        return StreamingResponse(
            content=file_like,
            media_type="application/x-yaml",
            headers={
                "Content-Disposition": "attachment; filename=service_definition.yml"
            },
        )
    except FileNotFoundError:
        raise make_http_exception(
            status_code=404,
            message=f"Form service_definition.yml not found for {dataset=}.",
        )


@router.get("/cdm/lite_variables")
def get_cdm_lite_variables() -> dict[str, list[str] | dict]:
    return cdm_lite_variables


@router.get("/{dataset}/{source}/disabled_fields")
def get_disabled_fields(
    dataset: str, source: str, session: Annotated[HttpAPISession, Depends(session_gen)]
) -> list[str]:
    disabled_fields = session.cdsobs_config.get_disabled_fields(dataset, source)
    return disabled_fields


@router.get("/{dataset}/forms/{json_name}")
def get_form(
    dataset: str,
    json_name: str,
    session: Annotated[HttpAPISession, Depends(session_gen)],
) -> StreamingResponse:
    """Get the service definition for a dataset."""
    try:
        s3_client = S3Client.from_config(session.cdsobs_config.s3config)
        bucket_name = s3_client.get_bucket_name(dataset_name=dataset)
        s3_obj = s3_client.s3.Object(bucket_name, json_name)
        file_like = s3_obj.get()["Body"]
        return StreamingResponse(
            content=file_like,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={json_name}"},
        )
    except FileNotFoundError:
        raise make_http_exception(
            status_code=404, message=f"Form {json_name} not found for {dataset=}."
        )
