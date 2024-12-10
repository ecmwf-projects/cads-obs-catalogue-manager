"""Retrieve pipeline."""
import itertools
from pathlib import Path

from cads_adaptors.adaptors.cadsobs.retrieve import retrieve_data

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.retrieve.retrieve_services import (
    _get_catalogue_entries,
    get_urls_and_check_size,
)
from cdsobs.service_definition.api import get_service_definition
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.utils import get_database_session

logger = get_logger(__name__)


def retrieve_observations(
    catalogue_url: str,
    storage_url: str,
    retrieve_args: RetrieveArgs,
    output_dir: Path,
    size_limit: int,
) -> Path:
    """
    Retrieve data from the obs repository and save it to a netCDF file.

    IMPORTANT: This depends on the functions that now live
    in the cads_adaptors.adaptors.cadsobs.retrieve

    Parameters
    ----------
    catalogue_url:
      URL of the catalogue database including credentials, in the form of
      "postgresql+psycopg2://someuser:somepass@hostname:port/catalogue"
    storage_url:
      Storage URL
    retrieve_args :
      Arguments defining how to filter the database.
    output_dir :
      Path to directory where to save the output netCDF file.
    size_limit :
      Maximum size allowed for the download
    """
    from cads_adaptors.adaptors import Context

    logger.info("Starting retrieve pipeline.")
    # Query the storage to get the URLS of the files that contain the data requested
    with get_database_session(catalogue_url) as session:
        catalogue_repository = CatalogueRepository(session)
        entries = _get_catalogue_entries(catalogue_repository, retrieve_args)
        object_urls = get_urls_and_check_size(
            entries, retrieve_args, size_limit, storage_url
        )
        global_attributes = get_service_definition(
            retrieve_args.dataset
        ).global_attributes
    field_attributes = cdm_lite_variables["attributes"]
    cdm_lite_vars = list(
        set(itertools.chain.from_iterable(cdm_lite_variables.values()))
    )
    context = Context()
    output_path = retrieve_data(
        retrieve_args.dataset,
        retrieve_args.params.model_dump(),
        output_dir,
        object_urls,
        cdm_lite_vars,
        field_attributes,
        global_attributes,
        context,
    )
    return output_path
