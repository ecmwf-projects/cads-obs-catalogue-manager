"""Retrieve pipeline."""
import itertools
from pathlib import Path

from cads_adaptors.adaptors.cadsobs.retrieve import retrieve_data

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.retrieve.retrieve_services import (
    get_catalogue_entries,
    get_urls,
)
from cdsobs.service_definition.api import get_service_definition
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.utils import get_database_session

logger = get_logger(__name__)


def retrieve_observations(
    config: CDSObsConfig,
    storage_url: str,
    retrieve_args: RetrieveArgs,
    output_dir: Path,
) -> Path:
    """
    Retrieve data from the obs repository and save it to a netCDF file.

    IMPORTANT: This depends on the functions that now live
    in the cads_adaptors.adaptors.cadsobs.retrieve

    Parameters
    ----------
    config:
      Configuration of the obs repo
    storage_url:
      Storage URL
    retrieve_args :
      Arguments defining how to filter the database.
    output_dir :
      Path to directory where to save the output netCDF file.
    """
    from cads_adaptors.adaptors import Context

    logger.info("Starting retrieve pipeline.")
    # Query the storage to get the URLS of the files that contain the data requested
    with get_database_session(config.catalogue_db.get_url()) as session:
        catalogue_repository = CatalogueRepository(session)
        entries = get_catalogue_entries(catalogue_repository, retrieve_args)
        object_urls = get_urls(entries, storage_url)
        service_definition = get_service_definition(config, retrieve_args.dataset)
        global_attributes = service_definition.global_attributes
    field_attributes = cdm_lite_variables["attributes"]
    cdm_lite_vars = list(
        set(itertools.chain.from_iterable(cdm_lite_variables.values()))
    )
    disabled_fields_config = service_definition.disabled_fields
    source = retrieve_args.params.dataset_source
    if isinstance(disabled_fields_config, dict):
        disabled_fields = disabled_fields_config.get(source, [])
    else:
        disabled_fields = disabled_fields_config
    cdm_lite_vars = [v for v in cdm_lite_vars if v not in disabled_fields]
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
