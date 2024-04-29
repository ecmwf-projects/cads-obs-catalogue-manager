"""Retrieve pipeline."""
from pathlib import Path
from typing import Sequence

import h5netcdf
from cads_adaptors.adaptors.cadsobs.retrieve import retrieve_data

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.retrieve.retrieve_services import estimate_data_size
from cdsobs.service_definition.api import get_service_definition
from cdsobs.utils.logutils import SizeError, get_logger
from cdsobs.utils.utils import get_database_session

logger = get_logger(__name__)
MAX_NUMBER_OF_GROUPS = 10


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
    output_path = retrieve_data(
        retrieve_args.dataset,
        retrieve_args.params.model_dump(),
        output_dir,
        object_urls,
        cdm_lite_variables,
        global_attributes,
    )
    return output_path


def get_urls_and_check_size(
    entries: Sequence[Catalogue],
    retrieve_args: RetrieveArgs,
    size_limit: int,
    storage_url: str,
) -> list[str]:
    """
    Get the catalogue entries to get the urls of the assets. Check the size too.

    We group this is this function to ensure that entries, that can be a bit big,
    are garbage collected.
    """
    object_urls = [f"{storage_url}/{e.asset}" for e in entries]
    _check_data_size(entries, retrieve_args, size_limit)
    return object_urls


def get_vars_in_cdm_lite(incobj: h5netcdf.File) -> list[str]:
    """Return the variables in incobj that are defined in the CDM-lite."""
    vars_in_cdm_lite = [v for v in incobj.variables if v in cdm_lite_variables]
    # This searches for variables with "|cdm_table  in their name."
    vars_with_bar_in_cdm_lite = [
        v
        for v in incobj.variables
        if "|" in v and v.split("|")[0] in cdm_lite_variables
    ]
    vars_in_cdm_lite += vars_with_bar_in_cdm_lite
    return vars_in_cdm_lite


def _check_data_size(
    entries: Sequence[Catalogue], retrieve_args: RetrieveArgs, size_limit: int
):
    """Estimate the size of the download and raise an error if it is too big."""
    estimated_data_size = estimate_data_size(entries, retrieve_args)
    logger.info(f"Estimated size of the data to retrieve is {estimated_data_size}B")
    if estimated_data_size > size_limit:
        raise SizeError("Requested data size is over the size limit.")


def _get_catalogue_entries(
    catalogue_repository: CatalogueRepository, retrieve_args: RetrieveArgs
) -> Sequence[Catalogue]:
    """Return the entries of the catalogue that contain the requested data."""
    entries = catalogue_repository.get_by_filters(
        retrieve_args.params.get_filter_arguments(dataset=retrieve_args.dataset),
        sort=True,
    )
    if len(entries) == 0:
        raise RuntimeError(
            "No entries found in catalogue for this parameter combination."
        )
    logger.info("Retrieved list of required partitions from the catalogue.")
    return entries
