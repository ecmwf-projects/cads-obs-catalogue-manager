from typing import Sequence

import pandas
import pandas as pd
import sqlalchemy as sa

from cdsobs.observation_catalogue.models import CadsDatasetVersion, Catalogue
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.utils.exceptions import DataNotFoundException
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def merged_constraints_table(entries: sa.engine.result.ScalarResult) -> pd.DataFrame:
    """Merge a set of  constraints tables."""

    def _get_entry_constraints(e):
        logger.debug(f"Reading constraints for entry {e.id}")
        entry_constraints = (
            (
                ConstraintsSchema(**e.constraints)
                .to_table(e.stations)
                .drop("stations", axis=1)
                .set_index(["time"])
                .assign(source=e.dataset_source, version=e.version)
            )
            .reset_index()
            .groupby(["time", "source", "version"])
            .any()
            .reset_index()
        )
        return entry_constraints

    # Loop over entries to get
    table_constraints = [_get_entry_constraints(e) for e in entries]
    logger.info("Concatenating the tables")
    if len(table_constraints) > 1:
        df_total = pandas.concat(
            table_constraints, axis=0, ignore_index=True
        ).set_index(["time", "source", "version"])
    else:
        df_total = table_constraints[0]
    # replace NaNs (new cells created by combining data, no value in original data)
    # with False:
    df_total = df_total.fillna(False)
    # Some combinations can be repeated, they must be combined with any()
    # as there may be datasets with more parameters in the constraints.
    df_total = df_total.reset_index().groupby(["time", "source", "version"]).any()
    return df_total


def get_urls(
    entries: Sequence[Catalogue],
    storage_url: str,
) -> list[str]:
    """
    Get the catalogue entries to get the urls of the assets. Check the size too.

    We group this is this function to ensure that entries, that can be a bit big,
    are garbage collected.
    """
    object_urls = [f"{storage_url}/{e.asset}" for e in entries]
    return object_urls


def get_catalogue_entries(
    catalogue_repository: CatalogueRepository, retrieve_args: RetrieveArgs
) -> Sequence[Catalogue]:
    """Return the entries of the catalogue that contain the requested data."""
    if retrieve_args.params.version == "last":
        last_version = catalogue_repository.session.scalar(
            sa.select(sa.func.max(CadsDatasetVersion.version)).filter(
                CadsDatasetVersion.dataset == retrieve_args.dataset,
                CadsDatasetVersion.deprecated == False,  # noqa
            )
        )
        if last_version is None:
            raise RuntimeError("Failure determining the last version.")
        retrieve_args.params.version = last_version
    entries = catalogue_repository.get_by_filters(
        retrieve_args.params.get_filter_arguments(dataset=retrieve_args.dataset),
        sort=True,
    )

    if len(entries) == 0:
        raise DataNotFoundException(
            "No entries found in catalogue for this parameter combination."
        )
    logger.info("Retrieved list of required partitions from the catalogue.")
    return entries
