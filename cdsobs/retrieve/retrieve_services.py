from typing import Sequence

import numpy
import pandas
import pandas as pd
import sqlalchemy as sa

from cdsobs.observation_catalogue.models import CadsDatasetVersion, Catalogue
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema
from cdsobs.retrieve.filter_datasets import between
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.utils.exceptions import DataNotFoundException, SizeError
from cdsobs.utils.logutils import get_logger, sizeof_fmt

logger = get_logger(__name__)


def estimate_data_size(
    entries: Sequence[Catalogue], retrieve_args: RetrieveArgs
) -> float:
    """
    Estimate the size of a set of catalogue entries/records.

    It does not download them but it uses the constraints and the data size as stored in
    the catalogue. This is not 100% accurate.
    """
    estimated_size = 0.0
    for entry in entries:
        constraints = ConstraintsSchema(**entry.constraints)
        constraints_table = constraints.to_table(entry.stations)
        retrieve_constraints = filter_retrieve_constraints(
            constraints_table, retrieve_args
        )
        partition_obs = constraints.get_num_obs()
        retrieve_obs = retrieve_constraints.get_num_obs()
        # We estimate the size of the output file, not the size of the data volume in
        # the memory uncompressed.
        estimated_size += entry.file_size * (retrieve_obs / partition_obs)
    return estimated_size


def merged_constraints_table(entries: sa.engine.result.ScalarResult) -> pd.DataFrame:
    """Merge a set of  constraints tables."""

    def _get_entry_constraints(e):
        logger.debug(f"Reading constraints for entry {e.id}")
        entry_constraints = (
            ConstraintsSchema(**e.constraints)
            .to_table(e.stations)
            .set_index(["stations", "time"])
            .assign(source=e.dataset_source, version=e.version)
        )
        return entry_constraints

    table_constraints = [_get_entry_constraints(e) for e in entries]
    if len(table_constraints) > 1:
        df_total = pandas.concat(table_constraints, axis=0)
    else:
        df_total = table_constraints[0]
    # Some combinations can be repeated, they must be combined with any()
    # as there may be datasets with more parameters in the constraints.
    df_total = (
        df_total.reset_index().groupby(["stations", "time", "source", "version"]).any()
    )
    # replace NaNs (new cells created by combining data, no value in original data)
    # with False:
    df_total = df_total.fillna(False)
    return df_total.reset_index()


def filter_retrieve_constraints(
    constraints_table: pd.DataFrame, retrieve_args: RetrieveArgs
) -> ConstraintsSchema:
    """Filter constraints according to the retrieve arguments."""
    # Get from total partition constraints the obs that will be retrieved
    times = pd.to_datetime(constraints_table["time"]).dt.tz_localize(None)
    if retrieve_args.params.time_coverage is not None:
        time_mask = between(times, *retrieve_args.params.time_coverage).values  # type: ignore
    else:
        time_mask = numpy.zeros(shape=len(times))
        if retrieve_args.params.year is not None:
            year_mask = times.dt.year.isin(retrieve_args.params.year)
            time_mask = time_mask * year_mask
        if retrieve_args.params.month is not None:
            month_mask = times.dt.month.isin(retrieve_args.params.month)
            time_mask = time_mask * month_mask
        if retrieve_args.params.day is not None:
            day_mask = times.dt.day.isin(retrieve_args.params.day)
            time_mask = time_mask * day_mask

    constraints_mask = time_mask
    if retrieve_args.params.variables is not None:
        # Some partitions do not contain all the variables requested, so here we need to
        # compute the intersection.
        variables_in_partition = constraints_table.columns.intersection(
            retrieve_args.params.variables
        ).tolist()
        # Variables here are columns, so we filter them here, not in the mask
        retrieve_table = constraints_table[
            ["stations", "time"] + variables_in_partition  # type: ignore
        ]
    else:
        retrieve_table = constraints_table
    # Filter the stations
    if retrieve_args.params.stations is not None:
        station_mask = (
            constraints_table["stations"].isin(retrieve_args.params.stations).values
        )
        constraints_mask = constraints_mask * station_mask
    retrieve_table = retrieve_table.loc[constraints_mask]
    return ConstraintsSchema.from_table(retrieve_table)


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


def _check_data_size(
    entries: Sequence[Catalogue], retrieve_args: RetrieveArgs, size_limit: int
):
    """Estimate the size of the download and raise an error if it is too big."""
    estimated_data_size = estimate_data_size(entries, retrieve_args)
    logger.info(f"Estimated size of the data to retrieve is {estimated_data_size}B")
    if estimated_data_size > size_limit:
        raise SizeError(
            f"Requested data size is over the size limit ({sizeof_fmt(size_limit)})."
        )


def _get_catalogue_entries(
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
