from typing import Sequence

import numpy
import pandas
import pandas as pd
import sqlalchemy as sa

from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema
from cdsobs.retrieve.filter_datasets import between
from cdsobs.retrieve.models import RetrieveArgs
from cdsobs.utils.logutils import get_logger

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
        return (
            ConstraintsSchema(**e.constraints)
            .to_table(e.stations)
            .set_index(["stations", "time"])
            .assign(source=e.dataset_source)
        )

    table_constraints = [_get_entry_constraints(e) for e in entries]
    if len(table_constraints) > 1:
        df_total = pandas.concat(table_constraints, axis=0)
    else:
        df_total = table_constraints[0]
    # Some combinations can be repeated, they must be combined with any()
    # as there may be datasets with more parameters in the constraints.
    df_total = df_total.reset_index().groupby(["stations", "time", "source"]).any()
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


def ezclump(mask) -> list[slice]:
    """
    Find the clumps (groups of data with the same values) for a 1D bool array.

    Internal function form numpy.ma.extras

    Returns a series of slices.
    """
    if mask.ndim > 1:
        mask = mask.ravel()
    idx = (mask[1:] ^ mask[:-1]).nonzero()
    idx = idx[0] + 1

    if mask[0]:
        if len(idx) == 0:
            return [slice(0, mask.size)]

        r = [slice(0, idx[0])]
        r.extend((slice(left, right) for left, right in zip(idx[1:-1:2], idx[2::2])))
    else:
        if len(idx) == 0:
            return []

        r = [slice(left, right) for left, right in zip(idx[:-1:2], idx[1::2])]

    if mask[-1]:
        r.append(slice(idx[-1], mask.size))
    return r
