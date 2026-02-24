"""
Factorize a table.

Factorize a table producing a dataframe where rows are sets of values of the original
columns.
The factorization can be exploded in a dataframe containing the exact same rows of the
original table minus duplicates.
The reconstructed table does not have necessarily the same order of rows, this may be
seen as a
 compression method it looses the order.
In case the table to be factorized contains a columns where all values are different
(i. e. an index) the output will be
the original dataframe where individual table cells values are substituted with tuples
of the single value.

main methods are:



"""

from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd

# factoring the keys year, month, day of a time series is an exercise for which we already know what to expect
# output groups expected are all years the months with 30 and 31 days, 28 and bisestile years distinct group.


def aggregate(a_df: pd.DataFrame, *args: str) -> tuple:
    aggregate_on = args[0]
    if not isinstance(a_df[aggregate_on].iloc[0], tuple):
        out = list(set(a_df[aggregate_on]))
    else:
        out = list(set().union(*[set(y) for y in a_df[aggregate_on]]))
    out.sort()
    return tuple(out)


def group_by_aggregate_on(
    a_dataframe: pd.DataFrame, group_list: list[str], aggregate_on: str
) -> pd.DataFrame:
    a_dataframe = a_dataframe.drop_duplicates(keep="last")
    out = a_dataframe.groupby(group_list).apply(aggregate, aggregate_on)
    return pd.DataFrame(out.rename(aggregate_on)).reset_index()


def all_perms(elements: list) -> Iterable[list]:
    if len(elements) <= 1:
        yield elements
    else:
        for perm in all_perms(elements[1:]):
            for i in range(len(elements)):
                # nb elements[0:1] works in both string and list contexts
                yield perm[:i] + elements[0:1] + perm[i:]


def sort_by_uniques(df: pd.DataFrame, columns: list[str]) -> list[str]:
    nu = df[columns].nunique().to_dict()
    k = list(nu.keys())
    try:
        return [
            x for _, x in sorted(zip([nu[ak] for ak in k], k), key=lambda pair: pair[0])
        ]
    except Exception:
        return [
            x
            for _, x in sorted(
                zip(
                    k,
                    [nu[ak] for ak in k],
                ),
                key=lambda pair: pair[0],
            )
        ]


def process_all_groups(a_dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # order of columns can influence outcome groups
    # we assume that columns are ordered by sort_by_uniques
    # we want to aggregate on the most varying column firs, so ve reverse
    # columns.reverse()
    out = a_dataframe.copy()
    for c in columns:
        group_list = columns.copy()
        group_list.remove(c)
        out = group_by_aggregate_on(out, group_list, c)
    # columns.reverse()
    return out[columns]


def iterative_ordering(a_dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # we order the columns in place and serach for the minimum memory usage following some scheme ...
    old = a_dataframe.copy()
    new = a_dataframe.copy()
    it = 0
    while (
        new.memory_usage(index=True).sum() <= old.memory_usage(index=True).sum()
        and it < 3
    ):
        if new.memory_usage(index=True).sum() == old.memory_usage(index=True).sum():
            it += 1
        else:
            it = 0
        old = new.copy()
        new_ordered = sort_by_uniques(old, columns)
        new = process_all_groups(old, new_ordered)
    return new


def explode_constraints(dff: pd.DataFrame) -> pd.DataFrame:
    # expand the constraints dataframe
    keys = list(dff.keys())
    out = pd.DataFrame(columns=keys)
    for i, a_row in dff.iterrows():
        dfs = [pd.DataFrame({k: a_row[k]}) for k in keys]
        for df in dfs:
            df["xxxkxxx"] = 1
        for i in range(1, len(keys)):
            dfs[i] = pd.merge(dfs[i - 1], dfs[i], on="xxxkxxx")
        del dfs[-1]["xxxkxxx"]
        out = pd.concat([out, dfs[-1]])
    return out


def test_factorization():
    start, end = datetime(2000, 1, 1), datetime(2005, 12, 31)

    t_list = [
        start + timedelta(hours=i)
        for i in range(int((end - start).total_seconds() / 3600) + 1)
    ]

    df = pd.DataFrame({"date": t_list})

    keys = ["year", "month", "day"]

    for what in keys:
        df[what] = [getattr(d, what) for d in t_list]

    print(df[keys].drop_duplicates(keep="last"))
    print(explode_constraints(iterative_ordering(df.copy(), keys)))


if __name__ == "__main__":
    test_factorization()
