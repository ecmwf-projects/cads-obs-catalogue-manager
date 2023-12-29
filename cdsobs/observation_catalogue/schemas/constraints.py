from datetime import datetime
from itertools import product
from typing import List, cast

import pandas
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from cdsobs.cdm.tables import STATION_COLUMN

DATE_FORMAT = "%Y-%m-%d"


class ConstraintsSchema(BaseModel):
    time: list[datetime] | list[str]
    variable_constraints: dict[str, list[int]]
    # assuming dims are always ["stations", "time"] at the moment
    dims: list[str] = Field(default_factory=lambda: ["stations", "time"])

    @classmethod
    @field_validator("time", mode="before")
    def parse_time(cls, values: list[str]):
        if isinstance(values[0], str):
            return [datetime.strptime(v, DATE_FORMAT) for v in values]

    def jsonable(self):
        self.time = [t.strftime(DATE_FORMAT) for t in self.time]
        return self

    def to_table(self, stations: list[str]) -> pd.DataFrame:
        dim_columns = list(product(stations, self.time))
        df = pd.DataFrame(dim_columns, columns=self.dims)
        # forcing order
        df.sort_values(self.dims, inplace=True)
        for var, indices in self.variable_constraints.items():
            df[var] = df.index.isin(indices)
        return df

    @classmethod
    def from_table(cls, table: pd.DataFrame):
        # Forcing order
        table = table.sort_values(["stations", "time"])
        variables = [c for c in list(table) if c not in ["stations", "time"]]
        sorted_variables = cast(List[str], sorted(variables))
        variable_constraints: dict[str, list[int]] = {}
        for var in sorted_variables:
            variable_constraints[var] = table.index[table[var]].tolist()
        # Time can be str
        if table["time"].dtype == "object":
            time = table["time"].astype("datetime64[ns]")
        else:
            time = table["time"]  # type: ignore[assignment]
        # Convert to UTC
        return cls(
            time=time.drop_duplicates().sort_values().dt.tz_localize(None).tolist(),
            variable_constraints=variable_constraints,
        )

    def get_num_obs(self) -> int:
        obs = set()
        for v in self.variable_constraints.values():
            obs.update(v)
        return len(obs) * len(self.variable_constraints)


def get_partition_constraints(
    partition_data: pd.DataFrame, time_column: str = "report_timestamp"
) -> ConstraintsSchema:
    """
    Find which values are constraints given the complete data_table.

    Parameters
    ----------
    partition_data:
      Dataframe with the data in the partition.
    time_column:
      Optional, name of the time columns, default is "report_timestamp

    Returns a list of constraints
    -------

    """
    partition_data = partition_data[["observed_variable", STATION_COLUMN, time_column]]
    # We only take into account daily granularity
    grouper = [
        "observed_variable",
        STATION_COLUMN,
        pandas.Grouper(key=time_column, freq="D"),
    ]
    df_constraints = partition_data.groupby(grouper, as_index=False).count()
    df_constraints["values"] = True
    df = df_constraints.pivot(
        columns="observed_variable",
        index=[time_column, STATION_COLUMN],
        values="values",
    )
    df.fillna(False, inplace=True)
    df.reset_index(inplace=True)
    df = df.rename({STATION_COLUMN: "stations", time_column: "time"}, axis=1)
    return ConstraintsSchema.from_table(df)
