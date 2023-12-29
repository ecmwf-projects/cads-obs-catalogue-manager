from datetime import datetime

import pandas as pd

from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema


def test_to_table():
    constraints = ConstraintsSchema(
        time=[datetime(2022, 1, 1)], variable_constraints={"tas": [1]}
    )
    df = constraints.to_table(["7", "8"])
    assert not df.loc[0, "tas"]
    assert df.loc[1, "tas"]


def from_table():
    constraints = ConstraintsSchema.from_table(
        pd.DataFrame(
            {
                "stations": ["7", "8"],
                "time": [datetime(2022, 1, 1), datetime(2022, 1, 1)],
                "tas": [False, True],
            }
        )
    )
    assert len(constraints.time) == 1
    assert "tas" in constraints.variable_constraints.keys()
    assert [1] == constraints.variable_constraints["tas"]
