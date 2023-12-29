from cdsobs.retrieve.retrieve_services import (
    estimate_data_size,
    filter_retrieve_constraints,
    merged_constraints_table,
)


def test_merged_constraints_table(mock_entries):
    result = merged_constraints_table(mock_entries)
    assert all(result[result["stations"] == "7"]["air_pressure"].values == [True, True])
    assert all(
        result[result["stations"] == "7"]["column_burden"].values == [True, False]
    )


def test_estimate_data_size(mock_entries, mock_retrieve_args):
    assert estimate_data_size(mock_entries, mock_retrieve_args) == 1


def test_get_retrieve_constraints(mock_entries, mock_retrieve_args):
    constraints_table = merged_constraints_table(mock_entries)
    response = filter_retrieve_constraints(constraints_table, mock_retrieve_args)
    assert len(response.time) == 2  # there are two dates with station 7
    assert len(response.variable_constraints) == 1
