from cdsobs.retrieve.retrieve_services import (
    estimate_data_size,
    filter_retrieve_constraints,
    merged_constraints_table,
)


def test_merged_constraints_table(mock_entries):
    result = merged_constraints_table(mock_entries)
    expected = """  stations                  time      source version  air_pressure  column_burden
0        7  1998-01-02, 00:00:00  OzoneSonde   1.0.0          True           True
1        7  1998-01-04, 00:00:00  OzoneSonde   1.0.0          True          False
2        8  1998-01-02, 00:00:00  OzoneSonde   1.0.0          True          False"""
    assert result.to_string() == expected


def test_estimate_data_size(mock_entries, mock_retrieve_args):
    assert estimate_data_size(mock_entries, mock_retrieve_args) == 1


def test_get_retrieve_constraints(mock_entries, mock_retrieve_args):
    constraints_table = merged_constraints_table(mock_entries)
    response = filter_retrieve_constraints(constraints_table, mock_retrieve_args)
    assert len(response.time) == 2  # there are two dates with station 7
    assert len(response.variable_constraints) == 1
