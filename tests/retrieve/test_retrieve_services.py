from cdsobs.retrieve.retrieve_services import (
    merged_constraints_table,
)


def test_merged_constraints_table(mock_entries):
    result = merged_constraints_table(mock_entries).reset_index()
    expected = (
        "                   time      source version  air_pressure  column_burden\n"
        "0  1998-01-02, 00:00:00  OzoneSonde   1.0.0          True           True\n"
        "1  1998-01-04, 00:00:00  OzoneSonde   1.0.0          True          False"
    )

    assert result.to_string() == expected
