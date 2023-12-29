from cdsobs.cdm.api import read_cdm_code_tables
from cdsobs.cdm.tables import read_cdm_tables


def test_read_cdm_table(test_config):
    cdm_tables = read_cdm_tables(test_config.cdm_tables_location)
    assert len(cdm_tables) > 0


def test_read_cdm_code_tables(test_config):
    cdm_code_table = read_cdm_code_tables(test_config.cdm_tables_location)
    assert len(cdm_code_table) > 0
