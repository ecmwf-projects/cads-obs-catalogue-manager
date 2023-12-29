from cdsobs.cdm.api import to_cdm_dataset_normalized
from cdsobs.cdm.denormalize import denormalize_tables
from cdsobs.cdm.tables import read_cdm_tables


def test_denormalize_tables(test_partition, test_config):
    # For this to work we need a record record_number for stations, which is not
    # available for WOUDC dataset. We only use this function for CUON now, so we add a
    # record number here to test it.
    test_partition.data["record_number"] = 1
    cdm_tables = read_cdm_tables(test_config.cdm_tables_location)
    cdm_dataset = to_cdm_dataset_normalized(test_partition, cdm_tables)
    tables_to_use = ["observation_table", "header_table", "station_configuration"]
    dn_table = denormalize_tables(
        cdm_tables, cdm_dataset.dataset, tables_to_use, ignore_errors=False
    )
    print(dn_table)
