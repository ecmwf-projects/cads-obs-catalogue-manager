import pandas as pd
import pytest

from cdsobs.cdm.api import define_units, read_cdm_code_tables, to_cdm_dataset
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.core import (
    DatasetPartition,
    IngestionRunParams,
    PartitionParams,
    TimeBatch,
)
from cdsobs.metadata import get_dataset_metadata


def test_to_cdm_dataset_runtime_error(test_config, test_sds):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = test_sds.get(dataset_name)
    run_params = IngestionRunParams(
        dataset_name, source, DEFAULT_VERSION, test_config, service_definition
    )
    dataset_metadata = get_dataset_metadata(run_params)

    # Create a partition with extra columns not in CDM
    data = pd.DataFrame(
        {
            "observation_id": [1, 2],
            "ozone_partial_pressure": [10.0, 20.0],
            # "extra_col" is not in CDM tables and not dropped
            "extra_col": [1, 2],
        }
    )

    partition_params = PartitionParams(
        time_batch=TimeBatch(1969, 1),
        latitude_coverage_start=0,
        longitude_coverage_start=0,
        lat_tile_size=1.0,
        lon_tile_size=1.0,
        stations_ids=[],
        sources=[],
    )

    from typing import cast

    from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema

    partition = DatasetPartition(
        dataset_metadata, partition_params, data, cast(ConstraintsSchema, None)
    )

    with pytest.raises(
        RuntimeError, match="The following fields where read but are not in the CDM"
    ):
        to_cdm_dataset(partition)


def test_read_cdm_code_tables_explicit(test_config):
    tables = ["observed_variable", "units"]
    cdm_tables = read_cdm_code_tables(
        test_config.cdm_tables_location, tables_to_use=tables
    )
    assert "observed_variable" in cdm_tables
    assert "units" in cdm_tables
    assert "station_type" not in cdm_tables


def test_define_units_runtime_error(test_config, test_sds):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = test_sds.get(dataset_name)
    source_definition = service_definition.sources[source]
    cdm_code_tables = read_cdm_code_tables(test_config.cdm_tables_location)

    # Create a dataframe compatible with the function
    # It needs "observed_variable", "observation_value", "units", "original_units"
    data = pd.DataFrame(
        {
            "observed_variable": ["geopotential_height"],
            "observation_value": [100.0],
            "units": ["m"],
            "original_units": ["m"],
        }
    )

    # Temporarily change description units to cause mismatch
    # In YAML/code, geopotential_height mapping converts 'Pa' -> 'm'
    # So new_units = 'm'.
    # If description units (original) is NOT 'm' (e.g. 'ft') then
    # new_units ('m') != description_units ('ft') -> RuntimeError

    original_units = source_definition.descriptions["geopotential_height"].units
    # We set description units to 'ft'.
    # get_unit_change will return 'm' (from CDM mapping in YAML)
    # 25:         geopotential_height:
    # 26:           names:
    # 27:             Pa: m
    source_definition.descriptions["geopotential_height"].units = "ft"

    try:
        with pytest.raises(
            RuntimeError, match="New units set in CDM mapping section must agree"
        ):
            define_units(data, source_definition, cdm_code_tables["observed_variable"])
    finally:
        source_definition.descriptions["geopotential_height"].units = original_units
