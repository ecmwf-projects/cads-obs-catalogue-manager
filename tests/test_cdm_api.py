import pytest

from cdsobs.cdm.api import (
    check_cdm_compliance,
    define_units,
    read_cdm_code_tables,
)
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.api import read_batch_data
from cdsobs.ingestion.core import (
    TimeBatch,
    TimeSpaceBatch,
)
from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.api import get_service_definition


def test_check_cdm_compliance(test_config, caplog):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(test_config, dataset_name)
    homogenised_data = _get_homogenised_data(
        dataset_name, service_definition, source, test_config
    )
    available_cdm_tables = test_config.get_dataset(dataset_name).available_cdm_tables
    cdm_tables = read_cdm_tables(test_config.cdm_tables_location, available_cdm_tables)
    cdm_fields_mapping = check_cdm_compliance(homogenised_data, cdm_tables)
    assert len(cdm_fields_mapping) > 1


def _get_homogenised_data(dataset_name, service_definition, source, test_config):
    dataset_config = test_config.get_dataset(dataset_name)
    time_batch = TimeSpaceBatch(TimeBatch(year=1969, month=2))
    dataset_metadata = get_dataset_metadata(
        test_config, dataset_config, service_definition, source, DEFAULT_VERSION
    )
    homogenised_data = read_batch_data(
        test_config, dataset_metadata, service_definition, time_batch
    )
    return homogenised_data


def test_apply_variable_unit_change(test_config):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(test_config, dataset_name)
    homogenised_data = _get_homogenised_data(
        dataset_name, service_definition, source, test_config
    )
    source_definition = service_definition.sources[source]
    cdm_code_tables = read_cdm_code_tables(test_config.cdm_tables_location)
    actual = define_units(
        homogenised_data, source_definition, cdm_code_tables["observed_variable"]
    )
    assert "original_units" in actual and "units" in actual and len(actual) > 0

    with pytest.raises(RuntimeError):
        source_definition.descriptions["geopotential_height"].units = "wrong"
        define_units(
            homogenised_data, source_definition, cdm_code_tables["observed_variable"]
        )
