import pytest

from cdsobs.cdm.api import (
    apply_unit_changes,
    check_cdm_compliance,
    get_aux_fields_mapping_from_service_definition,
    read_cdm_code_tables,
)
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.ingestion.api import read_batch_data
from cdsobs.ingestion.core import (
    TimeBatch,
    TimeSpaceBatch,
    get_aux_vars_from_service_definition,
    get_variables_from_service_definition,
)
from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.api import get_service_definition


def test_check_cdm_compliance(test_config, caplog):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(dataset_name)
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
        test_config, dataset_config, service_definition, source
    )
    homogenised_data = read_batch_data(
        test_config, dataset_metadata, service_definition, time_batch
    )
    return homogenised_data


def test_get_aux_fields_mapping_from_service_definition():
    dataset_name = (
        "insitu-observations-near-surface-temperature-us-climate-reference-network"
    )
    source = "USCRN_HOURLY"
    expected = {
        "air_temperature": [
            "air_temperature_positive_total_uncertainty",
            "air_temperature_negative_total_uncertainty",
            "air_temperature_max_positive_total_uncertainty",
            "air_temperature_max_negative_total_uncertainty",
            "air_temperature_min_positive_total_uncertainty",
            "air_temperature_min_negative_total_uncertainty",
            "air_temperature_random_uncertainty",
            "air_temperature_positive_systematic_uncertainty",
            "air_temperature_negative_systematic_uncertainty",
            "air_temperature_positive_quasisystematic_uncertainty",
            "air_temperature_negative_quasisystematic_uncertainty",
        ],
        "downward_shortwave_irradiance_at_earth_surface": [
            "downward_shortwave_irradiance_at_earth_surface_quality_flag"
        ],
        "hourly_maximum_downward_shortwave_irradiance_at_earth_surface": [
            "hourly_maximum_downward_shortwave_irradiance_at_earth_surface_quality_flag"
        ],
        "hourly_minimum_downward_shortwave_irradiance_at_earth_surface": [
            "hourly_minimum_downward_shortwave_irradiance_at_earth_surface_quality_flag"
        ],
        "relative_humidity": ["relative_humidity_quality_flag"],
        "soil_temperature": ["soil_temperature_quality_flag"],
        "hourly_maximum_soil_temperature": ["hourly_maximum_soil_temperature_flag"],
        "hourly_minimum_soil_temperature": [
            "hourly_minimum_soil_temperature_quality_flag"
        ],
    }

    service_definition = get_service_definition(dataset_name)
    source_definition = service_definition.sources[source]
    variables = get_variables_from_service_definition(service_definition, source)
    actual = get_aux_fields_mapping_from_service_definition(
        source_definition, variables
    )
    assert actual == expected


def test_apply_variable_unit_change(test_config):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(dataset_name)
    homogenised_data = _get_homogenised_data(
        dataset_name, service_definition, source, test_config
    )
    source_definition = service_definition.sources[source]
    cdm_code_tables = read_cdm_code_tables(test_config.cdm_tables_location)
    actual = apply_unit_changes(
        homogenised_data, source_definition, cdm_code_tables["observed_variable"]
    )
    assert "original_units" in actual and "units" in actual and len(actual) > 0

    with pytest.raises(RuntimeError):
        source_definition.descriptions["geopotential_height"].units = "wrong"
        apply_unit_changes(
            homogenised_data, source_definition, cdm_code_tables["observed_variable"]
        )


def test_get_aux_vars_from_service_definition():
    dataset_name = (
        "insitu-observations-near-surface-temperature-us-climate-reference-network"
    )
    source = "USCRN_HOURLY"
    service_definition = get_service_definition(dataset_name)
    actual = get_aux_vars_from_service_definition(service_definition, source)
    print(actual)
