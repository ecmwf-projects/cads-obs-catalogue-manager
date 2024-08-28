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
from cdsobs.service_definition.service_definition_models import UnitChange


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
    source = "uscrn_hourly"
    expected = {
        "accumulated_precipitation": [],
        "air_temperature": [
            {
                "auxvar": "air_temperature_positive_total_uncertainty",
                "metadata_name": "positive_total_uncertainty",
            },
            {
                "auxvar": "air_temperature_negative_total_uncertainty",
                "metadata_name": "negative_total_uncertainty",
            },
            {
                "auxvar": "air_temperature_random_uncertainty",
                "metadata_name": "random_uncertainty",
            },
            {
                "auxvar": "air_temperature_positive_systematic_uncertainty",
                "metadata_name": "positive_systematic_uncertainty",
            },
            {
                "auxvar": "air_temperature_negative_systematic_uncertainty",
                "metadata_name": "negative_systematic_uncertainty",
            },
            {
                "auxvar": "air_temperature_positive_quasisystematic_uncertainty",
                "metadata_name": "positive_quasisystematic_uncertainty",
            },
            {
                "auxvar": "air_temperature_negative_quasisystematic_uncertainty",
                "metadata_name": "negative_quasisystematic_uncertainty",
            },
        ],
        "daily_maximum_air_temperature": [],
        "daily_minimum_air_temperature": [],
        "maximum_soil_temperature": [
            {"auxvar": "maximum_soil_temperature_flag", "metadata_name": "quality_flag"}
        ],
        "maximum_solar_irradiance": [
            {
                "auxvar": "maximum_solar_irradiance_quality_flag",
                "metadata_name": "quality_flag",
            }
        ],
        "minimum_soil_temperature": [
            {
                "auxvar": "minimum_soil_temperature_quality_flag",
                "metadata_name": "quality_flag",
            }
        ],
        "minimum_solar_irradiance": [
            {
                "auxvar": "minimum_solar_irradiance_quality_flag",
                "metadata_name": "quality_flag",
            }
        ],
        "relative_humidity": [
            {
                "auxvar": "relative_humidity_quality_flag",
                "metadata_name": "quality_flag",
            }
        ],
        "soil_moisture_100cm_from_earth_surface": [],
        "soil_moisture_10cm_from_earth_surface": [],
        "soil_moisture_20cm_from_earth_surface": [],
        "soil_moisture_50cm_from_earth_surface": [],
        "soil_moisture_5cm_from_earth_surface": [],
        "soil_temperature": [
            {
                "auxvar": "soil_temperature_quality_flag",
                "metadata_name": "quality_flag",
            },
            {
                "auxvar": "soil_temperature_processing_level",
                "metadata_name": "processing_level",
            },
        ],
        "soil_temperature_100cm_from_earth_surface": [],
        "soil_temperature_10cm_from_earth_surface": [],
        "soil_temperature_20cm_from_earth_surface": [],
        "soil_temperature_50cm_from_earth_surface": [],
        "soil_temperature_5cm_from_earth_surface": [],
        "solar_irradiance": [
            {"auxvar": "solar_irradiance_quality_flag", "metadata_name": "quality_flag"}
        ],
    }
    service_definition = get_service_definition(dataset_name)
    source_definition = service_definition.sources[source]
    variables = get_variables_from_service_definition(service_definition, source)
    actual = get_aux_fields_mapping_from_service_definition(
        source_definition, variables
    )
    assert actual == expected
    assert actual.all_list == [
        "air_temperature_positive_total_uncertainty",
        "air_temperature_negative_total_uncertainty",
        "air_temperature_random_uncertainty",
        "air_temperature_positive_systematic_uncertainty",
        "air_temperature_negative_systematic_uncertainty",
        "air_temperature_positive_quasisystematic_uncertainty",
        "air_temperature_negative_quasisystematic_uncertainty",
        "maximum_soil_temperature_flag",
        "maximum_solar_irradiance_quality_flag",
        "minimum_soil_temperature_quality_flag",
        "minimum_solar_irradiance_quality_flag",
        "relative_humidity_quality_flag",
        "soil_temperature_quality_flag",
        "soil_temperature_processing_level",
        "solar_irradiance_quality_flag",
    ]

    assert not actual.var_has_uncertainty_field("accumulated_precipitation")
    assert actual.var_has_uncertainty_field("air_temperature")
    assert actual.vars_with_uncertainty_field == ["air_temperature"]
    assert actual.quality_flag_fields == [
        "maximum_soil_temperature_flag",
        "maximum_solar_irradiance_quality_flag",
        "minimum_soil_temperature_quality_flag",
        "minimum_solar_irradiance_quality_flag",
        "relative_humidity_quality_flag",
        "soil_temperature_quality_flag",
        "solar_irradiance_quality_flag",
    ]
    assert not actual.var_has_quality_field("air_temperature")
    assert actual.var_has_quality_field("maximum_soil_temperature")
    assert actual.vars_with_quality_field == [
        "maximum_soil_temperature",
        "maximum_solar_irradiance",
        "minimum_soil_temperature",
        "minimum_solar_irradiance",
        "relative_humidity",
        "soil_temperature",
        "solar_irradiance",
    ]
    assert (
        actual.get_var_quality_flag_field_name("maximum_soil_temperature")
        == "maximum_soil_temperature_flag"
    )
    assert actual.get_var_uncertainty_field_names("air_temperature") == [
        "air_temperature_positive_total_uncertainty",
        "air_temperature_negative_total_uncertainty",
        "air_temperature_random_uncertainty",
        "air_temperature_positive_systematic_uncertainty",
        "air_temperature_negative_systematic_uncertainty",
        "air_temperature_positive_quasisystematic_uncertainty",
        "air_temperature_negative_quasisystematic_uncertainty",
    ]
    assert (
        actual.auxfield2metadata_name(
            "air_temperature", "air_temperature_positive_total_uncertainty"
        )
        == "positive_total_uncertainty"
    )


def test_apply_variable_unit_change(test_config):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(dataset_name)
    homogenised_data = _get_homogenised_data(
        dataset_name, service_definition, source, test_config
    )
    source_definition = service_definition.sources[source]
    source_definition.descriptions["geopotential_height"].units = "J kg-1"
    source_definition.cdm_mapping.unit_changes = dict()
    source_definition.cdm_mapping.unit_changes["geopotential_height"] = UnitChange(
        names={"m": "J kg-1"}, offset=0, scale=0.01020408163265306
    )
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
    source = "uscrn_hourly"
    service_definition = get_service_definition(dataset_name)
    actual = get_aux_vars_from_service_definition(service_definition, source)
    print(actual)
