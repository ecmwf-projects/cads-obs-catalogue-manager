import logging
from pathlib import Path

from cdsobs.service_definition.api import (
    get_service_definition,
    validate_service_definition,
)
from tests.utils import hash_string


def test_get_service_definition(test_config):
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = get_service_definition(test_config, dataset_name)
    assert hash_string(service_definition) == "80748d92c64a18f3ccc2e192099d3d90e906e4b7"


def test_new_service_definition_valid(caplog, test_config):
    service_definition = Path(
        test_config.cads_obs_config_location,
        "cads-obs-config",
        "service-definitions",
        "insitu-observations-igra-baseline-network.yml",
    )
    with caplog.at_level(logging.ERROR):
        validate_service_definition(
            str(service_definition),
            test_config.cdm_tables_location,
            validate_cdm=True,
        )
    print(caplog.text)
