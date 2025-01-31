import logging
from pathlib import Path

from cdsobs.service_definition.api import validate_service_definition


def test_new_service_definition_valid(caplog, test_config):
    service_definition = Path(
        test_config.cads_obs_insitu_location,
        "cads-forms-insitu",
        "insitu-observations-igra-baseline-network/service_definition.yml",
    )
    with caplog.at_level(logging.ERROR):
        validate_service_definition(
            str(service_definition),
            test_config.cdm_tables_location,
            validate_cdm=True,
        )
    print(caplog.text)
