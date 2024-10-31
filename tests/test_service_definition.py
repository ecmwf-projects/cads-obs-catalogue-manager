import logging
from pathlib import Path

from cdsobs.constants import cdsobs_path
from cdsobs.service_definition.api import validate_service_definition


def test_new_service_definition_valid(caplog, test_config):
    SERVICE_DEFINITION_YML = Path(
        cdsobs_path,
        "data/insitu-observations-ndacc/service_definition.yml",
    )
    with caplog.at_level(logging.ERROR):
        validate_service_definition(
            str(SERVICE_DEFINITION_YML),
            test_config.cdm_tables_location,
            validate_cdm=True,
        )
    print(caplog.text)
