import logging

from cdsobs.constants import SERVICE_DEFINITION_YML
from cdsobs.service_definition.api import validate_service_definition


def test_new_service_definition_valid(caplog):
    with caplog.at_level(logging.ERROR):
        validate_service_definition(SERVICE_DEFINITION_YML)
    print(caplog.text)
