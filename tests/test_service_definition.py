import yaml

from cdsobs.constants import SERVICE_DEFINITION_YML
from cdsobs.service_definition.api import validate_service_definition
from cdsobs.service_definition.service_definition_models import ServiceDefinition


def test_new_service_definition_valid():
    response = validate_service_definition(SERVICE_DEFINITION_YML)
    assert response[1] == "Valid service definition yaml"


def test_new_service_definition_invalid(mocker):
    mocker.patch(
        "yaml.safe_load", return_value={"products_hierarchy": ["test", "test"]}
    )
    response = validate_service_definition(SERVICE_DEFINITION_YML)
    assert response[1] == "There has been an error, check the logs for more information"


def test_update_service_definition(mocker):
    with open(SERVICE_DEFINITION_YML) as f:
        service_definition = yaml.safe_load(f)
    service_definition["sources"].pop("OzoneSonde")
    mocked_sd = ServiceDefinition(**service_definition)
    mocker.patch(
        "cdsobs.service_definition.validation.get_dataset_service_definition",
        return_value=mocked_sd,
    )
    response = validate_service_definition(SERVICE_DEFINITION_YML, is_update=True)
    assert response[1] == "Valid service definition yaml"


def test_update_service_definition_invalid(mocker):
    with open(SERVICE_DEFINITION_YML) as f:
        service_definition_dict = yaml.safe_load(f)
    service_definition_dict["sources"]["TestSource"] = service_definition_dict[
        "sources"
    ]["OzoneSonde"]
    mocked_sd = ServiceDefinition(**service_definition_dict)
    mocker.patch(
        "cdsobs.service_definition.validation.get_dataset_service_definition",
        return_value=mocked_sd,
    )
    response = validate_service_definition(SERVICE_DEFINITION_YML, is_update=True)
    assert response[1] == "There has been an error, check the logs for more information"
