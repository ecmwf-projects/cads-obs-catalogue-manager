import importlib
from pathlib import Path

import pydantic.error_wrappers
import yaml

from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.service_definition.validation import UpdateValidation, logger


def validate_service_definition(
    yml_file: str, is_update: bool = False
) -> tuple[ServiceDefinition | None, str]:
    """
    Validate fields of a service_definition.json candidate.

    Parameters
    ----------
    yml_file : str
      Input service_definition.yml.
    is_update : bool
      Wether we are updating an already defined dataset.

    Returns
    -------
    Valid ServiceDefinition and error conclusions
    """
    with open(yml_file) as f:
        verifying_dict = yaml.safe_load(f)
    verifying = None
    try:
        # pydantic checks everything when converting to class
        verifying = ServiceDefinition(**verifying_dict)

        errors = verifying.errors or any([s.errors for s in verifying.sources.values()])
    except pydantic.ValidationError as e:
        logger.error(e)
        errors = True

    if is_update:
        errors = UpdateValidation(verifying_dict, errors).main()

    if errors:
        msg = "There has been an error, check the logs for more information"
    else:
        msg = "Valid service definition yaml"

    logger.info(msg)
    return verifying, msg


def get_service_definition(dataset_name: str) -> ServiceDefinition:
    path_to_json = Path(
        str(importlib.resources.files("cdsobs")),
        f"data/{dataset_name}/service_definition.yml",
    )
    with open(path_to_json) as f:
        data = yaml.safe_load(f)

    return ServiceDefinition(**data)
