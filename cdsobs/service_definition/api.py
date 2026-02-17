from pathlib import Path

import pydantic.error_wrappers
import yaml

from cdsobs.cdm.api import get_cdm_fields, read_cdm_code_table
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.config import CDSObsConfig
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def validate_service_definition(
    yml_file: str, cdm_tables_location: Path, validate_cdm=False
) -> tuple[ServiceDefinition | None, str]:
    """
    Validate fields of a service_definition.json candidate.

    Parameters
    ----------
    yml_file : str
      Input service_definition.yml.
    cdm_tables_location: Path
      Location of CDM variables
    validate_cdm: bool
      Check if the variable and field names are CDM compliant.

    Returns
    -------
    Valid ServiceDefinition
    """
    with open(yml_file) as f:
        service_definition_dict = yaml.safe_load(f)

    try:
        # pydantic checks everything when converting to class
        service_definition = ServiceDefinition(**service_definition_dict)
        error_message = ""
    except pydantic.ValidationError as e:
        service_definition = None
        logger.error(e)
        error_message = str(e)

    # Validate the CDM compliance here so we don't need to read the data.
    cdm_tables = read_cdm_tables(cdm_tables_location)
    cdm_variable_table = read_cdm_code_table(
        cdm_tables_location, "observed_variable"
    ).table
    cdm_variables = cdm_variable_table.name.str.replace(" ", "_").tolist()
    cdm_fields = get_cdm_fields(cdm_tables)

    if validate_cdm:
        validate_cdm_in_sc(cdm_fields, cdm_variables, service_definition)

    if len(error_message) > 0:
        msg = "There has been an error, check the logs for more information"
    else:
        msg = "Valid service definition yaml"

    logger.info(msg)
    return service_definition, msg


def validate_cdm_in_sc(cdm_fields, cdm_variables, service_definition):
    for source_name, source in service_definition.sources.items():
        main_variables = source.main_variables
        variables_missing = sorted(set(main_variables) - set(cdm_variables))
        if len(variables_missing) > 0:
            raise AssertionError(
                "The following main_variables do not have"
                f"CDM compliant names {variables_missing}"
            )

        descriptions_no_main_vars = [
            d for d in source.descriptions if d not in main_variables
        ]
        fields_missing = sorted(set(descriptions_no_main_vars) - set(cdm_fields))
        if len(fields_missing) > 0:
            raise AssertionError(
                "The following fields in descriptions do not have"
                f"CDM compliant names {fields_missing}"
            )


def get_service_definition(
    config: CDSObsConfig, dataset_name: str
) -> ServiceDefinition:
    cadsobs_insitu_location = config.cads_obs_config_location
    path_to_yaml = Path(
        cadsobs_insitu_location,
        "cads-obs-config",
        "service-definitions",
        f"{dataset_name}.yml",
    )
    logger.info(f"Reading service definition file from {path_to_yaml}")
    with open(path_to_yaml) as f:
        data = yaml.safe_load(f)

    return ServiceDefinition(**data, path=path_to_yaml)
