from pprint import pprint

import yaml

from cdsobs.constants import SERVICE_DEFINITION_YML
from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.service_definition_models import ServiceDefinition


def test_get_dataset_metadata(test_config):
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"
    dataset_config = test_config.get_dataset(dataset)
    new_sc_dict = yaml.safe_load(SERVICE_DEFINITION_YML.read_text())
    service_definition = ServiceDefinition(**new_sc_dict)
    actual = get_dataset_metadata(
        test_config, dataset_config, service_definition, "TotalOzone"
    )
    pprint(actual)
