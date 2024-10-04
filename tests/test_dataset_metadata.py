from pathlib import Path
from pprint import pprint

import yaml

from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.service_definition_models import ServiceDefinition


def test_get_dataset_metadata(test_config):
    dataset = "insitu-observations-gruan-reference-network"
    dataset_config = test_config.get_dataset(dataset)
    sc_path = Path(
        "../cdsobs/data/insitu-observations-near-surface-temperature-us-climate-reference-network/service_definition_new.yml"
    )
    new_sc_dict = yaml.safe_load(sc_path.read_text())
    # new_sc_dict["sources"]["GRUAN"]["main_variables"].append("wrong_variable")
    # del new_sc_dict["sources"]["GRUAN"]["descriptions"]["air_temperature"]
    service_definition = ServiceDefinition(**new_sc_dict)
    actual = get_dataset_metadata(
        test_config, dataset_config, service_definition, "uscrn_daily"
    )
    pprint(actual)
