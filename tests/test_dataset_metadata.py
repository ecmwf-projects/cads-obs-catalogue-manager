from pprint import pprint

from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.api import get_service_definition


def test_get_dataset_metadata(test_config):
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"
    dataset_config = test_config.get_dataset(dataset)
    service_definition = get_service_definition(test_config, dataset)
    actual = get_dataset_metadata(
        test_config, dataset_config, service_definition, "TotalOzone", "1.0.0"
    )
    pprint(actual)
