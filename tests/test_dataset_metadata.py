from pprint import pprint

from cdsobs.constants import DEFAULT_VERSION
from cdsobs.ingestion.core import IngestionRunParams
from cdsobs.metadata import get_dataset_metadata
from cdsobs.service_definition.api import get_service_definition


def test_get_dataset_metadata(test_config):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    service_definition = get_service_definition(test_config, dataset_name)
    run_params = IngestionRunParams(
        dataset_name, "TotalOzone", DEFAULT_VERSION, test_config, service_definition
    )
    actual = get_dataset_metadata(run_params)
    pprint(actual)
