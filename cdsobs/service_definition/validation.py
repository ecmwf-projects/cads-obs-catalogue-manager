import requests
from requests import HTTPError

from cdsobs.service_definition.service_definition_models import (
    ServiceDefinition,
)
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def get_dataset_service_definition(url: str) -> ServiceDefinition:
    """Get service definition file from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return ServiceDefinition(**response.json())
    except HTTPError as err:
        raise RuntimeError(
            "Problems when retrieving original service definition"
        ) from err
