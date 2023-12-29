import requests
from requests import HTTPError

from cdsobs.service_definition.service_definition_models import (
    Product,
    ServiceDefinition,
)
from cdsobs.service_definition.utils import custom_assert
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


class UpdateValidation:
    def __init__(self, verifying_dict: dict, errors: bool):
        """
        Check current service definition candidate against its previous version.

        Parameters
        ----------
        verifying_dict :
          service definition to verify
        errors :
          true if the validation finds something wrong, false otherwise
        """
        self.verifying_dict = verifying_dict
        self.errors = errors
        self.original = get_dataset_service_definition("")

    def main(self) -> bool:
        for k in self.original.sources:
            self.errors = custom_assert(
                k in self.verifying_dict["sources"],
                f"Source {k} is not specified. Changes in the "
                f"service configuration can add new data "
                f"sources, not reduce them",
                self.errors,
            )

        new_sources = list(
            set(self.verifying_dict["sources"].keys()).difference(
                self.original.sources.keys()
            )
        )

        if len(new_sources) > 0:
            logger.info(f"New sources detected: {new_sources}")

        self.check_products()

        return self.errors

    def check_products(self):
        original_sources = self.original.sources
        verifying_sources = self.verifying_dict["sources"]
        for original_source, original_source_def in original_sources.items():
            try:
                verifying_products = [
                    Product(**pdict)
                    for pdict in verifying_sources[original_source]["products"]
                ]
            except KeyError:
                logger.error(
                    f"Source {original_source} not found in new service definition"
                )
                self.errors = True
            else:
                for op in original_source_def.products:
                    self.errors = custom_assert(
                        op in verifying_products,
                        f"Product {op} is missing. Products "
                        f"can not be removed or renamed, only "
                        f"added",
                        self.errors,
                    )


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
