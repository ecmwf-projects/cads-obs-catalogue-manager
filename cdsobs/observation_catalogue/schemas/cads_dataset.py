from pydantic import BaseModel

from cdsobs.utils.types import StrNotBlank


class CadsDatasetSchema(BaseModel):
    """Schema for the cads_dataset table in the catalogue.

    Each row represents a dataset. It uses dataset names as primary key to keep thing
    more simple.
    """

    name: StrNotBlank
