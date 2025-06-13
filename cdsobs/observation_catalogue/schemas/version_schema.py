from pydantic import BaseModel
from pydantic_extra_types.semantic_version import SemanticVersion

from cdsobs.utils.types import StrNotBlank


class CadsDatasetVersionSchema(BaseModel):
    """Schema for the cads_dataset table in the catalogue.

    Each row represents a dataset. It uses dataset names as primary key to keep thing
    more simple. Version is set as deprecated by default, to make it discoverable, it
    but be enabled by .
    """

    dataset: StrNotBlank
    version: SemanticVersion
    deprecated: bool = True
