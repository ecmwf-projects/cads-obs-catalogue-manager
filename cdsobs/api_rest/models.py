from pydantic import BaseModel

from cdsobs.retrieve.models import RetrieveArgs


class RetrievePayload(BaseModel):
    catalogue_url: str
    storage_url: str
    retrieve_args: RetrieveArgs
