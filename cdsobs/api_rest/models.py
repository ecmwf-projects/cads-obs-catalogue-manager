from pydantic import BaseModel

from cdsobs.retrieve.models import RetrieveArgs


class RetrieveConfig(BaseModel):
    size_limit: int = 10000


class RetrievePayload(BaseModel):
    retrieve_args: RetrieveArgs
    config: RetrieveConfig
