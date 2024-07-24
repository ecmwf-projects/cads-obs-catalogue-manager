from typing import Annotated, Literal

import pydantic
from pydantic import AfterValidator
from pydantic_core import PydanticCustomError

LonTileSize = Literal[360, 180, 90, 45, 30, 20, 15, 10, 5, 3, 2, 1]
LatTileSize = Literal[180, 90, 45, 30, 20, 15, 10, 5, 3, 2, 1]
TimeTileSize = Literal["month", "year"]
ByteSize = Annotated[int, pydantic.Field(gt=0)]
StrNotBlank = Annotated[str, pydantic.Field(min_length=1)]
BoundedLat = Annotated[float, pydantic.Field(ge=-90, le=90)]
BoundedLon = Annotated[float, pydantic.Field(ge=-180, le=360)]


def _validate_unique_list(v: list) -> list:
    if len(v) != len(set(v)):
        raise PydanticCustomError("unique_list", "List must be unique")
    return v


def _validate_len(v: list) -> list:
    if len(v) == 0:
        raise PydanticCustomError("unique_list", "List must be unique")
    return v


NotEmptyUniqueStrList = Annotated[
    list[str], AfterValidator(_validate_unique_list), _validate_len
]
