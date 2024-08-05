import hashlib
import json
from pathlib import Path
from typing import Sequence, cast

import h5netcdf
import numpy
import pandas
import xarray
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cdsobs import constants
from cdsobs.utils.types import ByteSize


def compute_hash(ipath: Path, hash_function=hashlib.sha256, block_size=10048576):
    """Compute a hash in a memory efficient way using 10Mb blocks."""
    with ipath.open("rb") as f:
        file_hash = hash_function()
        while True:
            # we use the read passing the size of the block to avoid
            # heavy ram usage
            data = f.read(block_size)
            if not data:
                # if we don't have any more data to read, stop.
                break
            # we partially calculate the hash
            file_hash.update(data)
    return file_hash.hexdigest()


def get_file_size(path: Path) -> ByteSize:
    """Get sie of a file in bytes."""
    result = cast(ByteSize, path.stat().st_size)
    return result


def jsonable_encoder(obj_in: BaseModel):
    """
    Convert pydantic object to a python object serializable to JSON.

    Replaces fastapi jsonable_encoder. Serializing to string and back to python is a
    workaround until pydantic 2.0 is out.
    """
    return json.loads(obj_in.model_dump_json())


def unique(sequence: Sequence) -> list:
    """Get unique values keeping the order."""
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]  # type: ignore[func-returns-value]


def get_code_mapping(
    incobj: h5netcdf.File | xarray.Dataset, inverse: bool = False
) -> dict:
    if isinstance(incobj, h5netcdf.File):
        attrs = incobj.variables["observed_variable"].attrs
    elif isinstance(incobj, xarray.Dataset):
        attrs = incobj["observed_variable"].attrs
    else:
        raise RuntimeError("Unsupported input type")
    labels = attrs["labels"]
    codes = attrs["codes"]
    if not isinstance(attrs["labels"], list):
        labels = [
            labels,
        ]
        codes = [
            codes,
        ]
    if inverse:
        mapping = {c: v for v, c in zip(labels, codes)}
    else:
        mapping = {v: c for v, c in zip(labels, codes)}
    return mapping


def datetime_to_seconds(dates: pandas.Series) -> numpy.ndarray:
    """From datetime64 to seconds since a reference time."""
    ref = constants.TIME_UNITS_REFERENCE_DATE
    return ((dates - numpy.datetime64(ref)) / numpy.timedelta64(1, "s")).astype(
        numpy.int64
    )


def get_database_session(url: str) -> Session:
    engine = create_engine(url)  # echo=True for more descriptive logs
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()
