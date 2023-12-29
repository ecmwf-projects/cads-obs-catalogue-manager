from typing import Hashable, Literal

import pandas
import xarray

StringTransform = Literal["char_to_str", "str_to_char", None]


def get_encoding_with_compression(
    dataset: pandas.DataFrame, string_transform: StringTransform = None
) -> dict[str | Hashable, dict]:
    encoding: dict[str | Hashable, dict] = dict()
    # Set compresison for the observations table
    for var in dataset.columns:
        encoding.update({var: dict(compression="gzip", compression_opts=1)})
        match dataset[var].dtype.kind, string_transform:
            case "O", "str_to_char":
                encoding[var].update(dict(dtype="S"))
            case "S" | "O", "char_to_str":
                encoding[var].update(dict(dtype="str", compression=None))
            case _, _:
                continue
    return encoding


def get_encoding_with_compression_xarray(
    dataset: xarray.Dataset, string_transform: StringTransform = None
) -> dict[str | Hashable, dict]:
    encoding: dict[str | Hashable, dict] = dict()
    # Set compresison for the observations table
    for var in list(dataset.data_vars) + list(dataset.coords):
        encoding.update({var: dict(zlib=True, complevel=1)})
        match dataset[var].dtype.kind, string_transform:
            case "O", "str_to_char":
                encoding[var].update(dict(dtype="S"))
            case "S" | "O", "char_to_str":
                encoding[var].update(dict(dtype="str", zlib=False))
            case _, _:
                continue
    return encoding
