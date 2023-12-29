from typing import Dict, List

# Below the is a CDM type mapping according to page 15 of the CDM specification
# • numeric Any numeric value (integer or floating point).
# • int An integer value.
# • varchar A variable length character string.
# • timestamp A timestamp with time zone, e.g. "2017-07-01 00:00:0.0+00".
# • [] An array of the indicated type.
# • * An optional element.
# • (pk) The indicated elements marked as (pk) within a table form the unique ID for the
# record.
cdm_dtypes2numpy: Dict[str, str | List[str]] = {
    "timestamp": "datetime64[ns]",
    "timestamp with timezone": [
        "datetime64[ns]",
        "datetime64[ns, UTC]",
    ],  # Other zones to be added if neccesary
    "varchar": "object",
    "int": ["int16", "int32", "int64"],
    "numeric": [
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
        "Sparse[float32, nan]",
        "Sparse[float64, nan]",
    ],
    "int[]": "object",
    "varchar[]": "object",
    "bool": "bool",
}
