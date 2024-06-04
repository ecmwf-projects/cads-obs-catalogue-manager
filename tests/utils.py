from hashlib import sha256
from typing import Any, Tuple


def get_object_str_hash(pyobj: Any) -> str:
    return sha256(str(pyobj).encode()).hexdigest()


def get_test_years(source: str) -> Tuple[int, int]:
    match source:
        case "OzoneSonde":
            start_year = 1969
            end_year = 1970
        case "TotalOzone":
            start_year = 2011
            end_year = 2012
        case "IGRA":
            start_year = 2005
            end_year = 2005
        case "IGRA_H":
            start_year = 1978
            end_year = 1979
        case "CUON":
            start_year = 1957
            end_year = 1958
        case "GRUAN":
            start_year = 2010
            end_year = 2011
        case "USCRN_SUBHOURLY":
            start_year = 2007
            end_year = 2007
        case "USCRN_HOURLY":
            start_year = 2006
            end_year = 2006
        case "USCRN_DAILY":
            start_year = 2007
            end_year = 2007
        case "USCRN_MONTHLY":
            start_year = 2006
            end_year = 2006
        case "IGS":
            start_year = 2000
            end_year = 2000
        case "EPN":
            start_year = 1996
            end_year = 1996
        case "IGS_R3":
            start_year = 2000
            end_year = 2000
        case _:
            raise NotImplementedError(f"Unsupported source {source}")
    return start_year, end_year
