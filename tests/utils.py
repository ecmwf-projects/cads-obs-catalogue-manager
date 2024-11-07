from typing import Tuple


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
            start_year = 1960
            end_year = 1961
        case "GRUAN":
            start_year = 2010
            end_year = 2011
        case "uscrn_subhourly":
            start_year = 2007
            end_year = 2007
        case "uscrn_hourly":
            start_year = 2006
            end_year = 2006
        case "uscrn_daily":
            start_year = 2007
            end_year = 2007
        case "uscrn_monthly":
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
        case "Brewer_O3":
            start_year = 2014
            end_year = 2014
        case "CH4":
            start_year = 2008
            end_year = 2008
        case "CO":
            start_year = 1995
            end_year = 1995
        case _:
            raise NotImplementedError(f"Unsupported source {source}")
    return start_year, end_year
