from pathlib import Path

from cdsobs.cdm.api import read_cdm_code_table
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def get_var_code_dict(cdm_tables_location: Path) -> dict:
    code_dict = (
        read_cdm_code_table(cdm_tables_location, "observed_variable")
        .table["name"]
        .str.replace(" ", "_")
        .to_dict()
    )
    return code_dict


def get_param_name_in_data(retrieved_dataset, param_name):
    match param_name:
        case "time_coverage":
            param_name_in_data = "report_timestamp"
        case "longitude_coverage" | "latitude_coverage":
            coord = param_name.split("_")[0]
            if f"{coord}|header_table" in retrieved_dataset.variables:
                param_name_in_data = f"{coord}|header_table"
            else:
                param_name_in_data = f"{coord}|station_configuration"
        case _:
            raise RuntimeError(f"Unknown parameter name {param_name}")
    return param_name_in_data


def between(index, start, end):
    return (index >= start) & (index < end)
