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


def between(index, start, end):
    return (index >= start) & (index < end)
