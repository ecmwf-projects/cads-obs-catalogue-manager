import os
from pathlib import Path

from cdsobs.config import read_and_validate_config
from cdsobs.constants import TEST_DATASETS, TEST_YEARS
from cdsobs.sanity_checks import run_sanity_checks
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def main():
    cdsobs_config_yml = Path(os.environ.get("CDSOBS_CONFIG"))
    config = read_and_validate_config(cdsobs_config_yml)
    run_sanity_checks(
        config, datasets_to_check=TEST_DATASETS, years_to_check=TEST_YEARS
    )


if __name__ == "__main__":
    main()
