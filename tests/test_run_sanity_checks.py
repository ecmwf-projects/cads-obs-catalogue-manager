import pytest

from cdsobs.constants import TEST_YEARS
from cdsobs.sanity_checks import run_sanity_checks


@pytest.mark.skip("Depends on cdsapi")
def test_run_sanity_checks(test_config, test_repository):
    run_sanity_checks(
        test_config,
        datasets_to_check=["insitu-observations-woudc-ozone-total-column-and-profiles"],
        years_to_check=TEST_YEARS,
        test=True,
    )
