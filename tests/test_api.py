import os
from pathlib import Path

import pytest
import sqlalchemy as sa

from cdsobs.api import run_ingestion_pipeline, run_make_cdm
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.utils.logutils import get_logger
from tests.conftest import TEST_API_PARAMETERS
from tests.utils import get_test_years

logger = get_logger(__name__)


@pytest.mark.parametrize("dataset_name,source", TEST_API_PARAMETERS)
def test_run_ingestion_pipeline(
    dataset_name, source, test_session, test_config, caplog, tmp_path
):
    start_year, end_year = get_test_years(source)
    os.environ["CADSOBS_AVOID_MULTIPROCESS"] = "0"
    run_ingestion_pipeline(
        dataset_name,
        source,
        test_session,
        test_config,
        start_year=start_year,
        end_year=end_year,
        update=False,
    )
    # assert insertions have been made
    counter = test_session.scalar(
        sa.select(sa.func.count())
        .select_from(Catalogue)
        .where(Catalogue.dataset == dataset_name)
    )
    assert counter > 0
    if dataset_name == "insitu-comprehensive-upper-air-observation-network":
        stations = test_session.scalar(
            sa.select(Catalogue.stations).where(Catalogue.dataset == dataset_name)
        )
        assert stations == ["0-20001-0-53772", "0-20001-0-53845"]


def test_make_cdm(test_config, tmp_path, caplog):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    start_year, end_year = get_test_years(source)
    run_make_cdm(
        dataset_name,
        source,
        test_config,
        start_year=start_year,
        end_year=end_year,
        output_dir=Path(tmp_path),
        save_data=True,
    )
    output_file = Path(
        tmp_path,
        "insitu-observations-woudc-ozone-total-column-and-profiles_OzoneSonde_1969_0.0_0.0.nc",
    )
    assert output_file.exists()
    assert any([f"Saved partition to {output_file}" in r.msg for r in caplog.records])
