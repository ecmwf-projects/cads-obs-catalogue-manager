import os
from pathlib import Path

import pytest
import sqlalchemy as sa

from cdsobs.api import run_ingestion_pipeline, run_make_cdm, set_version_status
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.dataset_version import (
    CadsDatasetVersionRepository,
)
from cdsobs.utils.logutils import get_logger
from tests.conftest import TEST_API_PARAMETERS
from tests.utils import get_test_years

logger = get_logger(__name__)


@pytest.mark.parametrize("dataset_name,source", TEST_API_PARAMETERS)
def test_run_ingestion_pipeline(
    dataset_name, source, test_session_pertest, test_config, test_sds, caplog, tmp_path
):
    start_year, end_year = get_test_years(source)
    os.environ["CADSOBS_AVOID_MULTIPROCESS"] = "0"
    run_ingestion_pipeline(
        dataset_name,
        source,
        test_session_pertest,
        test_config,
        start_year=start_year,
        end_year=end_year,
        disable_cdm_tag_check=True,
        service_definition=test_sds.get(dataset_name),
    )
    # assert insertions have been made
    counter = test_session_pertest.scalar(
        sa.select(sa.func.count())
        .select_from(Catalogue)
        .where(Catalogue.dataset == dataset_name)
    )
    assert counter > 0
    # assert dataset is disabled by default
    cads_dataset_version_repo = CadsDatasetVersionRepository(test_session_pertest)
    dataset_version = cads_dataset_version_repo.get_dataset_version(
        dataset_name=dataset_name, version=DEFAULT_VERSION
    )
    assert dataset_version.deprecated
    if dataset_name == "insitu-comprehensive-upper-air-observation-network":
        stations = test_session_pertest.scalar(
            sa.select(Catalogue.stations).where(Catalogue.dataset == dataset_name)
        )
        assert stations == ["0-20001-0-53772", "0-20001-0-53845"]


def test_make_cdm(test_config, test_sds, tmp_path, caplog):
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
        disable_cdm_tag_check=True,
        service_definition=test_sds.get(dataset_name),
    )
    output_file = Path(
        tmp_path,
        "insitu-observations-woudc-ozone-total-column-and-profiles_1.0.0_OzoneSonde_1969_0.0_0.0.nc",
    )
    assert output_file.exists()
    assert any([f"Saved partition to {output_file}" in r.msg for r in caplog.records])


def test_set_version_status(test_repository):
    session = test_repository.catalogue_repository.session
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    version = DEFAULT_VERSION
    config = test_repository.config
    with pytest.raises(RuntimeError):
        set_version_status(config, "notexistent_dataset", version, deprecated=False)
    set_version_status(config, dataset_name, version, deprecated=False)
    set_version_status(config, dataset_name, version, deprecated=True)
    cads_dataset_version_repo = CadsDatasetVersionRepository(session)
    dataset_version = cads_dataset_version_repo.get_dataset_version(
        dataset_name, version
    )
    assert dataset_version.deprecated
