import os
from pathlib import Path

import pytest
import sqlalchemy as sa

from cdsobs.api import run_ingestion_pipeline, run_make_cdm
from cdsobs.cdm.api import open_netcdf
from cdsobs.cdm.lite import auxiliary_variable_names
from cdsobs.ingestion.core import get_aux_vars_from_service_definition
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.service_definition.api import get_service_definition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger
from tests.utils import get_test_years

logger = get_logger(__name__)


@pytest.mark.parametrize(
    "dataset_name,source,test_update",
    [
        (
            "insitu-observations-woudc-ozone-total-column-and-profiles",
            "OzoneSonde",
            False,
        ),
        (
            "insitu-observations-woudc-ozone-total-column-and-profiles",
            "TotalOzone",
            False,
        ),
        ("insitu-observations-igra-baseline-network", "IGRA", False),
        ("insitu-observations-igra-baseline-network", "IGRA_H", False),
        ("insitu-comprehensive-upper-air-observation-network", "CUON", False),
        ("insitu-observations-gruan-reference-network", "GRUAN", False),
        (
            "insitu-observations-near-surface-temperature-us-climate-reference-network",
            "uscrn_subhourly",
            False,
        ),
        (
            "insitu-observations-near-surface-temperature-us-climate-reference-network",
            "uscrn_hourly",
            False,
        ),
        (
            "insitu-observations-near-surface-temperature-us-climate-reference-network",
            "uscrn_daily",
            False,
        ),
        (
            "insitu-observations-near-surface-temperature-us-climate-reference-network",
            "uscrn_monthly",
            False,
        ),
        (
            "insitu-observations-gnss",
            "IGS",
            False,
        ),
        (
            "insitu-observations-gnss",
            "EPN",
            False,
        ),
        (
            "insitu-observations-gnss",
            "IGS_R3",
            False,
        ),
    ],
)
def test_run_ingestion_pipeline(
    dataset_name, source, test_update, test_session, test_config, caplog, tmp_path
):
    start_year, end_year = get_test_years(source)
    service_definition = get_service_definition(dataset_name)
    os.environ["CADSOBS_AVOID_MULTIPROCESS"] = "0"
    run_ingestion_pipeline(
        dataset_name,
        service_definition,
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
    # Check variables
    asset = test_session.scalar(
        sa.select(Catalogue.asset).where(Catalogue.dataset == dataset_name)
    )
    s3client = S3Client.from_config(test_config.s3config)
    asset_filename = asset.split("/")[1]
    asset_local_path = Path(tmp_path, asset_filename)
    s3client.download_file(
        s3client.get_bucket_name(dataset_name), asset_filename, asset_local_path
    )
    dataset = open_netcdf(asset_local_path, decode_variables=True)
    variables_in_file = set(
        dataset.columns.tolist() + dataset.observed_variable.unique().tolist()
    )
    aux_variables = get_aux_vars_from_service_definition(service_definition, source)
    expected_variables = set(service_definition.sources[source].descriptions) - set(
        aux_variables
    )
    for v in [
        "observed_variable",
        "observation_value",
        "units",
    ] + auxiliary_variable_names:
        if v in variables_in_file:
            expected_variables.add(v)
    logger.info(
        f"{variables_in_file - expected_variables} are in file but not in the descriptions"
    )
    logger.info(
        f"{expected_variables - variables_in_file} are not in file but are in the description"
    )
    # assert variables_in_file == expected_variables

    if test_update:
        # testing update flag
        run_ingestion_pipeline(
            dataset_name,
            service_definition,
            source,
            test_session,
            test_config,
            start_year=start_year,
            end_year=end_year,
            update=False,
        )

        found_log = [
            "A partition with the chosen parameters already exists" in r.msg
            for r in caplog.records
        ]
        assert any(found_log)
        # no insertions have been made
        assert (
            test_session.scalar(sa.select(sa.func.count()).select_from(Catalogue))
            == counter
        )


def test_make_cdm(test_config, tmp_path, caplog):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    source = "OzoneSonde"
    service_definition = get_service_definition(dataset_name)
    start_year, end_year = get_test_years(source)
    run_make_cdm(
        dataset_name,
        service_definition,
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
