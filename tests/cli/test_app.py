from pathlib import Path

import pytest
from typer.testing import CliRunner

from cdsobs.cli.app import app
from cdsobs.constants import CONFIG_YML

runner = CliRunner()


@pytest.mark.skip(reason="this test does not reset db after running")
@pytest.mark.parametrize("verbose", [False])
def test_cli_make_production(verbose):
    args = [
        "make-production",
        "--dataset",
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "--config",
        CONFIG_YML,
        "--start-year",
        1969,
        "--end-year",
        1970,
        "--source",
        "OzoneSonde",
        "--version",
        "2.0.0",
    ]
    if verbose:
        args += ["--verbose"]
    result = runner.invoke(app, args, catch_exceptions=False)
    print(result.stdout)
    assert result.exit_code == 0


@pytest.mark.skip(reason="this test does not reset db after running")
def test_cli_retrieve(tmp_path, test_repository):
    runner = CliRunner()
    test_json_str = """[
    "insitu-observations-woudc-ozone-total-column-and-profiles",
    {
    "dataset_source": "OzoneSonde",
    "latitude_coverage": [0.0, 90.0],
    "longitude_coverage": [120.0, 140.0],
    "stations": ["7"],
    "time_coverage": ["1969-02-01 00:00:00", "1969-03-01 00:00:00"],
    "variables": [
        "air_temperature",
        "geopotential_height"
        ]
    }
]
    """
    test_json_path = str(Path(tmp_path, "test_retrieve_params.json"))
    with open(test_json_path, "w") as tj:
        tj.write(test_json_str)

    args = [
        "retrieve",
        "--retrieve-params",
        test_json_path,
        "--config",
        CONFIG_YML,
        "--output-dir",
        str(tmp_path),
    ]
    result = runner.invoke(
        app,
        args,
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_cli_make_cdm(tmp_path):
    args = [
        "make-cdm",
        "--dataset",
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "--config",
        CONFIG_YML,
        "--start-year",
        2001,
        "--end-year",
        2002,
        "--save-data",
        "--output-dir",
        tmp_path,
        "--source",
        "OzoneSonde",
    ]
    result = runner.invoke(app, args, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.skip(reason="this test does not reset db after running")
def test_cli_get_forms_jsons(tmp_path, test_repository):
    args = [
        "get-forms-jsons",
        "--dataset",
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "--config",
        CONFIG_YML,
        "--output-dir",
        tmp_path,
    ]
    result = runner.invoke(app, args, catch_exceptions=False)
    assert result.exit_code == 0
