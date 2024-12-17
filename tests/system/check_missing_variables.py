import os
from pathlib import Path

import pandas
import pytest
import sqlalchemy as sa

from cdsobs.api import run_ingestion_pipeline
from cdsobs.cdm.api import open_netcdf
from cdsobs.cdm.lite import auxiliary_variable_names
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.service_definition.api import get_service_definition
from cdsobs.storage import S3Client
from tests.conftest import TEST_API_PARAMETERS
from tests.utils import get_test_years


@pytest.mark.parametrize("dataset_name,source", TEST_API_PARAMETERS)
def test_run_ingestion_pipeline(
    dataset_name, source, test_session, test_config, caplog, tmp_path
):
    start_year, end_year = get_test_years(source)
    service_definition = get_service_definition(test_config, dataset_name)
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
    # Check variables
    variable_check_results_file = Path("variable_check_results.csv")
    index_cols = ["dataset_name", "dataset_source"]
    if variable_check_results_file.exists():
        results = pandas.read_csv(variable_check_results_file, index_col=index_cols)
    else:
        results = pandas.DataFrame(
            columns=[
                "dataset_name",
                "dataset_source",
                "in_file_not_in_descriptions",
                "in_descriptions_not_in_file",
            ]
        ).set_index(index_cols)
    # Get the file
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
    # Get variables in file
    variables_in_file = set(
        dataset.columns.tolist() + dataset.observed_variable.unique().tolist()
    )
    # Get expected variables according to service definition file
    expected_variables = set(service_definition.sources[source].descriptions)
    # Here we add some more variables to expected variables
    for v in [
        "observed_variable",
        "observation_value",
        "units",
    ] + auxiliary_variable_names:
        if v in variables_in_file:
            expected_variables.add(v)
    in_file_not_in_descriptions = tuple(variables_in_file - expected_variables)
    in_descriptions_not_in_file = tuple(expected_variables - variables_in_file)

    results.loc[(dataset_name, source), :] = pandas.Series(
        index=("in_file_not_in_descriptions", "in_descriptions_not_in_file"),
        data=[in_file_not_in_descriptions, in_descriptions_not_in_file],
    )
    results.to_csv(variable_check_results_file)
