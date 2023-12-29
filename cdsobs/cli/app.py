import os

import typer

from cdsobs.cli._catalogue_explorer import (
    catalogue_dataset_info,
    list_catalogue,
    list_datasets,
)
from cdsobs.cli._copy_dataset import copy_dataset
from cdsobs.cli._delete_dataset import delete_dataset
from cdsobs.cli._get_forms_jsons import get_forms_jsons_command
from cdsobs.cli._make_cdm import make_cdm
from cdsobs.cli._make_production import make_production
from cdsobs.cli._object_storage import check_consistency
from cdsobs.cli._retrieve import retrieve
from cdsobs.cli._utils import exception_handler
from cdsobs.cli._validate import validate_service_definition_json

app = typer.Typer(
    help="Copernicus Climate & Atmoshpere Data Store Observation Manager"
    " Command Line Interface",
)

validate_service_definition_json = app.command()(validate_service_definition_json)
make_production = app.command()(make_production)
retrieve = app.command()(retrieve)
list_catalogue = app.command()(list_catalogue)
catalogue_dataset_info = app.command()(catalogue_dataset_info)
list_datasets = app.command()(list_datasets)
check_consistency = app.command()(check_consistency)
copy_dataset = app.command()(copy_dataset)
delete_dataset = app.command()(delete_dataset)
make_cdm = app.command()(make_cdm)
get_forms_jsons = app.command("get_forms_jsons")(get_forms_jsons_command)


def main():
    debug_mode = os.getenv("CLI_DEBUG", False)
    try:
        app()
    except Exception as e:
        if debug_mode:
            raise e
        else:
            exception_handler(e)
