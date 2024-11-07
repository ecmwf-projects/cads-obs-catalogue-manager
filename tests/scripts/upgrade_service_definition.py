import copy
import json
from importlib.resources import files
from pathlib import Path

import yaml

from tests.scripts.upgrade_service_definition_v2 import (
    handle_uncertainty_flags_and_level,
)

""" Convert old json examples into new service definition format (v1) """


def main(old_path):
    with old_path.open("r") as op:
        old_data = json.load(op)
    new_data = copy.deepcopy(old_data)
    # Add global attributes
    new_data["global_attributes"] = {
        "responsible_organisation": "ECMWF",
        "contactemail": "https://support.ecmwf.int",
        "licence_list": "20180314_Copernicus_License_V1.1",
    }
    # Add CDM mapping
    sources = old_data["sources"]
    for source, sourcevals in sources.items():
        cdm_mapping = get_cdm_mapping(new_data, old_path, source, sourcevals)
        # Fix descriptions, remove name_for_output and rename the keys to the
        # CDM variable names
        rename = cdm_mapping["rename"]
        new_descriptions = get_new_descriptions(rename, sourcevals)
        # Add new main variables sectoin
        variables = get_source_variables(sourcevals, rename)
        new_data["sources"][source]["main_variables"] = variables
        # remap mandatory columns
        new_data["sources"][source]["mandatory_columns"] = []
        for mcol in old_data["sources"][source]["mandatory_columns"]:
            if mcol not in rename:
                rename[mcol] = mcol
            new_data["sources"][source]["mandatory_columns"].append(rename[mcol])
        new_data["sources"][source]["descriptions"] = new_descriptions
        # Fix header columns
        if "header_columns" in old_data["sources"][source]:
            header_columns = old_data["sources"][source]["header_columns"]
            new_header_columns = [list(hc.values())[0] for hc in header_columns]
            new_data["sources"][source]["header_columns"] = new_header_columns

        new_melt_columns = handle_uncertainty_flags_and_level(
            new_data, sourcevals, source, variables
        )
        new_data["sources"][source]["cdm_mapping"]["melt_columns"] = new_melt_columns
        # Delete legacy sections
    del new_data["out_columns_order"]
    del new_data["products_hierarchy"]
    # Dump to YAML
    output_path = Path(Path(old_path).parent, Path(old_path).stem + "_new.yml")
    with output_path.open("w") as op:
        op.write(yaml.dump(new_data))


def get_source_variables(source_definition, rename) -> list[str]:
    """Read the variables from the service definition file."""
    raw_variables = [
        g for g in source_definition["products"] if g["group_name"] == "variables"
    ][0]["columns"]
    variables_renamed = [rename[v] if v in rename else v for v in raw_variables]
    return variables_renamed


def get_new_descriptions(rename, sourcevals):
    descriptions = sourcevals["descriptions"]
    vars_str = ["station_name"]
    vars_datetime = ["report_timestamp"]
    attrs_to_remove = [
        "name_for_output",
        "long_name",
        "valid_max",
        "valid_min",
        "output_attributes",
    ]
    descriptions = {k.lower(): v for k, v in descriptions.items()}
    new_descriptions = {}
    for rawname, values in descriptions.items():
        if rawname in rename:
            cdm_name = rename[rawname]
        else:
            cdm_name = rawname
        new_values = values.copy()

        if cdm_name in vars_str:
            new_values["dtype"] = "object"
        elif cdm_name in vars_datetime:
            new_values["dtype"] = "datetime64[ns]"
        else:
            new_values["dtype"] = "float32"

        new_descriptions[cdm_name] = new_values
        for attrname in values:
            if attrname in attrs_to_remove:
                new_values.pop(attrname)

    return new_descriptions


def get_cdm_mapping(new_data, old_path, source, sourcevals):
    cdm_mapping = dict()
    cdm_mapping["rename"] = {
        k.lower(): v["name_for_output"] for k, v in sourcevals["descriptions"].items()
    }
    cdm_mapping["melt_columns"] = "CUON" not in str(old_path)
    cdm_mapping["unit_changes"] = {}
    new_data["sources"][source]["cdm_mapping"] = cdm_mapping
    return cdm_mapping


if __name__ == "__main__":
    input_sd_files = "insitu-observations-ndacc/service_definition.json"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
