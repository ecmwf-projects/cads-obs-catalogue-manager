import copy
import json
from importlib.resources import files
from pathlib import Path

import yaml

""" Convert old json examples into new service definition format (v1) """


def get_source_variables(source_definition) -> list[str]:
    """Read the variables from the service definition file."""
    variable_products = list(
        filter(lambda p: p["group_name"] == "variables", source_definition["products"])
    )
    variable_keys = set()
    for p in variable_products:
        variable_keys.update(p["columns"])
    variables = []
    for key in variable_keys:
        values = source_definition["descriptions"][key]
        values["name_for_output"] = values["name_for_output"].lower()
        variables.append(values)
    return variables


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
        cdm_mapping = dict()
        cdm_mapping["rename"] = {
            k.lower(): v["name_for_output"]
            for k, v in sourcevals["descriptions"].items()
        }
        cdm_mapping["melt_columns"] = "CUON" not in str(old_path)
        variables = get_source_variables(sourcevals)
        cdm_mapping["unit_changes"] = {}
        for var in variables:
            cdm_mapping["unit_changes"][var["name_for_output"]] = {
                "names": {var["units"]: var["units"]},
                "scale": 1,
                "offset": 0,
            }
        new_data["sources"][source]["cdm_mapping"] = cdm_mapping
        # Fix descriptions, remove name_for_output and rename the keys to the
        # CDM variable names
        rename = cdm_mapping["rename"]
        descriptions = sourcevals["descriptions"]
        descriptions = {k.lower(): v for k, v in descriptions.items()}
        new_descriptions = {}
        for rawname, values in descriptions.items():
            try:
                cdm_name = rename[rawname]
            except KeyError:
                pass
            new_values = values.copy()
            if "output_attributes" in new_values:
                oattrs = new_values.pop("output_attributes")
                new_values.update(oattrs)
            new_values["dtype"] = "float32"
            new_values["long_name"] = rawname
            new_descriptions[cdm_name] = new_values
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
    # Dump to YAML
    output_path = Path(str(old_path).replace(".json", ".yml"))
    with output_path.open("w") as op:
        op.write(yaml.dump(new_data))


if __name__ == "__main__":
    input_sd_files = "insitu-observations-igra-baseline-network/service_definition.json"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
