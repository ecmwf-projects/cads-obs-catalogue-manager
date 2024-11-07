import copy
import json
from importlib.resources import files
from pathlib import Path

import yaml

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
        # Handle melt columns
        new_melt_columns = handle_uncertainty_flags_and_level(
            new_data, source, variables
        )
        new_data["sources"][source]["cdm_mapping"]["melt_columns"] = new_melt_columns
        # Remove keys we do not want
        del new_data["sources"][source]["products"]
        if "array_columns" in new_data["sources"][source]:
            del new_data["sources"][source]["array_columns"]

    # Delete legacy sections
    del new_data["out_columns_order"]
    del new_data["products_hierarchy"]
    # Dump to YAML
    output_path = Path(Path(old_path).parent, Path(old_path).stem + "_new.yml")
    with output_path.open("w") as op:
        op.write(yaml.dump(new_data))


def rename_if_needed(raw_unc_name, rename):
    try:
        unc_col_name = rename[raw_unc_name]
    except KeyError:
        unc_col_name = raw_unc_name
    return unc_col_name


def handle_uncertainty_flags_and_level(new_data, source, variables):
    new_sourcevals = new_data["sources"][source]
    new_cdm_mapping = new_sourcevals["cdm_mapping"]
    rename = new_cdm_mapping["rename"]
    products = [p["group_name"] for p in new_sourcevals["products"]]
    new_melt_columns = {}
    new_cdm_mapping["melt_columns"] = new_melt_columns
    if any(["uncertainty" in p for p in products]):
        uncertainty = dict()
        for main_variable in variables:
            main_var_description = new_data["sources"][source]["descriptions"][
                main_variable
            ]
            for val in main_var_description.copy():
                if "uncertainty" in val:
                    raw_unc_name = main_var_description[val]
                    unc_col_name = rename_if_needed(raw_unc_name, rename)
                    unc_col_units = new_data["sources"][source]["descriptions"][
                        unc_col_name
                    ]["units"]
                    if val not in uncertainty:
                        uncertainty[val] = []
                    uncertainty[val].append(
                        dict(
                            name=unc_col_name,
                            main_variable=main_variable,
                            units=unc_col_units,
                        )
                    )
                    del new_sourcevals["descriptions"][unc_col_name]
                    del new_sourcevals["descriptions"][main_variable][val]
        new_melt_columns["uncertainty"] = uncertainty

    if any(["flag" in p for p in products]):
        quality_flag = dict()
        for main_variable in variables:
            main_var_description = new_data["sources"][source]["descriptions"][
                main_variable
            ]
            for val in main_var_description.copy():
                if "quality_flag" in val:
                    raw_flag_name = main_var_description[val]
                    flag_col_name = rename_if_needed(raw_flag_name, rename)
                    if val not in quality_flag:
                        quality_flag[val] = []
                    quality_flag[val].append(
                        dict(name=flag_col_name, main_variable=main_variable)
                    )
                    del new_sourcevals["descriptions"][raw_flag_name]
                    del new_sourcevals["descriptions"][main_variable][val]

        new_melt_columns["quality_flag"] = quality_flag

    if any(["processing_level" in p for p in products]):
        processing_level = dict()
        for main_variable in variables:
            main_var_description = new_data["sources"][source]["descriptions"][
                main_variable
            ]
            for val in main_var_description.copy():
                if "processing_level" in val:
                    raw_pl_name = main_var_description[val]
                    pl_col_name = rename_if_needed(raw_pl_name, rename)
                    if val not in processing_level:
                        processing_level[val] = []
                    processing_level[val].append(
                        dict(name=pl_col_name, main_variable=main_variable)
                    )
                    del new_sourcevals["descriptions"][val]
                    del new_sourcevals["descriptions"][main_variable][val]

        new_melt_columns["processing_level"] = processing_level
    return new_melt_columns


def get_source_variables(source_definition, rename) -> list[str]:
    """Read the variables from the service definition file."""
    raw_variables = [
        g for g in source_definition["products"] if g["group_name"] == "variables"
    ][0]["columns"]
    variables_renamed = [rename[v] if v in rename else v for v in raw_variables]
    return variables_renamed


def get_new_descriptions(rename, sourcevals):
    descriptions = sourcevals["descriptions"]
    vars_str = ["station_name", "primary_station_id", "license"]
    vars_datetime = [
        "report_timestamp",
        "report_timestamp_middle",
        "record_timestamp",
        "date_time",
    ]
    attrs_to_remove = [
        "name_for_output",
        "long_name",
        "valid_max",
        "valid_min",
        "output_attributes",
        "averaging_kernel",
        "apriori",
        "random_covariance",
        "systematic_covariance",
        "",
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
    rename_dict = cdm_mapping["rename"]
    # Use station name as ID if no primary_station_id available
    if "primary_station_id" not in rename_dict.copy().values():
        station_name_key = [k for k in rename_dict if rename_dict[k] == "station_name"][
            0
        ]
        rename_dict[station_name_key] = "primary_station_id"
        new_data["sources"][source]["header_columns"] = "primary_station_id"

    cdm_mapping["melt_columns"] = "CUON" not in str(old_path)
    cdm_mapping["unit_changes"] = {}
    new_data["sources"][source]["cdm_mapping"] = cdm_mapping
    return cdm_mapping


if __name__ == "__main__":
    input_sd_files = "insitu-observations-ndacc/service_definition.json"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
