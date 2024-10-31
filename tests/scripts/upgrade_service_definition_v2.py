import copy
from importlib.resources import files
from pathlib import Path
from pprint import pprint

import yaml

from cdsobs.cdm.lite import auxiliary_variable_names

""" Convert old json examples into new service definition format (v1) """


def get_source_variables(source_definition, rename) -> list[str]:
    """Read the variables from the service definition file."""
    raw_variables = [
        g for g in source_definition["products"] if g["group_name"] == "variables"
    ][0]["columns"]
    variables_renamed = [rename[v] if v in rename else v for v in raw_variables]
    return variables_renamed


def main(old_path):
    with old_path.open("r") as op:
        old_data = yaml.safe_load(op)
    new_data = copy.deepcopy(old_data)
    del new_data["out_columns_order"]
    del new_data["products_hierarchy"]
    # Add CDM mapping
    new_sources = new_data["sources"]
    for source, new_sourcevals in new_sources.items():
        new_cdm_mapping = new_sourcevals["cdm_mapping"]
        rename = new_cdm_mapping["rename"]
        # Add main variables
        variables = get_source_variables(new_sourcevals, rename)
        new_data["sources"][source]["main_variables"] = variables
        # Handle cdm mapping

        if source != "CUON":
            products = [p["group_name"] for p in new_sourcevals["products"]]
            new_melt_columns = {}
            new_cdm_mapping["melt_columns"] = new_melt_columns
            if any(["uncertainty" in p for p in products]):
                uncertainty = dict()
                for main_variable in variables:
                    main_var_description = new_data["sources"][source]["descriptions"][
                        main_variable
                    ]
                    for val in main_var_description:
                        if "uncertainty" in val:
                            raw_unc_name = main_var_description[val]
                            try:
                                unc_col_name = rename[raw_unc_name]
                            except KeyError:
                                unc_col_name = raw_unc_name
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
                new_melt_columns["uncertainty"] = uncertainty

            if any(["flag" in p for p in products]):
                quality_flag = dict()
                for main_variable in variables:
                    main_var_description = new_data["sources"][source]["descriptions"][
                        main_variable
                    ]
                    for val in main_var_description:
                        if "quality_flag" in val:
                            raw_flag_name = main_var_description[val]
                            try:
                                flag_col_name = rename[raw_flag_name]
                            except KeyError:
                                flag_col_name = raw_flag_name
                            if val not in quality_flag:
                                quality_flag[val] = []
                            quality_flag[val].append(
                                dict(name=flag_col_name, main_variable=main_variable)
                            )

                new_melt_columns["quality_flag"] = quality_flag

            if any(["processing_level" in p for p in products]):
                processing_level = dict()
                for main_variable in variables:
                    main_var_description = new_data["sources"][source]["descriptions"][
                        main_variable
                    ]
                    for val in main_var_description:
                        if "processing_level" in val:
                            raw_pl_name = main_var_description[val]
                            try:
                                pl_col_name = rename[raw_pl_name]
                            except KeyError:
                                pl_col_name = raw_pl_name
                            if val not in processing_level:
                                processing_level[val] = []
                            processing_level[val].append(
                                dict(name=pl_col_name, main_variable=main_variable)
                            )

                new_melt_columns["processing_level"] = processing_level
        # Fix descriptions, remove name_for_output and rename the keys to the
        # CDM variable names
        new_descriptions = new_sourcevals["descriptions"]

        for name, new_values in new_descriptions.items():
            new_descriptions[name] = new_values
            for val_to_del in ["long_name"] + auxiliary_variable_names:
                if val_to_del in new_values:
                    del new_values[val_to_del]
        names_to_del = []
        for name in new_descriptions:
            if any([auxvar in name for auxvar in auxiliary_variable_names]):
                names_to_del.append(name)
        new_descriptions = {
            k: v for k, v in new_descriptions.items() if k not in names_to_del
        }
        new_data["sources"][source]["descriptions"] = new_descriptions
        del new_sourcevals["products"]
        del new_sourcevals["order_by"]

    # Dump to YAML
    output_path = Path(old_path.parent, old_path.stem + "_new").with_suffix(".yml")
    print(f"Would save to {output_path}")
    pprint(new_data)
    with output_path.open("w") as op:
        op.write(yaml.dump(new_data))


if __name__ == "__main__":
    input_sd_files = "insitu-observations-ndacc/service_definition.yml"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
