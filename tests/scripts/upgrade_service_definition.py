import copy
import json
from importlib.resources import files
from pathlib import Path

import yaml

""" Convert old json examples into new service definition format (v1) """

# Variables to rename respect the original service definition jsons

VARS2RENAME_NDACC_COMMON = dict(
    error_o3_vertical_column_density_du="total_ozone_column_total_uncertainty",
    error_o3_vertical_column_density_mol="total_ozone_column_mol_total_uncertainty",
    o3_air_mass_factor="total_ozone_column_air_mass_factor",
    o3_vertical_column_density_du="total_ozone_column",
    o3_vertical_column_density_mol="total_ozone_column_mol",
    license_type="license_type",
    idstation="primary_station_id",
)

VARS2RENAME = dict(
    Brewer_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|header_table",
        lon="longitude|header_table",
    ),
    Dobson_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|header_table",
        lon="longitude|header_table",
    ),
    OzoneSonde_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|header_table",
        lon="longitude|header_table",
    ),
    CH4=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
    ),
    CO=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
    ),
    Ftir_profile_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
        o3_column_absorption_solar="total_ozone_column",
    ),
    Mwr_profile_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
        o3_column_absorption_solar="total_ozone_column",
    ),
    Uvvis_profile_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
        o3_column_absorption_solar="total_ozone_column",
    ),
    Lidar_profile_O3=dict(
        **VARS2RENAME_NDACC_COMMON,
        lat="latitude|observation_table",
        lon="longitude|observation_table",
        latitude_instrument="latitude|header_table",
        longitude_instrument="longitude|header_table",
        o3_column_absorption_solar="total_ozone_column",
    ),
)

VARS2ADD_UNITS = dict(Brewer_O3=dict(total_ozone_column_air_mass_factor="1"))

GROUPS2RENAME = dict(
    Brewer_O3=dict(error="total_uncertainty"),
    Dobson_O3=dict(error="total_uncertainty"),
    OzoneSonde_O3=dict(error="total_uncertainty"),
    CH4=dict(error="total_uncertainty"),
    CO=dict(error="total_uncertainty"),
    Ftir_profile_O3=dict(error="total_uncertainty"),
    Mwr_profile_O3=dict(error="total_uncertainty"),
    Uvvis_profile_O3=dict(error="total_uncertainty"),
    Lidar_profile_O3=dict(combined_uncertainty="total_uncertainty"),
)

GROUPS2REMOVE = dict(
    Lidar_profile_O3=["originator_uncertainty"], Uvvis_profile_O3="Uvvis_profile_O3"
)

ADD2MAIN_VARIABLES = dict(
    Brewer_O3=["total_ozone_column_air_mass_factor"],
)

SPACE_COLUMNS2RENAME = dict(
    location_latitude="latitude|header_table",
    location_longitude="longitude|header_table",
)
ndacc_cords = ["latitude|header_table", "longitude|header_table"]
ADD_TO_HEADER_COLUMNS = dict(
    Brewer_O3=ndacc_cords,
    Dobson_O3=ndacc_cords,
    OzoneSonde_O3=ndacc_cords,
    CH4=ndacc_cords,
    CO=ndacc_cords,
    Ftir_profile_O3=ndacc_cords,
    Mwr_profile_O3=ndacc_cords,
    Uvvis_profile_O3=ndacc_cords,
    Lidar_profile_O3=ndacc_cords,
)


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
        new_descriptions = get_new_descriptions(rename, source, sourcevals)
        # Rename groups
        if source in GROUPS2RENAME:
            new_products = rename_groups(new_data, source)
            new_data["sources"][source]["products"] = new_products
        # Add new main variables section
        variables = get_source_variables(sourcevals, rename)
        if source in ADD2MAIN_VARIABLES:
            variables.extend(ADD2MAIN_VARIABLES[source])
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
            new_header_columns = [list(hc)[0] for hc in header_columns]
            new_header_columns = [
                rename_if_needed(hc, rename) for hc in new_header_columns
            ]
            if source in ADD_TO_HEADER_COLUMNS:
                new_header_columns.extend(ADD_TO_HEADER_COLUMNS[source])
            if source == "Dobson_O3":
                new_header_columns.remove("report_timestamp")
            new_data["sources"][source]["header_columns"] = new_header_columns
        # Handle melt columns
        new_melt_columns = handle_uncertainty_flags_and_level(
            new_data, source, variables
        )
        new_data["sources"][source]["cdm_mapping"]["melt_columns"] = new_melt_columns
        # Remove keys we do not want
        del new_data["sources"][source]["products"]
        del new_data["sources"][source]["order_by"]
        if "array_columns" in new_data["sources"][source]:
            del new_data["sources"][source]["array_columns"]

    # Fix coords name
    new_data["space_columns"] = dict(
        y="latitude|header_table", x="longitude|header_table"
    )
    # Delete legacy sections
    del new_data["out_columns_order"]
    del new_data["products_hierarchy"]
    # Dump to YAML
    output_path = Path(Path(old_path).parent, Path(old_path).stem + "_new.yml")
    with output_path.open("w") as op:
        op.write(yaml.dump(new_data))


def rename_groups(new_data, source):
    new_products = []
    for oldgroup in new_data["sources"][source]["products"]:
        old_group_name = oldgroup["group_name"]
        old_group_columns = oldgroup["columns"]
        if old_group_name in GROUPS2RENAME[source]:
            new_group = dict(
                group_name=GROUPS2RENAME[source][old_group_name],
                columns=old_group_columns,
            )
        else:
            new_group = oldgroup
        new_products.append(new_group)
    return new_products


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


def get_new_descriptions(rename, source, sourcevals):
    descriptions = sourcevals["descriptions"]
    vars_str = [
        "station_name",
        "primary_station_id",
        "license",
        "license_type",
        "funding",
    ]
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
        "apriori_contribution",
        "random_covariance",
        "systematic_covariance",
        "originator_uncertainty",
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

        if source in VARS2ADD_UNITS and cdm_name in VARS2ADD_UNITS[source]:
            new_values["units"] = VARS2ADD_UNITS[source][cdm_name]

        if source in GROUPS2RENAME:
            for v in values:
                if v in GROUPS2RENAME[source]:
                    new_values[GROUPS2RENAME[source][v]] = new_values.pop(v)

        new_descriptions[cdm_name] = new_values
        for attrname in values:
            if attrname in attrs_to_remove:
                new_values.pop(attrname)

    return new_descriptions


def get_renamed_varname(
    original_name: str, original_descriptions: dict, source: str
) -> str:
    vars2rename_source = VARS2RENAME[source]
    if original_name in vars2rename_source:
        return vars2rename_source[original_name]
    else:
        return original_descriptions["name_for_output"]


def get_cdm_mapping(new_data, old_path, source, sourcevals):
    cdm_mapping = dict()
    cdm_mapping["rename"] = {
        k.lower(): get_renamed_varname(k, v, source)
        for k, v in sourcevals["descriptions"].items()
    }
    rename_dict = cdm_mapping["rename"]
    # Use station name as ID if no primary_station_id available
    if "primary_station_id" not in rename_dict.copy().values():
        station_name_key = [k for k in rename_dict if rename_dict[k] == "station_name"][
            0
        ]
        rename_dict[station_name_key] = "primary_station_id"

    cdm_mapping["melt_columns"] = "CUON" not in str(old_path)
    cdm_mapping["unit_changes"] = {}
    new_data["sources"][source]["cdm_mapping"] = cdm_mapping
    return cdm_mapping


if __name__ == "__main__":
    input_sd_files = "insitu-observations-ndacc/service_definition.json"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
