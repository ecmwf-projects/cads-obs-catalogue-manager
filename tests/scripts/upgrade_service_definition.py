import copy
import json
from dataclasses import dataclass
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
        latitude="latitude|observation_table",
        longitude="longitude|observation_table",
        lat="latitude|header_table",
        lon="longitude|header_table",
        o3_column_stratospheric_scatter_solar_zenith="total_ozone_column",
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


RAWVARS2RENAME = dict(
    Uvvis_profile_O3=dict(
        o3_column_stratospheric_scatter_solar_zenith_uncertainty_random_standard="o3_column_stratospheric_scatter_solar_zenith_uncertainty_random",
        o3_column_stratospheric_scatter_solar_zenith_uncertainty_systematic_standard="o3_column_stratospheric_scatter_solar_zenith_uncertainty_system",
    )
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

rename_location_coords = dict(
    location_latitude="latitude|header_table",
    location_longitude="longitude|header_table",
)
SPACE_COLUMNS2RENAME = dict(
    Brewer_O3=rename_location_coords,
    Dobson_O3=rename_location_coords,
    OzoneSonde_O3=rename_location_coords,
    CH4=rename_location_coords,
    CO=rename_location_coords,
    Ftir_profile_O3=rename_location_coords,
    Mwr_profile_O3=rename_location_coords,
    Uvvis_profile_O3=rename_location_coords,
    Lidar_profile_O3=rename_location_coords,
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


@dataclass
class SourceFixerMappings:
    """Mappings for fixing problems in original service definition JSONS."""

    vars2rename: dict[str, str] | None
    rawvars2rename: dict[str, str] | None
    vars2add_units: dict[str, str] | None
    groups2rename: dict[str, str] | None
    add2main_variables: dict[str, str] | None
    space_columns2rename: dict[str, str] | None
    add2header_columns: dict[str, str] | None
    melt_columns: bool = False


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
        fixer_mappings = SourceFixerMappings(
            vars2rename=VARS2RENAME.get(source),
            rawvars2rename=RAWVARS2RENAME.get(source),
            vars2add_units=VARS2ADD_UNITS.get(source),
            groups2rename=GROUPS2RENAME.get(source),
            add2main_variables=ADD2MAIN_VARIABLES.get(source),
            space_columns2rename=SPACE_COLUMNS2RENAME.get(source),
            add2header_columns=ADD_TO_HEADER_COLUMNS.get(source),
            melt_columns="CUON" not in str(old_path),
        )
        # Fix raw names in descriptions
        new_descriptions = new_data["sources"][source]["descriptions"]
        if fixer_mappings.rawvars2rename is not None:
            rawvars2rename = fixer_mappings.rawvars2rename
            for k, v in new_descriptions.copy().items():
                if k in rawvars2rename:
                    renamed_field = rawvars2rename[k]
                    field_desc = new_descriptions.pop(k)
                    new_descriptions[renamed_field] = field_desc
        # Get cdm mapping section
        cdm_mapping = get_cdm_mapping(new_descriptions, fixer_mappings)
        new_data["sources"][source]["cdm_mapping"] = cdm_mapping
        # Fix descriptions, remove name_for_output and rename the keys to the
        # CDM variable names
        rename = cdm_mapping["rename"]
        new_descriptions_fixed = fix_new_descriptions(
            rename, new_descriptions, fixer_mappings
        )
        new_data["sources"][source]["descriptions"] = new_descriptions_fixed
        # Rename groups
        if source in GROUPS2RENAME or source in RAWVARS2RENAME:
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
        if source in RAWVARS2RENAME:
            for field in new_group["columns"].copy():
                if field in RAWVARS2RENAME[source]:
                    new_group["columns"].remove(field)
                    new_field = RAWVARS2RENAME[source][field]
                    new_group["columns"].append(new_field)
        new_products.append(new_group)
    return new_products


def rename_if_needed(raw_name: str, rename: dict) -> str:
    try:
        unc_col_name = rename[raw_name]
    except KeyError:
        unc_col_name = raw_name
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


def fix_new_descriptions(
    rename: dict[str, str],
    new_descriptions: dict[str, dict],
    fixer_mappings: SourceFixerMappings,
) -> dict[str, dict]:
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
    new_descriptions = {k.lower(): v for k, v in new_descriptions.items()}
    for rawname, desc_mapping in new_descriptions.copy().items():
        cdm_name = rename_if_needed(rawname, rename)
        new_desc_mapping = desc_mapping.copy()
        dtype = get_dtype(cdm_name)
        new_desc_mapping["dtype"] = dtype
        vars2add_units = fixer_mappings.vars2add_units
        if vars2add_units is not None and cdm_name in vars2add_units:
            new_desc_mapping["units"] = vars2add_units[cdm_name]
        groups2rename = fixer_mappings.groups2rename
        if groups2rename is not None:
            for key, val in desc_mapping.items():
                if key in groups2rename:
                    new_desc_mapping[groups2rename[key]] = new_desc_mapping.pop(key)

        new_descriptions[cdm_name] = new_desc_mapping
        if cdm_name != rawname:
            del new_descriptions[rawname]
        for attrname in desc_mapping:
            if attrname in attrs_to_remove:
                new_desc_mapping.pop(attrname)
        rawvars2rename = fixer_mappings.rawvars2rename
        if rawvars2rename is not None:
            for key, val in new_desc_mapping.copy().items():
                if new_desc_mapping[key] in rawvars2rename:
                    new_desc_mapping[key] = rawvars2rename[val]

    return new_descriptions


def get_dtype(cdm_name) -> str:
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
    if cdm_name in vars_str:
        dtype = "object"
    elif cdm_name in vars_datetime:
        dtype = "datetime64[ns]"
    else:
        dtype = "float32"
    return dtype


def get_renamed_varname(
    original_name: str, original_descriptions: dict, fixer_mappings: SourceFixerMappings
) -> str:
    vars2rename = fixer_mappings.vars2rename
    if original_name in vars2rename:
        return vars2rename[original_name]
    else:
        return original_descriptions["name_for_output"]


def get_cdm_mapping(new_descriptions: dict[str, dict], fixer_mappings) -> dict:
    cdm_mapping = dict()
    cdm_mapping["rename"] = {
        k.lower(): get_renamed_varname(k, v, fixer_mappings)
        for k, v in new_descriptions.items()
    }
    rename_dict = cdm_mapping["rename"]
    # Use station name as ID if no primary_station_id available
    if "primary_station_id" not in rename_dict.copy().values():
        station_name_key = [k for k in rename_dict if rename_dict[k] == "station_name"][
            0
        ]
        rename_dict[station_name_key] = "primary_station_id"
    cdm_mapping["unit_changes"] = {}
    return cdm_mapping


if __name__ == "__main__":
    input_sd_files = "insitu-observations-ndacc/service_definition.json"
    for file in files("cdsobs").joinpath("data").glob(input_sd_files):  # type: ignore
        print(file)
        main(file)
