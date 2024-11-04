global_attributes_names = [
    "comments",
    "description",
    "history",
    "product_code",
    "product_level",
    "product_name",
    "product_references",
    "product_version",
    "traceability",
]
variable_names = [
    "observation_id",
    "observed_variable",
    "units",
    "actual_time",
    "agency",
    "observation_value",
    "city",
    "country",
    "height_of_station_above_sea_level",
    "latitude",
    "longitude",
    "z_coordinate",
    "data_policy_licence",
    "platform_type",
    "primary_station_id",
    "qc_method",
    "quality_flag",
    "report_id",
    "report_timestamp",
    "report_meaning_of_time_stamp",
    "report_duration",
    "report_type",
    "sensor_id",
    "source_id",
    "station_name",
    "z_coordinate_type",
]
optional_variable_names = [
    "source_id",
    "product_citation",
    "data_policy_licence",
    "homogenisation_adjustment",
    "homogenisation_method",
    "uncertainty_type",
    "uncertainty_value",
    "uncertainty_units",
    "number_of_observations",
    "secondary_id",
    "sensor_model",
    "station_type",
    "platform_type",
    "report_type",
    "station_automation",
    "profile_id",
    "z_coordinate",
    "spatial_representativeness",
    "exposure_of_sensor",
    "fg_depar@body",
    "an_depar@body",
    "fg_depar@offline",
    "record_timestamp",
    "desroziers_30_uncertainty",  # Temporally added for CUON
]
# Add uncetainty numbered vars programatycally, as they are to many to add by hand to
# the list
number_of_uncertainty_types = 17
uncertainty_numbered_vars = [
    f"{unc_var}{n}"
    for n in range(number_of_uncertainty_types + 1)
    for unc_var in ["uncertainty_value", "uncertainty_type", "uncertainty_units"]
]
optional_variable_names += uncertainty_numbered_vars
auxiliary_variable_names = [
    "total_uncertainty",
    "positive_total_uncertainty",
    "negative_total_uncertainty",
    "random_uncertainty",
    "positive_random_uncertainty",
    "negative_random_uncertainty",
    "systematic_uncertainty",
    "positive_systematic_uncertainty",
    "negative_systematic_uncertainty",
    "quasisystematic_uncertainty",
    "positive_quasisystematic_uncertainty",
    "negative_quasisystematic_uncertainty",
    "quality_flag",
    "combined_uncertainty",
    "processing_level",
    "desroziers_30_uncertainty",
    "RISE_bias_estimate",
    "humidity_bias_estimate",
    "wind_bias_estimate",
]
cdm_lite_variables = dict(
    mandatory=variable_names,
    optional=optional_variable_names,
    auxiliary=auxiliary_variable_names,
)
