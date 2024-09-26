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
    "uncertainty_type1",
    "uncertainty_value1",
    "uncertainty_units1",
    "uncertainty_type2",
    "uncertainty_value2",
    "uncertainty_units2",
    "uncertainty_type3",
    "uncertainty_value3",
    "uncertainty_units3",
    "uncertainty_type4",
    "uncertainty_value4",
    "uncertainty_units4",
    "uncertainty_type5",
    "uncertainty_value5",
    "uncertainty_units5",
]
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
