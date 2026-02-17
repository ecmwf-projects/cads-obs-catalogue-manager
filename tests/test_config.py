from pprint import pprint

from cdsobs.service_definition.api import get_service_definition


def test_config(test_config):
    pprint(test_config)
    dataset_name = (
        "insitu-observations-near-surface-temperature-us-climate-reference-network"
    )
    service_definition = get_service_definition(test_config, dataset_name)
    lon_tile_size = service_definition.get_tile_size("lon", "uscrn_monthly", 2001)
    assert lon_tile_size == 90
    lat_tile_size = service_definition.get_tile_size("lat", "uscrn_daily", 2010)
    assert lat_tile_size == 90
    lat_tile_size = service_definition.get_tile_size("lat", "uscrn_subhourly", 2006)
    assert lat_tile_size == 30
    lat_tile_size = service_definition.get_tile_size("lat", "uscrn_subhourly", 2015)
    assert lat_tile_size == 20

    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    service_definition = get_service_definition(test_config, dataset_name)
    disabled_fields_config = service_definition.disabled_fields
    source = "CUON"
    if isinstance(disabled_fields_config, dict):
        disabled_fields = disabled_fields_config.get(source, [])
    else:
        disabled_fields = disabled_fields_config

    expected_fields = ["report_type", "report_duration", "station_type", "secondary_id"]
    assert disabled_fields == expected_fields
