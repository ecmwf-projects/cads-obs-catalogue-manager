from pprint import pprint


def test_config(test_config):
    pprint(test_config)
    dataset_name = (
        "insitu-observations-near-surface-temperature-us-climate-reference-network"
    )
    dataset_config = test_config.get_dataset(dataset_name)
    lon_tile_size = dataset_config.get_tile_size("lon", "uscrn_monthly", 2001)
    assert lon_tile_size == 90
    lat_tile_size = dataset_config.get_tile_size("lat", "uscrn_daily", 2010)
    assert lat_tile_size == 90
    lat_tile_size = dataset_config.get_tile_size("lat", "uscrn_subhourly", 2006)
    assert lat_tile_size == 30
    lat_tile_size = dataset_config.get_tile_size("lat", "uscrn_subhourly", 2015)
    assert lat_tile_size == 20
