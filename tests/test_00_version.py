import cdsobs


def test_version() -> None:
    assert cdsobs.__version__ != "999"
