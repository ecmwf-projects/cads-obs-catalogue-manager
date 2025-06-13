import importlib
import importlib.resources
from pathlib import Path

import cftime
import h5netcdf
import numpy
import pandas
from matplotlib import pyplot

from cdsobs.constants import TIME_UNITS
from cdsobs.ingestion.readers.cuon import _maybe_concat_chars, get_cuon_stations
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def plot_station_number():
    active_json = str(
        Path(importlib.resources.files("tests"), "data/cuon_data", "active.json")
    )
    stations = get_cuon_stations(active_json)
    stations["start of records"] = cftime.num2date(
        stations["start of records"], units=TIME_UNITS
    )
    stations["end of records"] = cftime.num2date(
        stations["end of records"], units=TIME_UNITS
    )
    print(stations.head().to_string())
    months = pandas.date_range("1901-01-01", "2023-12-31", freq="MS")
    station_number = pandas.Series(index=months)
    for monthdate in months:
        station_record_has_started = stations["start of records"] <= monthdate
        station_record_has_not_ended = stations["end of records"] >= monthdate
        station_number.loc[monthdate] = numpy.logical_and(
            station_record_has_started, station_record_has_not_ended
        ).sum()

    station_number.iloc[0:100].plot()
    pyplot.show()


def get_char_var_data(inc_group, variable):
    return _maybe_concat_chars(inc_group[variable][:])


def check_primary_keys_consistency():
    idir = Path("/data/public/converted_v19")
    for ipath in idir.glob("*.nc"):
        logger.info(f"Checking {ipath}")
        try:
            check_primary_keys_consistency_file(ipath)
        except Exception as e:
            logger.info(f"Exception captured for {ipath}: {e}")


def check_primary_keys_consistency_file(ipath):
    with h5netcdf.File(ipath) as inc:
        station_table = inc.groups["station_configuration"]
        header_table = inc.groups["header_table"]
        observations_table = inc.groups["observations_table"]
        station_ids_station_table = get_char_var_data(station_table, "primary_id")
        station_ids_header_table = get_char_var_data(header_table, "primary_station_id")
        ids_ok = set(station_ids_station_table) == set(station_ids_header_table)
        if not ids_ok:
            logger.warning(f"Station ids wrong for {ipath}")
        records_ok = set(station_table["record_number"][:]) == set(
            header_table["station_record_number"][:]
        )
        if not records_ok:
            logger.warning(f"Station record number wrong for {ipath}")
        report_ids_observations_table = get_char_var_data(
            observations_table, "report_id"
        )
        report_ids_header_table = get_char_var_data(header_table, "report_id")
        report_ids_ok = set(report_ids_observations_table) == set(
            report_ids_header_table
        )
        if not report_ids_ok:
            logger.warning(f"Station record ids wrong for {ipath}")


if __name__ == "__main__":
    plot_station_number()
