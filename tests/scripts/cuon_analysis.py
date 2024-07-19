from pathlib import Path

import cftime
import h5netcdf
import numpy
import pandas
from matplotlib import pyplot

from cdsobs.constants import TIME_UNITS
from cdsobs.ingestion.readers.cuon import get_cuon_stations
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def plot_station_number():
    stations = get_cuon_stations()
    stations["start of records"] = cftime.num2date(
        stations["start of records"], units=TIME_UNITS
    )
    stations["end of records"] = cftime.num2date(
        stations["end of records"], units=TIME_UNITS
    )
    print(stations.head().to_string())
    months = pandas.date_range("1905-01-01", "2023-12-31", freq="MS")
    station_number = pandas.Series(index=months)
    for monthdate in months:
        station_record_has_started = stations["start of records"] <= monthdate
        station_record_has_not_ended = stations["end of records"] >= monthdate
        station_number.loc[monthdate] = numpy.logical_and(
            station_record_has_started, station_record_has_not_ended
        ).sum()

    station_number.plot()
    pyplot.show()


def get_char_var_data(inc_group, variable):
    str_len = inc_group[variable].shape[1]
    return inc_group[variable][:].view(f"S{str_len}").T[0]


def check_primary_keys_consistency():
    idir = Path(
        "/home/garciam/git/copds/cads-obs-catalogue-manager/tests/data/cuon_data"
    )
    for ipath in idir.glob("*.nc"):
        with h5netcdf.File(ipath) as inc:
            station_table = inc.groups["station_configuration"]
            header_table = inc.groups["header_table"]
            observations_table = inc.groups["observations_table"]
            station_ids_station_table = get_char_var_data(station_table, "primary_id")
            station_ids_header_table = get_char_var_data(
                header_table, "primary_station_id"
            )
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
    check_primary_keys_consistency()
