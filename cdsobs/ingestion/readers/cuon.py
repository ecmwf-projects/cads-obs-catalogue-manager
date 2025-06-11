import calendar
import importlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import cftime
import dask
import h5py
import numpy
import pandas

from cdsobs import constants
from cdsobs.cdm.denormalize import denormalize_tables
from cdsobs.cdm.tables import CDMTable, read_cdm_tables
from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.api import EmptyBatchException
from cdsobs.ingestion.core import TimeBatch, TimeSpaceBatch
from cdsobs.retrieve.filter_datasets import between, get_var_code_dict
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.utils import datetime_to_seconds

logger = get_logger(__name__)


@dataclass
class CUONFileandSlices:
    path: Path
    variable_slices: dict[str, slice]


def _read_nc_file(
    file_and_slices: CUONFileandSlices, table_name: str, time_batch: TimeBatch
) -> dict[str, numpy.ndarray] | None:
    try:
        return read_nc_file(file_and_slices, table_name, time_batch)
    except Exception as e:
        logger.warning(
            f"The following error was captured reading {file_and_slices.path}: {e}"
        )
        return None


def read_nc_file(
    file_and_slices: CUONFileandSlices, table_name: str, time_batch: TimeBatch
) -> dict[str, numpy.ndarray] | None:
    """Read nc table using h5py."""
    sorted_by_variable = [
        "advanced_homogenisation",
        "advanced_uncertainty",
        "era5fb",
        "observations_table",
    ]
    sorted_by_date = ["header_table", "source_configuration"]

    # Get times in seconds from 1900-01-01
    selected_end, selected_start = _get_times_in_seconds_from(time_batch)

    # open file and select only data necessary to read
    file_path = file_and_slices.path
    logger.info(f"Reading table {table_name} from {file_path}")
    slices = file_and_slices.variable_slices
    with h5py.File(file_path) as hfile:
        var_data: dict[str, dict[str, numpy.ndarray]] = {}
        # iterating through the variables and their indices in recordindices
        # store them in a dataframe dictionary for further use
        var_data = _process_table(
            hfile,
            selected_end,
            selected_start,
            sorted_by_date,
            sorted_by_variable,
            table_name,
            var_data,
            slices,
        )
        if var_data == {}:
            return None
        else:
            fields_in_file = list(list(var_data.values())[0])
            result = {
                field: numpy.concatenate(
                    [vardata[field] for varcode, vardata in var_data.items()]
                )
                for field in fields_in_file
            }
            return result


def _process_table(
    hfile: h5py.File,
    selected_end: int,
    selected_start: int,
    sorted_by_date: list[str],
    sorted_by_variable: list[str],
    table_name: str,
    var_data: dict[str, dict[str, numpy.ndarray]],
    slices: dict[str, slice],
) -> dict[str, dict[str, numpy.ndarray]]:
    if table_name in sorted_by_variable:
        vals_to_exclude = [
            "index",
            "recordtimestamp",
            "string1",
            "string4",
            "type",
            "expver",
            "class",
            "stream",
            "report_status@hdr",
            "report_event1@hdr",
            "report_rdbflag@hdr",
            "lat@hdr",
            "lon@hdr",
            "lsm@modsurf",
            "orography@modsurf",
            "windspeed10m@modsurf",
            "vertco_reference_2@body",
            "ppcode@conv_body",
            "datum_anflag@body",
            "datum_status@body",
            "datum_event1@body",
            "datum_rdbflag@body",
            "qc_pge@body",
            "lsm@surfbody_feedback",
            "obs_error@errstat",
            "final_obs_error@errstat",
            "fg_error@errstat",
            "eda_spread@errstat",
            "processing_level",
            "location_method",
            "source_id",
            "crs",
            "original_units",
        ]
        file_vars = [
            fv
            for fv in numpy.array(hfile["recordindices"])
            if fv not in vals_to_exclude
        ]
        for variable in file_vars:
            logger.debug(f"Reading variable {variable}")
            selector = slices[variable]
            # dropping string dims - not necessary for dataframes
            fields = [
                f
                for f in hfile[table_name]
                if "string" not in f and f not in vals_to_exclude
            ]
            data: dict[str, numpy.ndarray] = {
                field: _get_field_data(field, hfile, selector, table_name)
                for field in fields
            }
            var_data[variable] = data
    else:
        time_index = hfile["header_table"]["report_timestamp"][:]
        if table_name in sorted_by_date:
            selector = (time_index >= selected_start) & (time_index <= selected_end)
        else:
            selector = slice(None)
        # dropping string dims - not necessary for dataframes
        fields = [f for f in hfile[table_name] if "string" not in f]
        data = {
            field: _get_field_data(field, hfile, selector, table_name)
            for field in fields
        }
        var_data[table_name] = data
    return var_data


def _get_field_data(field, hfile, selector, table_name):
    field_obj = hfile[table_name][field]
    if len(field_obj.shape) > 1:
        field_data = field_obj[selector, ...]
    else:
        field_data = field_obj[selector]

    field_data = _maybe_concat_chars(field_data)
    field_data = _maybe_swap_bytes(field_data)
    return field_data


def _get_times_in_seconds_from(time_batch):
    year = time_batch.year
    month = time_batch.month
    last_month_day = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01T00:00:00"
    end = f"{year}-{month:02d}-{last_month_day}T23:59:59"
    selected_start = datetime_to_seconds(numpy.datetime64(start))
    selected_end = datetime_to_seconds(numpy.datetime64(end))
    return selected_end, selected_start


def _maybe_concat_chars(field_data):
    # recover byte array strings - not necessary for dataframes
    if len(field_data.shape) > 1:
        field_len, str_len = field_data.shape
        field_data = field_data.view(f"S{str_len}").reshape(field_len)
    else:
        field_data = numpy.array(field_data)
    return field_data


def _maybe_swap_bytes(field_data):
    if field_data.dtype.byteorder == ">":
        # Flip big endian fields
        field_data = field_data.newbyteorder().byteswap()
    return field_data


def read_table_data(
    file_and_slices: CUONFileandSlices, table_name: str, time_batch: TimeBatch
) -> pandas.DataFrame:
    """Read nc table of all station files using h5py."""
    result = _read_nc_file(file_and_slices, table_name, time_batch)

    if result is not None:
        final_df_out = pandas.DataFrame(result)
    else:
        final_df_out = pandas.DataFrame()
    # Reduce field size for memory efficiency
    for field in final_df_out:
        if str(final_df_out[field].dtype) == "float64":
            final_df_out[field] = final_df_out[field].astype("float32")
    return final_df_out


def filter_batch_stations(
    files: Iterable[Path], time_space_batch: TimeSpaceBatch
) -> list[Path]:
    station_metadata = get_cuon_stations()
    selected_end, selected_start = _get_times_in_seconds_from(
        time_space_batch.time_batch
    )
    lon_start, lon_end, lat_start, lat_end = time_space_batch.get_spatial_coverage()
    lon_mask = between(station_metadata.lon, lon_start, lon_end)
    lat_mask = between(station_metadata.lat, lat_start, lat_end)
    time_mask = numpy.logical_and(
        station_metadata["start of records"] <= selected_end,
        station_metadata["end of records"] >= selected_start,
    )
    mask = lon_mask * lat_mask * time_mask
    batch_stations = station_metadata.loc[mask].index
    return [f for f in files if f.name.split("_")[0] in batch_stations]


def get_cuon_stations():
    # Read file with CUON stations locations
    columns = [
        "start of records",
        "end of records",
        "lat",
        "lon",
        "country code",
        "file path",
    ]
    cuon_stations_file = Path(
        str(importlib.resources.files("cdsobs")),
        "data/insitu-comprehensive-upper-air-observation-network/active.json",
    )
    station_metadata = pandas.read_json(cuon_stations_file, orient="index")
    station_metadata.columns = columns
    return station_metadata


def read_cuon_netcdfs(
    dataset_name: str,
    config: CDSObsConfig,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
    input_dir: str,
) -> pandas.DataFrame:
    files = list(Path(input_dir).glob("*.nc"))
    if len(files) == 0:
        raise RuntimeError(f"CUON files not found in {Path(input_dir).absolute()}")
    files = filter_batch_stations(files, time_space_batch)
    if len(files) == 0:
        raise EmptyBatchException
    # Avoid for now: sensor_configuration, source_configuration
    tables_to_use = config.get_dataset(dataset_name).available_cdm_tables
    cdm_tables = read_cdm_tables(config.cdm_tables_location, tables_to_use)
    files_and_slices = read_all_nc_slices(files, time_space_batch.time_batch)
    denormalized_tables_futures = []
    scheduler = get_scheduler()
    # Check for emptiness
    if len(files_and_slices) == 0:
        raise EmptyBatchException
    # Use dask to speed up the process
    for file_and_slices in files_and_slices:
        denormalized_table_future = dask.delayed(_get_denormalized_table_file)(
            cdm_tables, config, file_and_slices, tables_to_use, time_space_batch
        )
        if denormalized_table_future is not None:
            denormalized_tables_futures.append(denormalized_table_future)
    denormalized_tables = dask.compute(
        *denormalized_tables_futures,
        scheduler=scheduler,
        num_workers=min(len(files_and_slices), 32),
    )
    # Check for emptiness
    if all([dt is None for dt in denormalized_tables]):
        raise EmptyBatchException
    result = pandas.concat(denormalized_tables)
    allnan_cols = []
    for col in result:
        if result[col].isnull().all():
            allnan_cols.append(col)
    logger.info(f"Removing columns {allnan_cols} as they don't have any data")
    result = result.drop(allnan_cols, axis=1)
    return result


def get_scheduler():
    if os.environ.get("CADSOBS_AVOID_MULTIPROCESS"):
        # This is for the tests.
        scheduler = "synchronous"
    else:
        # Do not use threads as HDF5 is not yet thread safe.
        scheduler = "processes"
    return scheduler


def _get_denormalized_table_file(*args):
    try:
        return get_denormalized_table_file(*args)
    except NoDataInFileException:
        return None


def get_denormalized_table_file(
    cdm_tables, config, file_and_slices, tables_to_use, time_space_batch
):
    dataset_cdm: dict[str, pandas.DataFrame] = {}
    for table_name, table_definition in cdm_tables.items():
        # Fix era5fb having different names in the CDM and in the files
        if table_name == "era5fb_table":
            table_name_in_file = "era5fb"
        else:
            table_name_in_file = table_name
        # Read table data
        table_data = read_table_data(
            file_and_slices, table_name_in_file, time_space_batch.time_batch
        )
        # Don't try to fix empty tables unless it is one of the main tables
        if len(table_data) > 0 or table_name in ["header_table", "observations_table"]:
            # Make sure that latitude and longitude always carry on their table name.
            table_data = _fix_table_data(
                dataset_cdm,
                table_data,
                table_definition,
                table_name,
                file_and_slices.path,
                time_space_batch,
            )
            dataset_cdm[table_name] = table_data
        else:
            # Copy tables to use and remove the empty table
            # this is for not losing the stations with no homogenisation_table
            tables_to_use = [t for t in tables_to_use if t != table_name]
    # Filter stations outside ofthe Batch
    lats = dataset_cdm["header_table"]["latitude"]
    lons = dataset_cdm["header_table"]["longitude"]
    if (lats.dtype.kind == "S") or (lons.dtype.kind == "S"):
        raise NoDataInFileException(
            f"Skipping file {file_and_slices.path} with malformed latitudes"
        )
    lon_start, lon_end, lat_start, lat_end = time_space_batch.get_spatial_coverage()
    lon_mask = between(lons, lon_start, lon_end)
    lat_mask = between(lats, lat_start, lat_end)
    spatial_mask = lon_mask * lat_mask
    if spatial_mask.sum() < len(spatial_mask):
        logger.info(
            f"Records have been found outside the SpatialBatch ranges for {file_and_slices.path}, "
            "filtering out."
        )
        dataset_cdm["header_table"] = dataset_cdm["header_table"].loc[spatial_mask]
    # Denormalize tables
    denormalized_table_file = denormalize_tables(
        cdm_tables, dataset_cdm, tables_to_use, ignore_errors=False
    )
    # Decode time
    if len(denormalized_table_file) > 0:
        for time_field in ["record_timestamp", "report_timestamp", "date_time"]:
            denormalized_table_file.loc[:, time_field] = cftime.num2date(
                denormalized_table_file.loc[:, time_field],
                constants.TIME_UNITS,
                only_use_cftime_datetimes=False,
            )
    else:
        logger.warning(f"No data was found in file {file_and_slices.path}")
    # Need this here to avoid nans in this variable that is an integer
    denormalized_table_file["uncertainty_type1"] = 1
    denormalized_table_file["uncertainty_type1"] = denormalized_table_file[
        "uncertainty_type1"
    ].astype("int")
    # Fixes for CUON V29 files
    if "RISE_bias_estimate" in denormalized_table_file:
        logger.warning("Applying fixes for CUON V29 files homogenisation")
        denormalized_table_file = denormalized_table_file.rename(
            dict(RISE_bias_estimate="homogenisation_adjustment"), axis=1
        )
        denormalized_table_file["profile_id"] = denormalized_table_file.report_id.copy()
        denormalized_table_file["quality_flag"] = 2
        denormalized_table_file["homogenisation_method"] = 14
        denormalized_table_file["report_meaning_of_timestamp"] = 1
        # Merge homogenisation adjustments
        homogenisation_adjustment = denormalized_table_file["homogenisation_adjustment"]
        observed_variable = denormalized_table_file["observed_variable"]
        # Humidity
        humidity_adjustment = denormalized_table_file["humidity_bias_estimate"]
        humidity_variables = (34, 137, 138, 39)
        mask = numpy.isin(observed_variable, humidity_variables)
        homogenisation_adjustment.loc[mask] = humidity_adjustment[mask]
        # Wind
        wind_adjustment = denormalized_table_file["wind_bias_estimate"]
        wind_variables = (106, 107, 139, 140)
        mask = numpy.isin(observed_variable, wind_variables)
        homogenisation_adjustment.loc[mask] = wind_adjustment[mask]
        # Remove these, we don't need them, it is not all in homogenisation_adjustment
        denormalized_table_file = denormalized_table_file.drop(
            ["humidity_bias_estimate", "wind_bias_estimate"], axis=1
        )
    else:
        logger.warning("Bias estimates not found for this partition")
    # Remove obs id zero
    denormalized_table_file = denormalized_table_file.loc[
        denormalized_table_file["observed_variable"] != 0
    ]
    # Decode variable names
    code_dict = get_var_code_dict(config.cdm_tables_location)
    denormalized_table_file["observed_variable"] = denormalized_table_file[
        "observed_variable"
    ].map(code_dict)
    return denormalized_table_file


class NoDataInFileException(RuntimeError):
    pass


def _fix_table_data(
    dataset_cdm: dict[str, pandas.DataFrame],
    table_data: pandas.DataFrame,
    table_definition: CDMTable,
    table_name: str,
    file_path: Path,
    time_space_batch: TimeSpaceBatch,
):
    # the name in station_configuration
    if table_name == "header_table":
        vars_to_drop = [
            "station_name",
            "platform_sub_type",
            "platform_type",
            "station_type",
            "crs",
            "profile_id",
        ]
        table_data = table_data.drop(vars_to_drop, axis=1, errors="ignore")

    if table_name == "station_configuration":
        vars_to_drop = ["station_automation"]
        table_data = table_data.drop(vars_to_drop, axis=1, errors="ignore")

    # Check that observation id is unique and fix if not
    if table_name == "observations_table":
        # If there is nothing here it is a waste of time to continue
        if len(table_data) == 0:
            logger.warning(f"No data found in {file_path} for {time_space_batch}.")
            raise NoDataInFileException
        # Check if observation ids are unique and replace them if not
        if not table_data.observation_id.is_unique:
            raise RuntimeError(f"observation_id is not unique in {file_path}, fixing")
        # Fix latitude and longitude adding lond and latd so they really represent the
        # measurement location
        table_data["latitude"] += table_data["latd"]
        table_data["longitude"] += table_data["lond"]
        table_data["date_time"] += table_data["timed"]
    # Remove duplicate station records
    if table_name == "station_configuration":
        table_data = table_data.drop_duplicates(
            subset=["primary_id", "record_number"], ignore_index=True
        )
        table_data = table_data.drop(["latitude", "longitude"], axis=1)
    # Check primary keys can be used to build a unique index
    primary_keys = table_definition.primary_keys
    if table_name in [
        "era5fb_table",
        "advanced_homogenisation",
        "advanced_uncertainty",
    ]:
        table_data = table_data.reset_index()
        table_data_len = len(table_data)
        obs_table_len = len(dataset_cdm["observations_table"])
        logger.warning(
            f"Filling era5fb table index with observation_id from observations_table in {file_path}"
        )
        obs_id_name = "obs_id" if table_name == "era5fb_table" else "observation_id"
        if table_data_len != obs_table_len:
            raise RuntimeError(
                f"{table_name} size is different to observations_table size "
                f"{table_data_len} vs {obs_table_len}"
            )
        else:
            observation_ids = dataset_cdm["observations_table"].observation_id.values
        table_data[obs_id_name] = observation_ids
        table_data = table_data.set_index(obs_id_name).rename(
            {"index": f"index|{table_name}"}, axis=1
        )
    # Rename uncertainty
    if table_name == "advanced_uncertainty":
        table_data = table_data.rename(dict(desroziers_30="uncertainty_value1"), axis=1)
        table_data["uncertainty_type1"] = 1
        table_data["uncertainty_type1"] = table_data["uncertainty_type1"].astype("int")
        table_data.loc[:, "uncertainty_units1"] = dataset_cdm["observations_table"][
            "units"
        ].values
    if "level_0" in table_data:
        table_data = table_data.drop("level_0", axis=1)
    primary_keys_are_unique = (
        table_data.reset_index().set_index(primary_keys).index.is_unique
    )
    if not primary_keys_are_unique:
        logger.warning(
            f"Unable to build a unique index with primary_keys in {file_path}"
            f"{table_definition.primary_keys} in table {table_name}"
        )
    return table_data


def read_nc_file_slices(
    nc_file: Path, time_batch: TimeBatch
) -> CUONFileandSlices | None:
    """Read nc table using h5py."""
    # check if file is available
    if not nc_file.exists():
        raise FileNotFoundError

    # Get times in seconds from 1900-01-01
    selected_end, selected_start = _get_times_in_seconds_from(time_batch)

    # open file and select only data necessary to read
    try:
        with h5py.File(nc_file) as hfile:
            vals_to_exclude = ["index", "recordtimestamp", "string1"]
            file_vars = [
                fv
                for fv in numpy.array(hfile["recordindices"])
                if (fv not in vals_to_exclude) and ("string" not in fv)
            ]
            record_times = hfile["recordindices"]["recordtimestamp"]
            # load record record_times
            record_times = record_times[:]
            first_timestamp = record_times.min()
            last_timestamp = record_times.max()

            if first_timestamp > selected_end or last_timestamp < selected_start:
                # Return None if there are no times inside the batch
                logger.warning(
                    f"No times found in {nc_file} inside the batch: "
                    f"{first_timestamp=} {last_timestamp=} {selected_start=} {selected_end=}"
                )
                result = None
            else:
                ris = {
                    filevar: hfile["recordindices"][filevar][:] for filevar in file_vars
                }

                selectors = {}
                for variable in file_vars:
                    logger.debug(f"Reading slice for variable {variable}")
                    times_indices = numpy.searchsorted(
                        record_times, (selected_start, selected_end)
                    )
                    first_index = times_indices[0]
                    if times_indices[1] == len(ris[variable]):
                        last_index = times_indices[1] - 1
                    else:
                        last_index = times_indices[1]
                    selectors[variable] = slice(
                        ris[variable][first_index], ris[variable][last_index]
                    )
                result = CUONFileandSlices(nc_file, selectors)
    except Exception as e:
        logger.warning(f"Failed to read indices with error {e}")
        result = None

    return result


def read_all_nc_slices(files: List, time_batch: TimeBatch) -> list[CUONFileandSlices]:
    """Read variable slices of all station files using h5py."""
    tocs = []

    for file in files:
        logger.info(f"Reading slices from {file=}")
        toc = dask.delayed(read_nc_file_slices)(Path(file), time_batch)
        tocs.append(toc)

    scheduler = get_scheduler()
    tocs = dask.compute(*tocs, scheduler=scheduler, num_workers=min(len(files), 32))
    tocs = [t for t in tocs if t is not None]
    return tocs
