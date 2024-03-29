"""Retrieve pipeline."""
import itertools
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Tuple

import cftime
import fsspec
import h5netcdf
import numpy
import pandas
import xarray
from fsspec.implementations.http import HTTPFileSystem

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.constants import TIME_UNITS_REFERENCE_DATE
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.filter_datasets import (
    between,
    get_param_name_in_data,
)
from cdsobs.retrieve.models import RetrieveArgs, RetrieveFormat, RetrieveParams
from cdsobs.retrieve.retrieve_services import estimate_data_size, ezclump
from cdsobs.service_definition.api import get_service_definition
from cdsobs.utils.logutils import SizeError, get_logger
from cdsobs.utils.utils import get_code_mapping, get_database_session

logger = get_logger(__name__)
MAX_NUMBER_OF_GROUPS = 10


def retrieve_observations(
    catalogue_url: str,
    storage_url: str,
    retrieve_args: RetrieveArgs,
    output_dir: Path,
    size_limit: int,
) -> Path:
    """
    Retrieve data from the obs repository and save it to a netCDF file.

    Parameters
    ----------
    catalogue_url:
      URL of the catalogue database including credentials, in the form of
      "postgresql+psycopg2://someuser:somepass@hostname:port/catalogue"
    storage_url:
      Storage URL
    retrieve_args :
      Arguments defining how to filter the database.
    output_dir :
      Path to directory where to save the output netCDF file.
    size_limit :
      Maximum size allowed for the download
    """
    logger.info("Starting retrieve pipeline.")
    # Query the storage to get the URLS of the files that contain the data requested
    with get_database_session(catalogue_url) as session:
        catalogue_repository = CatalogueRepository(session)
        entries = _get_catalogue_entries(catalogue_repository, retrieve_args)
        object_urls = _get_urls_and_check_size(
            entries, retrieve_args, size_limit, storage_url
        )
    # Get the path of the output file
    output_path_netcdf = _get_output_path(output_dir, retrieve_args.dataset, "netCDF")
    # First we always write the netCDF-lite file
    logger.info(f"Streaming data to {output_path_netcdf}")
    # We first need to loop over the files to get the max size of the strings fields
    # This way we can know the size of the output
    # background cache will download blocks in the background ahead of time using a
    # thread.
    fs = fsspec.filesystem(
        "https", cache_type="background", block_size=10 * (1024**2)
    )
    # Silence fsspec log as background cache does print unformatted log lines.
    logging.getLogger("fsspec").setLevel(logging.WARNING)
    # Get the maximum size of the character arrays
    char_sizes = _get_char_sizes(fs, object_urls)
    if retrieve_args.params.variables is None:
        variables = set(itertools.chain.from_iterable([e.variables for e in entries]))
    else:
        variables = set(retrieve_args.params.variables)
    char_sizes["observed_variable"] = max([len(v) for v in variables])
    # Open the output file and dump the data from each input file.
    with h5netcdf.File(output_path_netcdf, "w") as oncobj:
        oncobj.dimensions["index"] = None
        for url in object_urls:
            filter_asset_and_save(fs, oncobj, retrieve_args, url, char_sizes)
        # Check if the resulting file is empty
        if len(oncobj.variables) == 0 or len(oncobj.variables["report_timestamp"]) == 0:
            raise RuntimeError(
                "No data was found, try a different parameter combination."
            )
        # Add atributes
        _add_attributes(oncobj, retrieve_args)
    # If the user asked for a CSV, we transform the file to CSV
    if retrieve_args.params.format == "netCDF":
        output_path = output_path_netcdf
    else:
        try:
            output_path = _to_csv(output_dir, output_path_netcdf, retrieve_args)
        finally:
            # Ensure that the netCDF is not left behind taking disk space.
            output_path_netcdf.unlink()
    return output_path


def _add_attributes(oncobj: h5netcdf.File, retrieve_args: RetrieveArgs):
    """Add relevant attributes to the output netCDF."""
    if "height_of_station_above_sea_level" in oncobj.variables:
        oncobj.variables["height_of_station_above_sea_level"].attrs["units"] = "m"
    oncobj.variables["longitude"].attrs["standard_name"] = "longitude"
    oncobj.variables["latitude"].attrs["standard_name"] = "latitude"
    oncobj.variables["report_timestamp"].attrs["standard_name"] = "time"
    oncobj.variables["longitude"].attrs["units"] = "degrees_east"
    oncobj.variables["latitude"].attrs["units"] = "degrees_north"
    oncobj.attrs["featureType"] = "point"
    # Global attributes
    service_definition = get_service_definition(retrieve_args.dataset)
    oncobj.attrs.update(service_definition.global_attributes)


def _to_csv(
    output_dir: Path, output_path_netcdf: Path, retrieve_args: RetrieveArgs
) -> Path:
    """Transform the output netCDF to CSV format."""
    output_path = _get_output_path(output_dir, retrieve_args.dataset, "csv")
    cdm_lite_dataset = xarray.open_dataset(
        output_path_netcdf, chunks=dict(observation_id=100000), decode_times=True
    )
    logger.info("Transforming netCDF to CSV")
    with output_path.open("w") as ofileobj:
        header = _get_csv_header(retrieve_args, cdm_lite_dataset)
        ofileobj.write(header)
    # Beware this will not work with old dask versions because of a bug
    # https://github.com/dask/dask/issues/10414
    cdm_lite_dataset.to_dask_dataframe().astype("str").to_csv(
        output_path,
        index=False,
        single_file=True,
        mode="a",
        compute_kwargs={"scheduler": "single-threaded"},
    )
    return output_path


def _get_urls_and_check_size(
    entries: Sequence[Catalogue],
    retrieve_args: RetrieveArgs,
    size_limit: int,
    storage_url: str,
) -> list[str]:
    """
    Get the catalogue entries to get the urls of the assets. Check the size too.

    We group this is this function to ensure that entries, that can be a bit big,
    are garbage collected.
    """
    object_urls = [f"{storage_url}/{e.asset}" for e in entries]
    _check_data_size(entries, retrieve_args, size_limit)
    return object_urls


def _get_char_sizes(fs: HTTPFileSystem, object_urls: list[str]) -> dict[str, int]:
    """
    Iterate over the input files to get the size of the string variables.

    We need to know this beforehand so we can stream to the output file.
    """
    char_sizes = {}
    for url in object_urls:
        with get_url_ncobj(fs, url) as incobj:
            for var, varobj in incobj.items():
                if varobj.dtype.kind == "S":
                    char_size = varobj.shape[1]
                else:
                    continue
                if var not in char_sizes:
                    char_sizes[var] = char_size
                else:
                    char_sizes[var] = max(char_sizes[var], char_size)

    return char_sizes


@dataclass
class AssetSizes:
    """Group different sizes we need to process the asset."""


def filter_asset_and_save(
    fs: HTTPFileSystem,
    oncobj: h5netcdf.File,
    retrieve_args: RetrieveArgs,
    url: str,
    char_sizes: dict[str, int],
):
    """Get the filtered data from the asset and dump it to the output file."""
    with get_url_ncobj(fs, url) as incobj:
        mask = get_mask(incobj, retrieve_args.params)
        if mask.any():
            number_of_groups = len(ezclump(mask))
            mask_size = mask.sum()
            # We will download the full chunks in this cases, as it is way more efficient
            download_all_chunk = (
                number_of_groups > MAX_NUMBER_OF_GROUPS or mask_size > mask.size * 0.8
            )
            if download_all_chunk:
                logger.debug("Downloading all chunk for efficiency")

            # Resize dimension needs to be done explicitly in h5netcdf
            output_current_size = oncobj.dimensions["index"].size
            new_size = output_current_size + mask.sum()
            oncobj.resize_dimension("index", new_size)
            # Get the variables in the input file that are in the CDM lite specification.
            vars_in_cdm_lite = get_vars_in_cdm_lite(incobj)
            # Filter and save the data for each variable.
            for ivar in vars_in_cdm_lite:
                filter_and_save_var(
                    incobj,
                    ivar,
                    oncobj,
                    output_current_size,
                    new_size,
                    char_sizes,
                    mask,
                    mask_size,
                    download_all_chunk,
                )
        else:
            # Sometimes no data will be found as for example requested station may not
            # have the requested varaibles available.
            logger.debug("No data found in asset for the query paramater.")


def filter_and_save_var(
    incobj: h5netcdf.File,
    ivar: str,
    oncobj: h5netcdf.File,
    current_size: int,
    new_size: int,
    char_sizes: dict[str, int],
    mask: numpy.typing.NDArray,
    mask_size: int,
    download_all_chunk: bool,
):
    """
    Filter and save the data for each variable.

    String variables need special treatment as they have an extra dimension.
    """
    ivarobj = incobj.variables[ivar]
    dimensions: tuple[str, ...] = ("index",)
    # Use input chunksize except if it is bigger than get data we are getting.
    chunksize: tuple[int, ...] = (
        (ivarobj.chunks[0],) if ivarobj.chunks[0] < mask_size else (mask_size,)
    )
    dtype = _get_output_dtype(ivar, ivarobj)
    attrs = dict()
    # Remove table name from the coordinates
    ivar, cdm_table = _remove_table_name_from_coordinates(incobj, ivar)
    if cdm_table is not None:
        attrs["cdm_table"] = cdm_table
    # Set time units
    if ivar == "report_timestamp":
        attrs["units"] = ivarobj.attrs["units"]
    # Handle character dimensions
    is_char = len(ivarobj.shape) > 1 or ivar == "observed_variable"
    if is_char:
        chunksize, dimensions = handle_string_dims(
            char_sizes, chunksize, dimensions, ivar, oncobj
        )
    # Create the variable
    if ivar not in oncobj.variables:
        # It is not worth it to go further than complevel 1 and it is much faster
        ovar = oncobj.create_variable(
            ivar,
            dimensions,
            dtype,
            chunks=chunksize,
            compression="gzip",
            compression_opts=1,
        )
    else:
        ovar = oncobj.variables[ivar]
    # Set variable attributes
    ovar.attrs.update(attrs)
    # Dump the data to the file
    if is_char:
        dump_char_variable(
            current_size,
            incobj,
            ivar,
            ivarobj,
            mask,
            new_size,
            ovar,
            download_all_chunk,
        )
    else:
        if download_all_chunk:
            data = ivarobj[:][mask]
        else:
            data = ivarobj[mask]
        ovar[current_size:new_size] = data


def _get_output_dtype(ivar: str, ivarobj: h5netcdf.Variable) -> str:
    if ivar == "observed_variable":
        dtype = "S1"
    else:
        dtype = ivarobj.dtype
    return dtype


def handle_string_dims(char_sizes, chunksize, dimensions, ivar, oncobj):
    ivar_str_dim = ivar + "_stringdim"
    ivar_str_dim_size = char_sizes[ivar]
    if ivar_str_dim not in oncobj.dimensions:
        oncobj.dimensions[ivar_str_dim] = ivar_str_dim_size
    dimensions += (ivar_str_dim,)
    chunksize += (ivar_str_dim_size,)
    return chunksize, dimensions


def dump_char_variable(
    current_size: int,
    incobj: h5netcdf.File,
    ivar: str,
    ivarobj: h5netcdf.Variable,
    mask: numpy.typing.NDArray,
    new_size: int,
    ovar: h5netcdf.Variable,
    download_all_chunk: bool,
):
    if ivar != "observed_variable":
        actual_str_dim_size = ivarobj.shape[-1]
        if download_all_chunk:
            data = ivarobj[:, 0:actual_str_dim_size][mask, :]
        else:
            data = ivarobj[mask, 0:actual_str_dim_size]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data
    else:
        # For observed variable, we use the attributes to decode the integers.
        if download_all_chunk:
            data = ivarobj[:][mask]
        else:
            data = ivarobj[mask]
        code2var = get_code_mapping(incobj, inverse=True)
        codes_in_data, inverse = numpy.unique(data, return_inverse=True)
        variables_in_data = numpy.array(
            [code2var[c].encode("utf-8") for c in codes_in_data]
        )
        data_decoded = variables_in_data[inverse]
        data_decoded = data_decoded.view("S1").reshape(data.size, -1)
        actual_str_dim_size = data_decoded.shape[-1]
        ovar[current_size:new_size, 0:actual_str_dim_size] = data_decoded


def _remove_table_name_from_coordinates(
    incobj: h5netcdf.File, ivar: str
) -> Tuple[str, str | None]:
    if ivar in ["latitude|header_table", "longitude|header_table"]:
        ovar, cdm_table = ivar.split("|")
    elif (
        ivar in ["latitude|station_configuration", "longitude|station_configuration"]
        and "latitude|header_table" not in incobj.variables
    ):
        ovar, cdm_table = ivar.split("|")
    else:
        ovar = ivar
        cdm_table = None
    return ovar, cdm_table


def get_vars_in_cdm_lite(incobj: h5netcdf.File) -> list[str]:
    """Return the variables in incobj that are defined in the CDM-lite."""
    vars_in_cdm_lite = [v for v in incobj.variables if v in cdm_lite_variables]
    # This searches for variables with "|cdm_table  in their name."
    vars_with_bar_in_cdm_lite = [
        v
        for v in incobj.variables
        if "|" in v and v.split("|")[0] in cdm_lite_variables
    ]
    vars_in_cdm_lite += vars_with_bar_in_cdm_lite
    return vars_in_cdm_lite


def get_mask(incobj: h5netcdf.File, retrieve_params: RetrieveParams) -> numpy.ndarray:
    """Return a boolean mask with requested observation_ids."""
    logger.debug("Filtering data in retrieved chunk data.")
    retrieve_params_dict = retrieve_params.model_dump()
    masks_combined = numpy.ones(
        shape=(incobj.dimensions["observation_id"].size,), dtype="bool"
    )
    # Filter header table
    if retrieve_params.stations is not None:
        stations_asked = [s.encode("utf-8") for s in retrieve_params.stations]
        stationvar = incobj.variables["primary_station_id"]
        field_len, strlen = stationvar.shape
        stations_in_partition = (
            incobj.variables["primary_station_id"][:]
            .view(f"S{strlen}")
            .reshape(field_len)
        )
        station_mask = numpy.isin(stations_in_partition, stations_asked)
        masks_combined = numpy.logical_and(masks_combined, station_mask)
    # Filter time and space
    time_and_space = ["time_coverage", "longitude_coverage", "latitude_coverage"]
    for param_name in time_and_space:
        coverage_range = retrieve_params_dict[param_name]
        if coverage_range is not None:
            # Get the parameter we went to filter as a pandas.Index()
            param_name_in_data = get_param_name_in_data(incobj, param_name)
            param_index = incobj.variables[param_name_in_data][:]
            if param_name == "time_coverage":
                # Turn dates into integers with the same units
                units = incobj.variables[param_name_in_data].attrs["units"]
                coverage_range = cftime.date2num(coverage_range, units=units)
            param_mask = between(param_index, coverage_range[0], coverage_range[1])
            masks_combined = numpy.logical_and(masks_combined, param_mask)
    # Filter days (month and year not needed)
    if retrieve_params_dict["day"] is not None:
        times_index = pandas.to_datetime(
            incobj.variables["report_timestamp"][:],
            unit="s",
            origin=TIME_UNITS_REFERENCE_DATE,
        )
        param_mask = times_index.day.isin(retrieve_params_dict["day"])
        masks_combined = numpy.logical_and(masks_combined, param_mask)

    # Decode variables
    if retrieve_params.variables is not None:
        variables_asked = retrieve_params.variables
        #  Map to codes
        var2code = get_code_mapping(incobj)
        codes_asked = [var2code[v] for v in variables_asked if v in var2code]
        variables_file = incobj.variables["observed_variable"][:]
        variable_mask = numpy.isin(variables_file, codes_asked)
        masks_combined = numpy.logical_and(masks_combined, variable_mask)

    return masks_combined


def get_url_ncobj(fs: HTTPFileSystem, url: str) -> h5netcdf.File:
    """Open an URL as a netCDF file object with h5netcdf."""
    fobj = fs.open(url)
    logger.debug(f"Reading data from {url}.")
    # xarray won't read bytes object directly with netCDF4
    ncfile = h5netcdf.File(fobj, "r")
    return ncfile


def _check_data_size(
    entries: Sequence[Catalogue], retrieve_args: RetrieveArgs, size_limit: int
):
    """Estimate the size of the download and raise an error if it is too big."""
    estimated_data_size = estimate_data_size(entries, retrieve_args)
    logger.info(f"Estimated size of the data to retrieve is {estimated_data_size}B")
    if estimated_data_size > size_limit:
        raise SizeError("Requested data size is over the size limit.")


def _get_catalogue_entries(
    catalogue_repository: CatalogueRepository, retrieve_args: RetrieveArgs
) -> Sequence[Catalogue]:
    """Return the entries of the catalogue that contain the requested data."""
    entries = catalogue_repository.get_by_filters(
        retrieve_args.params.get_filter_arguments(dataset=retrieve_args.dataset),
        sort=True,
    )
    if len(entries) == 0:
        raise RuntimeError(
            "No entries found in catalogue for this parameter combination."
        )
    logger.info("Retrieved list of required partitions from the catalogue.")
    return entries


def _get_output_path(output_dir: Path, dataset: str, format: RetrieveFormat) -> Path:
    """Retuen the path of the output file."""
    if format == "csv":
        extension = ".csv"
    else:
        extension = ".nc"
    output_path = Path(output_dir, dataset + "_" + uuid.uuid4().hex + extension)
    return output_path


def _get_csv_header(
    retrieve_args: RetrieveArgs, cdm_lite_dataset: xarray.Dataset
) -> str:
    """Return the header of the CSV file."""
    template = """
########################################################################################
# This file contains data retrieved from the CDS https://cds.climate.copernicus.eu/cdsapp#!/dataset/{dataset}
# This is a C3S product under the following licences:
#     - licence-to-use-copernicus-products
#     - woudc-data-policy
# This is a CSV file following the CDS convention cdm-obs
# Data source: {dataset_source}
# Version:
# Time extent: {time_start} - {time_end}
# Geographic area (minlat/maxlat/minlon/maxlon): {area}
# Variables selected and units
{varstr}
########################################################################################
"""
    area = "{}/{}/{}/{}".format(
        cdm_lite_dataset.latitude.min().compute().item(),
        cdm_lite_dataset.latitude.max().compute().item(),
        cdm_lite_dataset.longitude.min().compute().item(),
        cdm_lite_dataset.longitude.max().compute().item(),
    )
    time_start = "{:%Y%m%d}".format(cdm_lite_dataset.report_timestamp.to_index()[0])
    time_end = "{:%Y%m%d}".format(cdm_lite_dataset.report_timestamp.to_index()[-1])
    vars_and_units = zip(
        numpy.unique(cdm_lite_dataset.observed_variable.to_index().str.decode("utf-8")),
        numpy.unique(cdm_lite_dataset.units.to_index().str.decode("utf-8")),
    )
    varstr = "\n".join([f"# {v} [{u}]" for v, u in vars_and_units])
    header_params = dict(
        dataset=retrieve_args.dataset,
        dataset_source=retrieve_args.params.dataset_source,
        area=area,
        time_start=time_start,
        time_end=time_end,
        varstr=varstr,
    )
    header = template.format(**header_params)
    return header
