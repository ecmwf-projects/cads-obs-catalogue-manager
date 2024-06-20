import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple

import fsspec
import h5netcdf
import numpy
import pandas
import sqlalchemy as sa
from fsspec.implementations.http import HTTPFileSystem

from cdsobs.cdm.lite import auxiliary_variable_names
from cdsobs.cli._catalogue_explorer import stats_summary
from cdsobs.constraints import iterative_ordering
from cdsobs.ingestion.core import (
    get_aux_vars_from_service_definition,
    get_variables_from_sc_group,
)
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.retrieve.retrieve_services import merged_constraints_table
from cdsobs.service_definition.api import get_service_definition
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def get_forms_jsons(
    dataset: str,
    catalogue_repository: CatalogueRepository,
    output_path: Path,
    storage_client: S3Client,
    upload_to_storage: bool = False,
    get_stations_file: bool = False,
) -> Tuple[Path, ...]:
    """Save the geco output json files in a folder."""
    # widgets.json
    session = catalogue_repository.session
    widgets_file = get_widgets_json(session, output_path, dataset)
    # constraints
    constraints_file = get_constraints_json(session, output_path, dataset)
    # variables
    variables_file = get_variables_json(dataset, output_path)
    json_files: Tuple[Path, ...] = (widgets_file, constraints_file, variables_file)
    # stations file is optional and not computed by default
    if get_stations_file:
        stations_file = get_station_summary(
            dataset, session, storage_client.public_url_base, output_path
        )
        json_files += (stations_file,)
    if upload_to_storage:
        if storage_client is None:
            raise RuntimeError("Storage client must be set if upload is true.")
        bucket = storage_client.get_bucket_name(dataset)
        for json_file in json_files:
            logger.info(f"Uploading {json_file} to the storage.")
            storage_client.upload_file(bucket, json_file.name, json_file)
    return json_files


def get_variables_json(dataset: str, output_path: Path) -> Path:
    """JSON file with the variables and their metadata."""
    service_definition = get_service_definition(dataset)
    variables_json_content = {}
    for source_name, source in service_definition.sources.items():
        # We also pick up auxiliary variables here.
        descriptions = {k: v.model_dump() for k, v in source.descriptions.items()}
        variables_json_content[source_name] = descriptions
    output_file_path = Path(output_path, "variables.json")
    logger.info(f"Writing {output_file_path}")
    with output_file_path.open("w") as vof:
        json.dump(variables_json_content, vof, indent=True, sort_keys=True)
    return output_file_path


def get_constraints_json(session, output_path: Path, dataset) -> Path:
    """JSON file with the constraints in compressed form."""
    # This is probably slow, can it be improved?
    catalogue_entries = get_catalogue_entries_stream(session, dataset)
    merged_constraints = merged_constraints_table(catalogue_entries)
    # Remove the stations here to avoid using too much memory, set to true the constraints
    # if there is data for any of the stations
    merged_constraints = merged_constraints.drop("stations", axis=1)
    merged_constraints = merged_constraints.groupby(["time", "source"]).any()
    logger.info("Computing flat constraints.")
    # This returns a bool series where True are available combinations
    flat_constraints = merged_constraints.stack()
    # Filter out the False rows
    flat_constraints = flat_constraints.loc[flat_constraints]
    # Turn into a dataframe and remove the bools column (named 0 by default)
    flat_constraints = (
        flat_constraints.reset_index()
        .rename(dict(level_2="variables"), axis=1)
        .drop(0, axis=1)
        .rename(dict(source="dataset_source"), axis=1)
    )
    flat_constraints["variables"] = flat_constraints["variables"].astype(str)
    times = flat_constraints.time.astype("datetime64[s]")
    flat_constraints["year"] = times.dt.year.astype("str")
    flat_constraints["month"] = times.dt.month.astype("str").str.rjust(2, "0")
    flat_constraints["day"] = times.dt.day.astype("str").str.rjust(2, "0")
    flat_constraints = flat_constraints.drop("time", axis=1)
    # Auxiliary variables are metadata, but they are still added as variables here for
    # backwards compatibility
    flat_constraints = _add_auxiliary_variables(dataset, flat_constraints)
    # Compress constraints
    logger.info("Computing compressed constraints")
    compressed_constraints = iterative_ordering(
        flat_constraints, flat_constraints.columns
    )
    constraints_path = Path(output_path, "constraints.json")
    logger.info(f"Writing {constraints_path}")
    with constraints_path.open("w") as cof:
        compressed_constraints.to_json(cof, orient="records")
    return constraints_path


def _add_auxiliary_variables(
    dataset: str, flat_constraints: pandas.DataFrame
) -> pandas.DataFrame:
    """Look for auxiliary variables and adds them to the constraints.

    Looks for auxiliary variables in the Service definition file and adds them to the
    flat constraints. These are dummy variables that will be discarded by the adaptor
    and the metadata fields will be included.
    """
    service_definition = get_service_definition(dataset)
    aux_flat_constraints_dicts = []
    # When we find an aux variable related to the main variable we add it
    for row in flat_constraints.itertuples(index=False):
        source_definition = service_definition.sources[row.dataset_source]
        var_description = source_definition.descriptions[row.variables]
        for auxvar in auxiliary_variable_names:
            if hasattr(var_description, auxvar):
                auxvar_original_name = getattr(var_description, auxvar)
                rename_dict = source_definition.cdm_mapping.rename
                if rename_dict is not None and auxvar_original_name in rename_dict:
                    auxvar_final_name = rename_dict[auxvar_original_name]
                else:
                    auxvar_final_name = auxvar_original_name
                newrow = row._replace(variables=auxvar_final_name)
                aux_flat_constraints_dicts.append(newrow)
    aux_flat_constraints = pandas.DataFrame(aux_flat_constraints_dicts)
    flat_constraints = pandas.concat([flat_constraints, aux_flat_constraints])
    return flat_constraints


def get_widgets_json(session, output_path: Path, dataset: str) -> Path:
    """JSON file with the variables and their metadata."""
    catalogue_entries = get_catalogue_entries_stream(session, dataset)
    service_definition = get_service_definition(dataset)
    variables = get_all_variables_in_products(service_definition)
    summary = stats_summary(catalogue_entries)
    widgets_json_content = dict()
    widgets_json_content["variables"] = variables
    time_coverage_start, time_coverage_end = summary["total time coverage"]
    start_year = int(time_coverage_start[0:4])
    end_year = int(time_coverage_end[0:4])
    end_datetime = datetime.fromisoformat(time_coverage_end)
    if (end_datetime.month == 1) and (end_datetime.day == 1):
        # As the intervals are open at the right, there is no really data for the last
        # year! We still check that the coverage ends the first of Jan just to be sure
        # in case something changes in the future.
        end_year -= 1
    widgets_json_content["stations"] = summary["available stations"]
    widgets_json_content["dataset_source"] = summary["available dataset sources"]
    widgets_json_content["year"] = _to_str_list(range(start_year, end_year + 1))
    widgets_json_content["month"] = _to_str_list(range(1, 13), 2)
    widgets_json_content["day"] = _to_str_list(range(1, 32), 2)
    widgets_output_path = Path(output_path, "widgets.json")
    logger.info(f"Writing {widgets_output_path}")
    with widgets_output_path.open("w") as wof:
        json.dump(widgets_json_content, wof, indent=4, sort_keys=True)
    return widgets_output_path


def get_all_variables(service_definition):
    variables = []
    # Includes also auxiliary variables
    for source_name, source in service_definition.sources.items():
        variables.extend([variable for variable in source.descriptions])
    return variables


def get_all_variables_in_products(service_definition):
    variables = []
    # Includes also auxiliary variables
    for source_name, source in service_definition.sources.items():
        for product in source.products:
            variables.extend(get_variables_from_sc_group(source, product.group_name))
    return list(set(variables))


def get_all_aux_variables(service_definition):
    variables = []
    # Includes also auxiliary variables
    for source_name, source in service_definition.sources.items():
        aux_variables = get_aux_vars_from_service_definition(
            service_definition, source_name
        )
        variables.extend(aux_variables)
    return list(set(variables))


def get_station_summary(
    dataset: str, session, storage_url: str, output_path: Path
) -> Path:
    """Iterate over the input files to get the stations and their metadata."""
    stations_output_path = Path(output_path, "stations.json")
    fs = fsspec.filesystem("https")

    df_list = []

    service_definition = get_service_definition(dataset)
    for source in service_definition.sources:
        if service_definition.space_columns is None:
            space_columns = service_definition.sources[source].space_columns
        elif service_definition.space_columns is not None:
            space_columns = service_definition.space_columns
        else:
            raise RuntimeError("Space columns not found in service definition.")
        lonname, latname = space_columns.x, space_columns.y  # type: ignore[union-attr]
        catalogue_entries = get_catalogue_entries_stream(session, dataset)
        object_urls = [
            f"{storage_url}/{entry.asset}"
            for partition in catalogue_entries.partitions()
            for entry in partition
            if entry.dataset_source == source
        ]
        for url in object_urls:
            logger.info(f"Reading station data from {url}")
            with _get_url_ncobj(fs, url) as incobj:
                stationvar = incobj.variables["primary_station_id"]
                field_len, strlen = stationvar.shape
                stations_in_partition = (
                    stationvar[:].view(f"S{strlen}").reshape(field_len)
                )
                station_lons = incobj.variables[lonname][:]
                station_lats = incobj.variables[latname][:]
                url_df = pandas.DataFrame(
                    data=numpy.vstack([station_lons, station_lats]).T,
                    index=stations_in_partition,
                    columns=["longitude", "latitude"],
                    copy=False,
                )
                url_df.index = url_df.index.astype("str")
                url_df.index.name = "station_id"
                url_df = url_df.drop_duplicates()
                url_df["source"] = source
                df_list.append(url_df)

    logger.info(f"Writing {stations_output_path}")
    stations_df = pandas.concat(df_list).drop_duplicates().reset_index()
    stations_df.to_json(stations_output_path, orient="records")
    return stations_output_path


def get_catalogue_entries_stream(
    session, dataset: str
) -> sa.engine.result.ScalarResult:
    catalogue_entries = session.scalars(
        sa.select(Catalogue)
        .filter(Catalogue.dataset == dataset)
        .execution_options(yield_per=50)
    )
    return catalogue_entries


def _to_str_list(iterable: Iterable, min_chars: int | None = None) -> list[str]:
    """
    Return list from iterable coercing the items as strings.

    min_chars optional argument can be used to make all the strings to have the same
    length prepending zeroes.
    """
    str_iterable = (str(i) for i in iterable)
    if min_chars is None:
        result = str_iterable
    else:
        result = (str(i).rjust(min_chars, "0") for i in iterable)
    return list(result)


def _get_url_ncobj(fs: HTTPFileSystem, url: str) -> h5netcdf.File:
    """Open an URL as a netCDF file object with h5netcdf."""
    fobj = fs.open(url)
    logger.debug(f"Reading data from {url}.")
    # xarray won't read bytes object directly with netCDF4
    ncfile = h5netcdf.File(fobj, "r")
    return ncfile
