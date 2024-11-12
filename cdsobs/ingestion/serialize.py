from pathlib import Path
from typing import cast

import h5netcdf
import netCDF4
import numpy
import pandas

from cdsobs import constants
from cdsobs.cdm.api import CdmDataset, to_cdm_dataset
from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.api import read_batch_data
from cdsobs.ingestion.core import (
    DatasetMetadata,
    DatasetPartition,
    FileParams,
    PartitionParams,
    SerializedPartition,
    TimeSpaceBatch,
)
from cdsobs.netcdf import (
    get_encoding_with_compression,
    get_encoding_with_compression_xarray,
)
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.storage import StorageClient
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.types import ByteSize
from cdsobs.utils.utils import compute_hash, datetime_to_seconds, get_file_size

logger = get_logger(__name__)


def _get_default_fillvalue(dtype):
    kind = dtype.kind
    if kind in ["u", "i", "f"]:
        size = numpy.dtype(dtype).itemsize
        fillvalue = netCDF4.default_fillvals[f"{kind}{size}"]
    else:
        fillvalue = netCDF4.default_fillvals["S1"]
    return fillvalue


def write_pandas_to_netcdf(
    filepath: Path,
    input_data: pandas.DataFrame,
    encoding: dict,
    var_selection: list[str] | None = None,
    attrs: dict | None = None,
):
    """Write each variable to netcdf using h5netcdf.

    input_data is a pandas dataframe with one column, one for each variable
    fbencodings is the encodings of variable types,
    e.g. {'observations_id': { 'compression': 'gzip' } }.
    """
    oncobj = h5netcdf.File(filepath, "w")

    if var_selection is None:
        var_selection = input_data.columns

    sdict = {}
    oncobj.dimensions["observation_id"] = len(input_data)
    dimensions_base: tuple[str, ...] = ("observation_id",)
    # To avoid too large chunks, we set the max size to 6e5
    MAX_CHUNKSIZE = int(3e5)
    max_chunksize = min(MAX_CHUNKSIZE, len(input_data))

    for v in var_selection:
        vardata = input_data[v]
        var_encoding = encoding[v] if v in encoding else {}
        fillvalue = _get_default_fillvalue(vardata.values.dtype)
        if str(vardata.values.dtype) not in ["string", "object"]:
            # This is needed so
            ovar = oncobj.create_variable(
                v,
                data=vardata.values,
                dimensions=dimensions_base,
                chunks=(max_chunksize,),
                fillvalue=fillvalue,
                **var_encoding,
            )
        else:
            try:
                vardata = vardata.astype("bytes").values
            except UnicodeError:
                # Need this in some cases for the non ascii characters to be well
                # handled. This should be fixed before, not sure why it happens here.
                vardata = vardata.str.encode("UTF-8").astype("bytes").values
            slen = vardata.dtype.itemsize
            sdict[v] = slen
            strdim = "string_" + v
            oncobj.dimensions[strdim] = slen
            dimensions = dimensions_base + (strdim,)
            max_chunksize_str = (
                max_chunksize // slen
                if max_chunksize == MAX_CHUNKSIZE
                else max_chunksize
            )
            var_encoding["dtype"] = "S1"
            ovar = oncobj.create_variable(
                v,
                data=vardata.view("S1").reshape(vardata.shape[0], slen),
                chunks=(max_chunksize_str, slen),
                dimensions=dimensions,
                fillvalue=fillvalue,
                **var_encoding,
            )
        if attrs is not None and v in attrs:
            ovar.attrs.update(attrs[v])

    oncobj.sync()
    oncobj.close()


def to_netcdf(
    cdm_dataset: CdmDataset, tempdir: Path, encode_variables: bool = True
) -> Path:
    """Save a partition in netCDF format."""
    filename = get_partition_filename(
        cdm_dataset.dataset_params, cdm_dataset.partition_params
    )
    output_path = Path(tempdir, filename)
    logger.debug(f"Saving partition to {output_path}")
    # Encode strings, otherwise it will fail when non-ascii chars are present.
    encoding = get_encoding_with_compression(
        cdm_dataset.dataset, string_transform="str_to_char"
    )
    # Attributes for the variables
    attrs: dict[str, dict[str, str | list]] = dict()
    # Encode variable names as integer
    if encode_variables:
        logger.info("Encoding observed variables using the CDM variable codes.")
        code_table = cdm_dataset.dataset_params.cdm_code_tables[
            "observed_variable"
        ].table
        # strip to remove extra spaces
        var2code = get_var2code(code_table)
        encoded_data = (
            cdm_dataset.dataset["observed_variable"]
            .str.encode("UTF-8")
            .map(var2code)
            .astype("uint8")
        )
        cdm_dataset.dataset["observed_variable"] = encoded_data
        codes_in_data = encoded_data.unique()
        var2code_subset = {
            var.decode("ascii"): code
            for var, code in var2code.items()
            if code in codes_in_data
        }
        encoding["observed_variable"]["dtype"] = encoded_data.dtype
        attrs["observed_variable"] = dict(
            labels=list(var2code_subset), codes=list(var2code_subset.values())
        )
    # Encode dates
    for varname in cdm_dataset.dataset.columns:
        var_series = cdm_dataset.dataset[varname]
        if var_series.dtype.kind == "M":
            cdm_dataset.dataset[varname] = datetime_to_seconds(var_series)
    attrs["report_timestamp"] = dict(units=constants.TIME_UNITS)
    # Write to netCDF
    write_pandas_to_netcdf(
        output_path, cdm_dataset.dataset.reset_index(), encoding=encoding, attrs=attrs
    )
    return output_path


def get_var2code(code_table):
    code_dict = pandas.Series(
        index=code_table["name"].str.strip().str.replace(" ", "_").str.encode("ascii"),
        data=code_table.index,
    ).to_dict()
    return code_dict


def to_storage(
    storage_client: StorageClient, dataset_name: str, output_path: Path
) -> str:
    """Upload file to storage."""
    bucket_name = storage_client.get_bucket_name(dataset_name)
    storage_client.create_directory(bucket_name)
    # confusing.
    return storage_client.upload_file(bucket_name, output_path.name, output_path)


def serialize_partition(partition: DatasetPartition, odir: Path) -> SerializedPartition:
    """
    Normalize the data and save it to a netCDF file.

    Parameters
    ----------
    partition :
      Input partition. Has the data loaded in memory as a pandas.DataFrame
    odir :
      Directory where to save the output file.

    Returns
    -------
    An SerializedPartition object with the partition and file parameters.
    """
    # Get in memory representation of the CDM
    cdm_dataset = to_cdm_dataset(partition)
    # Save to netcdf
    logger.info(f"Writing partition to {odir}")
    temp_output_path = to_netcdf(cdm_dataset, odir)
    # Builds an object with extra information about the file
    logger.info("Getting file size and checksum.")
    file_params = get_file_params(temp_output_path, cdm_dataset)
    return SerializedPartition(
        file_params,
        partition.partition_params,
        partition.dataset_metadata,
        partition.constraints,
    )


def get_file_params(file_path: Path, cdm_dataset: CdmDataset) -> FileParams:
    """
    Get useful information about the file (and the data size).

    Parameters
    ----------
    file_path:
      path where the dataframe is stored
    cdm_dataset:
      Dict with a xarray dataset for each CDM group

    Returns
    -------
    FileParams
    """
    file_size: ByteSize = get_file_size(file_path)
    data_size = cdm_dataset.dataset.memory_usage(deep=True).sum()
    total_data_size = cast(ByteSize, data_size)
    file_checksum = compute_hash(file_path)
    return FileParams(file_size, total_data_size, file_checksum, file_path)


def get_partition_filename(
    dataset_params: DatasetMetadata, partition_params: PartitionParams
) -> str:
    """Build the file name following a readable descriptive pattern."""
    dp = dataset_params
    pp = partition_params
    if pp.time_batch.month is None:
        time_str = pp.time_coverage_start.strftime("%Y")
    else:
        time_str = pp.time_coverage_start.strftime("%Y%m")
    filename = (
        f"{dp.name}_{dp.dataset_source}_{time_str}_"
        f"{pp.latitude_coverage_start}_{pp.longitude_coverage_start}.nc"
    )
    return filename


def batch_to_netcdf(
    dataset_params: DatasetMetadata,
    output_dir: Path,
    service_definition: ServiceDefinition,
    test_config: CDSObsConfig,
    time_space_batch: TimeSpaceBatch,
    new_dataset_name: str,
) -> Path:
    """Write data from a year and month to a netCDF file.

    Instead of passing the batch data to partition and uploading into the storage, this
    function writes it to a netCDF file as a single big table. Currently, this is used
    for generating data for tests only.
    """
    time_batch = time_space_batch.time_batch
    homogenised_data = read_batch_data(
        test_config, dataset_params, service_definition, time_space_batch
    )
    source = dataset_params.dataset_source
    output_path = Path(
        output_dir,
        f"{new_dataset_name}_{source}_{time_batch.year}_{time_batch.month:02d}.nc",
    )
    for field in homogenised_data:
        if homogenised_data[field].dtype == "string":
            homogenised_data[field] = homogenised_data[field].str.encode("UTF-8")
    homogenised_data_xr = homogenised_data.to_xarray()
    if service_definition.global_attributes is not None:
        homogenised_data.attrs = {
            **homogenised_data.attrs,
            **service_definition.global_attributes,
        }
    encoding = get_encoding_with_compression_xarray(
        homogenised_data_xr, string_transform="str_to_char"
    )
    logger.info(f"Writing de-normalized and CDM mapped data to {output_path}")
    homogenised_data_xr.to_netcdf(
        output_path, encoding=encoding, engine="netcdf4", format="NETCDF4"
    )
    return output_path
