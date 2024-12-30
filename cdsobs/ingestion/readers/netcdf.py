from pathlib import Path

import pandas
import xarray

from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.api import EmptyBatchException
from cdsobs.ingestion.core import TimeSpaceBatch
from cdsobs.retrieve.filter_datasets import get_var_code_dict
from cdsobs.service_definition.service_definition_models import ServiceDefinition
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def read_flat_netcdfs(
    dataset_name: str,
    config: CDSObsConfig,
    service_definition: ServiceDefinition,
    source: str,
    time_space_batch: TimeSpaceBatch,
    input_dir: str,
) -> pandas.DataFrame:
    if time_space_batch.space_batch != "global":
        logger.warning("This reader does not support subsetting in space.")
    time_batch = time_space_batch.time_batch
    netcdf_path = Path(
        input_dir,
        f"{dataset_name}_{source}_{time_batch.year}_{time_batch.month:02d}.nc",
    )
    if netcdf_path.exists():
        data = xarray.open_dataset(netcdf_path).to_pandas()
    else:
        raise EmptyBatchException
    # Decode variable names
    code_dict = get_var_code_dict(config.cdm_tables_location)
    data["observed_variable"] = data["observed_variable"].map(code_dict)
    return data
