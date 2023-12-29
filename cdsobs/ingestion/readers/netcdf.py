from pathlib import Path
from typing import Tuple

import pandas
import xarray

from cdsobs.config import CDSObsConfig
from cdsobs.ingestion.api import EmptyBatchException
from cdsobs.ingestion.core import TimeSpaceBatch
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
) -> Tuple[pandas.DataFrame, pandas.Series]:
    if time_space_batch.space_batch != "global":
        logger.warning("This reader does not support subsetting in space.")
    time_batch = time_space_batch.time_batch
    netcdf_path = Path(
        input_dir,
        f"{dataset_name}_{source}_{time_batch.year}_{time_batch.month:02d}.nc",
    )
    if netcdf_path.exists():
        data = xarray.open_dataset(netcdf_path).to_pandas()
        data_types = data.dtypes
    else:
        raise EmptyBatchException
    return data, data_types  # type: ignore
