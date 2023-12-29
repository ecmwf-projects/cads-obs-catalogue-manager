from itertools import product
from pathlib import Path

from cdsobs.config import CDSObsConfig
from cdsobs.constants import CONFIG_YML
from cdsobs.ingestion.api import EmptyBatchException
from cdsobs.ingestion.core import (
    DatasetMetadata,
    TimeBatch,
    TimeSpaceBatch,
    get_variables_from_service_definition,
)
from cdsobs.ingestion.serialize import batch_to_netcdf
from cdsobs.service_definition.api import get_service_definition


def main():
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    new_dataset_name = "insitu-observations-woudc-netcdfs"
    source = "OzoneSonde"
    start_year = 1969
    end_year = 1969
    config = CDSObsConfig.from_yaml(CONFIG_YML)
    output_dir = Path(Path(__file__).parent.parent, "data", "woudc_netcdfs")
    service_definition = get_service_definition(dataset_name)
    variables = get_variables_from_service_definition(service_definition, source)
    dataset_params = DatasetMetadata(dataset_name, source, variables)
    for year, month in product(range(start_year, end_year + 1), range(1, 13)):
        time_space_batch = TimeSpaceBatch(TimeBatch(year, month))
        try:
            batch_to_netcdf(
                dataset_params,
                output_dir,
                service_definition,
                config,
                time_space_batch,
                new_dataset_name,
            )
        except EmptyBatchException:
            print(f"{time_space_batch=} empty, ignoring")


if __name__ == "__main__":
    main()
