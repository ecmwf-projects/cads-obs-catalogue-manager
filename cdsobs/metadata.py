from cdsobs.cdm.api import read_cdm_code_tables
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.config import CDSObsConfig, DatasetConfig
from cdsobs.ingestion.core import (
    DatasetMetadata,
)
from cdsobs.service_definition.service_definition_models import ServiceDefinition


def get_dataset_metadata(
    config: CDSObsConfig,
    dataset_config: DatasetConfig,
    service_definition: ServiceDefinition,
    source: str,
) -> DatasetMetadata:
    # Handle the main variables
    variables = service_definition.sources[source].main_variables
    # Read CDM tables
    cdm_tables = read_cdm_tables(
        config.cdm_tables_location, dataset_config.available_cdm_tables
    )
    cdm_code_tables = read_cdm_code_tables(config.cdm_tables_location)
    # Get the name of the space columns
    if service_definition.space_columns is not None:
        space_columns = service_definition.space_columns
    else:
        space_columns = service_definition.sources[source].space_columns  # type: ignore
    # Pack dataset metadata into an object to carry on.
    dataset_metadata = DatasetMetadata(
        dataset_config.name,
        source,
        variables,
        cdm_tables,
        cdm_code_tables,
        space_columns,
    )
    return dataset_metadata
