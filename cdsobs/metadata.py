from cdsobs.cdm.api import read_cdm_code_tables
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.ingestion.core import (
    DatasetMetadata,
    IngestionRunParams,
)


def get_dataset_metadata(run_params: IngestionRunParams) -> DatasetMetadata:
    """Instance an object containing information about the dataset.

    Instance an object containing information about the dataset, its
    configuration and the CDM tables used.
    """
    dataset_name = run_params.dataset_name
    source = run_params.source
    config = run_params.config
    service_definition = run_params.service_definition
    dataset_config = config.get_dataset(dataset_name)
    source_definition = service_definition.sources[source]
    # Handle the main variables
    variables = source_definition.main_variables
    # Read CDM tables
    cdm_tables = read_cdm_tables(
        config.cdm_tables_location, dataset_config.available_cdm_tables
    )
    cdm_code_tables = read_cdm_code_tables(config.cdm_tables_location)
    # Get the name of the space columns
    if service_definition.space_columns is not None:
        space_columns = service_definition.space_columns
    else:
        space_columns = source_definition.space_columns  # type: ignore
    # Pack dataset metadata into an object to carry on.
    dataset_metadata = DatasetMetadata(
        dataset_name,
        source,
        variables,
        cdm_tables,
        cdm_code_tables,
        space_columns,
        run_params.version,
    )
    return dataset_metadata
