from pathlib import Path
from pprint import pprint
from typing import Dict

import yaml
from pydantic import BaseModel

from cdsobs.cdm.api import read_cdm_code_tables
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.config import CDSObsConfig, DatasetConfig
from cdsobs.ingestion.core import DatasetMetadata
from cdsobs.service_definition.service_definition_models import (
    JoinIds,
    SpaceColumns,
    UnitChange,
)
from cdsobs.utils.types import NotEmptyUniqueStrList, StrNotBlank


class UncertaintyColumn(BaseModel, extra="forbid"):
    main_variable: str
    units: str


UncertaintyType = str
UncertaintyColumnName = str


class MeltColumns(BaseModel, extra="forbid"):
    uncertainty: Dict[
        UncertaintyType, list[dict[UncertaintyColumnName, UncertaintyColumn]]
    ] | None


class NewCdmMapping(BaseModel, extra="forbid"):
    rename: dict[StrNotBlank, StrNotBlank] | None = None
    add: dict[StrNotBlank, StrNotBlank | float | int | bool] | None = None
    unit_changes: dict[StrNotBlank, UnitChange] | None = None
    melt_columns: MeltColumns | None = None


class NewDescription(BaseModel, extra="forbid"):
    description: str
    dtype: str
    units: str | None = None


class NewSourceDefinition(BaseModel, extra="forbid"):
    main_variables: list[str]
    cdm_mapping: NewCdmMapping
    header_columns: list[str] | None = None
    header_table: StrNotBlank | None = None
    data_table: StrNotBlank
    join_ids: JoinIds | None = None
    space_columns: SpaceColumns | None = None
    descriptions: dict[str, NewDescription]
    mandatory_columns: list[str]
    _errors: bool = False


class NewServiceDefinition(BaseModel, extra="forbid"):
    global_attributes: dict
    out_columns_order: NotEmptyUniqueStrList | None = None
    products_hierarchy: NotEmptyUniqueStrList | None = None
    space_columns: SpaceColumns | None = None
    sources: dict[str, NewSourceDefinition]


def new_get_dataset_metadata(
    config: CDSObsConfig,
    dataset_config: DatasetConfig,
    service_definition: NewServiceDefinition,
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


def test_get_dataset_metadata(test_config):
    dataset = "insitu-observations-gruan-reference-network"
    dataset_config = test_config.get_dataset(dataset)
    sc_path = Path(
        "../cdsobs/data/insitu-observations-gruan-reference-network/service_definition_simplified.yml"
    )
    new_sc_dict = yaml.safe_load(sc_path.read_text())
    service_definition = NewServiceDefinition(**new_sc_dict)
    actual = new_get_dataset_metadata(
        test_config, dataset_config, service_definition, "GRUAN"
    )
    pprint(actual)
