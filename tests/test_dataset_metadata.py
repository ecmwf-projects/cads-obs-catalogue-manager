from pathlib import Path
from pprint import pprint
from typing import Dict

import numpy
import yaml
from pydantic import BaseModel, field_validator, model_validator

from cdsobs.cdm.api import (
    get_cdm_variables,
    read_cdm_code_table,
    read_cdm_code_tables,
)
from cdsobs.cdm.tables import read_cdm_tables
from cdsobs.config import CDSObsConfig, DatasetConfig
from cdsobs.ingestion.core import DatasetMetadata
from cdsobs.service_definition.service_definition_models import (
    JoinIds,
    SpaceColumns,
    UnitChange,
)
from cdsobs.utils.types import StrNotBlank


class UncertaintyColumn(BaseModel, extra="forbid"):
    name: str
    main_variable: str
    units: str


UncertaintyType = str
UncertaintyColumnName = str


class QualityFlagColumn(BaseModel, extra="forbid"):
    name: str
    main_variable: str


class ProcessingLevelColumn(BaseModel, extra="forbid"):
    name: str
    main_variable: str


class MeltColumns(BaseModel, extra="forbid"):
    uncertainty: Dict[UncertaintyType, list[UncertaintyColumn]] | None = None
    quality_flag: Dict[str, list[QualityFlagColumn]] | None = None
    processing_level: Dict[str, list[ProcessingLevelColumn]] | None = None


class NewCdmMapping(BaseModel, extra="forbid"):
    rename: dict[StrNotBlank, StrNotBlank] | None = None
    add: dict[StrNotBlank, StrNotBlank | float | int | bool] | None = None
    unit_changes: dict[StrNotBlank, UnitChange] | None = None
    melt_columns: MeltColumns | None = None


class NewDescription(BaseModel, extra="forbid"):
    description: str
    dtype: str
    units: str | None = None

    @classmethod
    @field_validator("dtype")
    def valid_type(cls, dtype):
        if dtype is not None:
            try:
                numpy.dtype(dtype)
            except TypeError:
                raise AssertionError("Invalid data type provided")
        return dtype


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

    @model_validator(mode="after")
    def validate_main_variables(self):
        cdm_tables_location = Path("~/.cdsobs")
        cdm_variable_table = read_cdm_code_table(
            cdm_tables_location, "observed_variable"
        ).table
        all_cdm_variables = cdm_variable_table.name.str.replace(" ", "_").tolist()
        for main_var in self.main_variables:
            assert main_var in all_cdm_variables
            assert main_var in self.descriptions
        return self

    @model_validator(mode="after")
    def validate_rename(self):
        rename = self.cdm_mapping.rename
        melt_columns = self.cdm_mapping.melt_columns
        if melt_columns is not None:
            if melt_columns.uncertainty is not None:
                uncertainty_cols = [
                    ucol.name
                    for utype, ucols in melt_columns.uncertainty.items()
                    for ucol in ucols
                ]
            else:
                uncertainty_cols = []
            if melt_columns.quality_flag is not None:
                quality_flag_cols = [
                    qacol.name
                    for qatype, qacols in melt_columns.quality_flag.items()
                    for qacol in qacols
                ]
            else:
                quality_flag_cols = []
            if melt_columns.processing_level is not None:
                processing_level_cols = [
                    plcol.name
                    for pltype, plcols in melt_columns.processing_level.items()
                    for plcol in plcols
                ]
            else:
                processing_level_cols = []
        else:
            uncertainty_cols = quality_flag_cols = processing_level_cols = []

        aux_var_cols = uncertainty_cols + quality_flag_cols + processing_level_cols

        if rename is not None:
            for raw_name, new_name in rename.items():
                assert new_name in list(self.descriptions) or new_name in aux_var_cols
        return self

    @model_validator(mode="after")
    def validate_descriptions(self):
        main_variables = self.main_variables
        descriptions_no_main_vars = [
            d for d in self.descriptions if d not in main_variables
        ]
        # TODO: This should not be hardcoded
        cdm_tables_location = Path("~/.cdsobs")
        cdm_tables = read_cdm_tables(cdm_tables_location)
        cdm_variables = get_cdm_variables(cdm_tables)
        fields_missing = sorted(set(descriptions_no_main_vars) - set(cdm_variables))
        if len(fields_missing) > 0:
            raise AssertionError(
                "The following fields in descriptions do not have"
                f"CDM compliant names {fields_missing}"
            )
        return self


class NewServiceDefinition(BaseModel, extra="forbid"):
    global_attributes: dict
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
        "../cdsobs/data/insitu-observations-near-surface-temperature-us-climate-reference-network/service_definition_new.yml"
    )
    new_sc_dict = yaml.safe_load(sc_path.read_text())
    # new_sc_dict["sources"]["GRUAN"]["main_variables"].append("wrong_variable")
    # del new_sc_dict["sources"]["GRUAN"]["descriptions"]["air_temperature"]
    service_definition = NewServiceDefinition(**new_sc_dict)
    actual = new_get_dataset_metadata(
        test_config, dataset_config, service_definition, "uscrn_daily"
    )
    pprint(actual)
