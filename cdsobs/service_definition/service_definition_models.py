"""Servide definition models."""
from typing import Dict

import numpy
from pydantic import BaseModel, field_validator, model_validator

from cdsobs.utils.types import StrNotBlank


class JoinIds(BaseModel):
    header: StrNotBlank
    data: StrNotBlank


class SpaceColumns(BaseModel):
    x: StrNotBlank
    y: StrNotBlank
    z: StrNotBlank | None = None


class UnitChange(BaseModel):
    names: dict[StrNotBlank, StrNotBlank]
    offset: float
    scale: float


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


class CdmMapping(BaseModel, extra="forbid"):
    rename: dict[StrNotBlank, StrNotBlank] | None = None
    add: dict[StrNotBlank, StrNotBlank | float | int | bool] | None = None
    unit_changes: dict[StrNotBlank, UnitChange] | None = None
    melt_columns: MeltColumns | None = None


class Description(BaseModel, extra="forbid"):
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


class SourceDefinition(BaseModel, extra="forbid"):
    main_variables: list[str]
    cdm_mapping: CdmMapping
    header_columns: list[str] | None = None
    header_table: StrNotBlank | None = None
    data_table: StrNotBlank
    join_ids: JoinIds | None = None
    space_columns: SpaceColumns | None = None
    descriptions: dict[str, Description]
    mandatory_columns: list[str]
    order_by: list[str] | None = None

    def is_multitable(self):
        return self.header_table is not None

    def get_raw_header_columns(self):
        return [
            k for k, v in self.cdm_mapping.rename.items() if v in self.header_columns
        ]

    def get_raw_mandatory_columns(self):
        return [
            k for k, v in self.cdm_mapping.rename.items() if v in self.mandatory_columns
        ]

    @model_validator(mode="after")
    def validate_main_variables(self):
        for main_var in self.main_variables:
            if main_var not in self.descriptions:
                raise AssertionError(f"{main_var} not found in descriptions.")
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
                if not (
                    new_name in list(self.descriptions) or new_name in aux_var_cols
                ):
                    raise AssertionError(
                        f"{new_name} is in rename but not in descriptions or melt_variables"
                    )
        return self


class ServiceDefinition(BaseModel, extra="forbid"):
    global_attributes: dict
    space_columns: SpaceColumns | None = None
    sources: dict[str, SourceDefinition]
