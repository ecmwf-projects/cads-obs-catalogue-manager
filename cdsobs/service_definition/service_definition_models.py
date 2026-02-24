from pathlib import Path
from typing import Dict, Literal

import numpy
from pydantic import BaseModel, Field, field_validator, model_validator

from cdsobs.cdm.tables import DEFAULT_CDM_TABLES_TO_USE
from cdsobs.utils.types import (
    LatTileSize,
    LonTileSize,
    StrNotBlank,
    TimeTileSize,
    get_year_tile_size,
)

AvailableReaders = Literal[
    "cdsobs.ingestion.readers.sql.read_header_and_data_tables",
    "cdsobs.ingestion.readers.sql.read_singletable_data",
    "cdsobs.ingestion.readers.cuon.read_cuon_netcdfs",
    "cdsobs.ingestion.readers.cuon_np.read_cuon_netcdfs",
    "cdsobs.ingestion.readers.netcdf.read_flat_netcdfs",
    "cdsobs.ingestion.readers.csv.read_flat_csvs",
    "cdsobs.ingestion.readers.parquet.read_flat_parquet",
]


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


MANDATORY_COLUMNS = [
    "station_name",
    "primary_station_id",
    "report_id",
    "observation_id",
    "longitude",
    "latitude",
    "report_timestamp",
    "height_of_station_above_sea_level",
    "report_meaning_of_time_stamp",
    "report_duration",
    "observed_variable",
    "units",
    "observation_value",
]


class SourceDefinition(BaseModel, extra="forbid"):
    main_variables: list[str]
    cdm_mapping: CdmMapping
    header_columns: list[str] | None = None
    header_table: StrNotBlank | None = None
    data_table: StrNotBlank
    join_ids: JoinIds | None = None
    space_columns: SpaceColumns | None = None
    descriptions: dict[str, Description]
    mandatory_columns: list[str] = MANDATORY_COLUMNS
    columns_to_drop: list[str] = Field(default_factory=list)
    order_by: list[str] | None = None

    def is_multitable(self) -> bool:
        return self.header_table is not None

    def get_raw_header_columns(self) -> list[str]:
        if self.cdm_mapping.rename is None:
            return []
        return [
            k
            for k, v in self.cdm_mapping.rename.items()
            if (self.header_columns is not None and v in self.header_columns)
        ]

    def get_raw_mandatory_columns(self) -> list[str]:
        if self.cdm_mapping.rename is None:
            return []
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
    name: str
    global_attributes: dict
    lat_tile_size: LatTileSize | dict[str, LatTileSize | dict[str, LatTileSize]]
    lon_tile_size: LonTileSize | dict[str, LonTileSize | dict[str, LonTileSize]]
    time_tile_size: TimeTileSize = "month"
    reader: AvailableReaders | dict[
        str, AvailableReaders
    ] = "cdsobs.ingestion.readers.sql.read_header_and_data_tables"
    available_cdm_tables: list[str] = DEFAULT_CDM_TABLES_TO_USE
    reader_extra_args: dict[str, str] | None = None
    ingestion_db: str = "main"
    read_with_spatial_batches: bool = False
    disabled_fields: list[str] | dict[str, list[str]] = Field(default_factory=list)
    space_columns: SpaceColumns | None = None
    sources: dict[str, SourceDefinition]
    path: Path | None = None

    def get_tile_size(
        self, kind: Literal["lat", "lon"], source: str, year: int
    ) -> LatTileSize | LonTileSize:
        """
        Handle the possible dependency of the tile sizes on year and source.

        Parameters
        ----------
        kind:
          Wether we want the tile size of the longitudes (lon) or latitudes (lat).
        source:
          The dataset source as defined in the service definition file.
        year:
          The year of data that is being processed.

        """
        tile_size_config: LatTileSize | LonTileSize = getattr(self, f"{kind}_tile_size")

        if isinstance(tile_size_config, int):
            # Tile size is always the same
            result = tile_size_config
        else:
            # Tile size depends on the source, the year or both
            if source in tile_size_config:
                # Tile size depends on the source
                source_tile_size = tile_size_config[source]
                if isinstance(source_tile_size, int):
                    # Tile size depends on source only
                    result = source_tile_size
                else:
                    # Tile size depends on the source and year
                    result = get_year_tile_size(source_tile_size, year)
            else:
                # Tile size depends on the year
                result = get_year_tile_size(tile_size_config, year)

        return result
