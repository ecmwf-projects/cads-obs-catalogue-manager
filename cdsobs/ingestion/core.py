import dataclasses
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, Protocol, Tuple, cast

import pandas
from dateutil.relativedelta import relativedelta
from pydantic_extra_types.semantic_version import SemanticVersion

from cdsobs.cdm.code_tables import CDMCodeTables
from cdsobs.cdm.tables import CDMTables
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.schemas.catalogue import CatalogueSchema
from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema
from cdsobs.service_definition.service_definition_models import (
    ServiceDefinition,
    SpaceColumns,
)
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.types import BoundedLat, BoundedLon, ByteSize

logger = get_logger(__name__)


@dataclass
class TimeBatch:
    """Represents the time interval defined by a year and optionally a month.

    If only a year is given, the time batch will span the whole year. If a year and a
    month are given, the time batch will only span that month.
    """

    year: int
    month: int | None = None

    def get_time_coverage(self) -> Tuple[datetime, datetime]:
        """Return the time batch as a [start_date, end_date.)."""
        if self.month is None:
            # Years always start in Jan
            start_date = datetime(year=self.year, month=1, day=1)
            delta = relativedelta(years=1)
        else:
            start_date = datetime(year=self.year, month=self.month, day=1)
            delta = relativedelta(months=1)
        return start_date, start_date + delta


@dataclass
class SpaceBatch:
    longitude_start: int
    longitude_end: int
    latitude_start: int
    latitude_end: int


@dataclass
class TimeSpaceBatch:
    time_batch: TimeBatch
    space_batch: SpaceBatch | Literal["global"] = "global"

    def get_time_coverage(self):
        return self.time_batch.get_time_coverage()

    def get_spatial_coverage(self):
        if self.space_batch == "global":
            return -180, 180, -90, 90
        else:
            return dataclasses.astuple(self.space_batch)


class MissingMandatoryColumns(KeyError):
    pass


@dataclass
class DatasetMetadata:
    """Dataset metadata to carry on."""

    name: str
    dataset_source: str
    variables: list[str]
    cdm_tables: CDMTables
    cdm_code_tables: CDMCodeTables
    space_columns: SpaceColumns
    version: str


def get_variables_from_service_definition(
    service_definition: ServiceDefinition, source: str
) -> list[str]:
    """Read the variables from the service definition file for a given dataset and source."""
    source_definition = service_definition.sources[source]
    variables = source_definition.main_variables
    return variables


@dataclass
class PartitionParams:
    """Partition metadata/bounds."""

    time_batch: TimeBatch
    latitude_coverage_start: BoundedLat
    longitude_coverage_start: BoundedLon
    lat_tile_size: float
    lon_tile_size: float
    stations_ids: list[str]
    sources: list[str]

    def __post_init__(self):
        self.time_coverage = self.time_batch.get_time_coverage()

    @property
    def latitude_coverage_end(self) -> BoundedLat:
        result = cast(BoundedLat, self.latitude_coverage_start + self.lat_tile_size)
        if result > 90:
            logger.warning("latitude_coverage_end is above 90, setting to 90.")
            result = 90
        return result

    @property
    def longitude_coverage_end(self) -> BoundedLon:
        result = cast(BoundedLon, self.longitude_coverage_start + self.lon_tile_size)
        if result > 360:
            logger.warning("longitude_coverage_end is above 360, setting to 360.")
            result = 360
        return result

    @property
    def time_coverage_start(self) -> datetime:
        return self.time_coverage[0]

    @property
    def time_coverage_end(self) -> datetime:
        return self.time_coverage[1]


@dataclass
class FileParams:
    """Container for several properties of a file."""

    file_size: ByteSize
    data_size: ByteSize
    file_checksum: str
    local_temp_path: Path


@dataclass
class DatasetPartition:
    """Represents the data from a dataset contained in a time,lat,lon cube."""

    dataset_metadata: DatasetMetadata
    partition_params: PartitionParams
    data: pandas.DataFrame
    constraints: ConstraintsSchema


@dataclass
class SerializedPartition:
    """Represents a partition that has been serialized and saved to a file."""

    file_params: FileParams
    partition_params: PartitionParams
    dataset_metadata: DatasetMetadata
    constraints: ConstraintsSchema


def to_catalogue_record(partition: SerializedPartition, asset: str) -> CatalogueSchema:
    """Transform a serialied partition to an entry in the catalogue."""
    dataset_params = partition.dataset_metadata
    partition_params = partition.partition_params
    file_params = partition.file_params
    catalogue_record = CatalogueSchema(
        dataset=dataset_params.name,
        dataset_source=dataset_params.dataset_source,
        time_coverage_start=partition_params.time_coverage_start,
        time_coverage_end=partition_params.time_coverage_end,
        latitude_coverage_start=partition_params.latitude_coverage_start,
        latitude_coverage_end=partition_params.latitude_coverage_end,
        longitude_coverage_start=partition_params.longitude_coverage_start,
        longitude_coverage_end=partition_params.longitude_coverage_end,
        variables=dataset_params.variables,
        stations=partition_params.stations_ids,
        sources=partition_params.sources,
        asset=asset,
        file_size=file_params.file_size,
        data_size=file_params.data_size,
        file_checksum=file_params.file_checksum,
        constraints=partition.constraints,
        version=SemanticVersion.parse(dataset_params.version),
    )
    return catalogue_record


class DatasetReaderFunctionCallable(Protocol):
    """Signature of the dataset readers."""

    def __call__(
        self,
        dataset_name: str,
        config: CDSObsConfig,
        service_definition: ServiceDefinition,
        source: str,
        time_batch: Optional[TimeSpaceBatch],
        **kwargs,
    ) -> pandas.DataFrame:
        ...


@dataclass
class IngestionRunParams:
    dataset_name: str
    source: str
    version: str
    config: CDSObsConfig
    service_definition: ServiceDefinition
