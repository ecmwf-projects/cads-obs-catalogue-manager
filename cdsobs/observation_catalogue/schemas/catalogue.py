from datetime import datetime
from operator import and_

import pydantic
import sqlalchemy
from pydantic_extra_types.semantic_version import SemanticVersion
from sqlalchemy.sql.elements import BinaryExpression, ColumnElement

from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.schemas.constraints import ConstraintsSchema
from cdsobs.utils.types import BoundedLat, BoundedLon, ByteSize


class CatalogueSchema(pydantic.BaseModel):
    """Pydantic model that represents a catalogue record."""

    dataset: str
    version: SemanticVersion
    dataset_source: str
    time_coverage_start: datetime
    time_coverage_end: datetime
    latitude_coverage_start: BoundedLat
    latitude_coverage_end: BoundedLat
    longitude_coverage_start: BoundedLon
    longitude_coverage_end: BoundedLon
    variables: list[str]
    stations: list[str]
    sources: list[str]
    asset: str
    file_size: ByteSize
    data_size: ByteSize
    file_checksum: str
    constraints: ConstraintsSchema

    @classmethod
    @pydantic.field_validator("dataset")
    def cds_conventions(cls, dataset: str) -> str:
        assert (
            dataset.replace(" ", "_").lower() == dataset
        ), "Dataset name does not follow the CDS convention (lowercase and _ for spaces)"
        return dataset


class CliCatalogueFilters(pydantic.BaseModel):
    dataset: str
    dataset_source: str
    time: list[str] | list[datetime] = pydantic.Field(max_length=2)
    latitudes: list[float] = pydantic.Field(max_length=2)
    longitudes: list[float] = pydantic.Field(max_length=2)
    variables: list[str]
    stations: list[str]

    @property
    def empty(self) -> bool:
        # all filter fields left empty
        return (
            not len(self.dataset)
            and not len(self.dataset_source)
            and not len(self.time)
            and not len(self.latitudes)
            and not len(self.longitudes)
            and not len(self.variables)
            and not len(self.stations)
        )

    def to_repository_filters(self) -> list[BinaryExpression | ColumnElement]:
        conditions: list[BinaryExpression | ColumnElement] = []
        if len(self.dataset):
            conditions.append(Catalogue.dataset == self.dataset)  # type: ignore[arg-type]
        if len(self.dataset_source):
            conditions.append(Catalogue.dataset_source == self.dataset_source)  # type: ignore[arg-type]
        if len(self.variables):
            conditions.append(Catalogue.variables.op("&&")(self.variables))
        if len(self.stations):
            conditions.append(Catalogue.stations.op("&&")(self.stations))
        conditions.extend(self.tuple_matches())
        return conditions

    def tuple_matches(self) -> list[BinaryExpression]:
        conditions = []
        matches_dict = {
            "time": (Catalogue.time_coverage_start, Catalogue.time_coverage_end),
            "latitudes": (
                Catalogue.latitude_coverage_start,
                Catalogue.latitude_coverage_end,
            ),
            "longitudes": (
                Catalogue.longitude_coverage_start,
                Catalogue.longitude_coverage_end,
            ),
        }
        for cli_field, catalogue_fields in matches_dict.items():
            request_values = getattr(self, cli_field)
            match len(request_values):
                case 1:
                    # asked value between start and end of partition
                    conditions.append(
                        and_(
                            catalogue_fields[0] <= request_values[0],
                            catalogue_fields[1] >= request_values[0],
                        )
                    )
                case 2:
                    # start and end of partition inside asked value interval
                    conditions.append(
                        sqlalchemy.func.tsrange(
                            catalogue_fields[0], catalogue_fields[1]
                        ).op("&&")(
                            sqlalchemy.func.tsrange(
                                request_values[0], request_values[1]
                            )
                        )
                    )
        return conditions
