from datetime import datetime
from pprint import pformat
from typing import List

from sqlalchemy import BigInteger, Float, ForeignKey, String
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    deferred,
    mapped_column,
)
from sqlalchemy_json import mutable_json_type

JSONType = mutable_json_type(dbtype=JSONB, nested=True)


class Base(DeclarativeBase):
    pass


class CadsDataset(Base):
    """Schema for the cads_dataset table in the catalogue.

    Each row represents a dataset. It uses dataset names as primary key to keep thing
    more simple.
    """

    __tablename__ = "cads_dataset"
    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String)


class Catalogue(Base):
    """Schema for the catalogue table in the catalogue.

    Each row represents a partition.
    """

    __tablename__ = "catalogue"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dataset: Mapped[int] = mapped_column(ForeignKey("cads_dataset.name"), index=True)
    dataset_source: Mapped[str] = mapped_column(String)
    time_coverage_start: Mapped[datetime] = mapped_column(TIMESTAMP)
    time_coverage_end: Mapped[datetime] = mapped_column(TIMESTAMP)
    latitude_coverage_start: Mapped[float] = mapped_column(Float)
    latitude_coverage_end: Mapped[float] = mapped_column(Float)
    longitude_coverage_start: Mapped[float] = mapped_column(Float)
    longitude_coverage_end: Mapped[float] = mapped_column(Float)
    variables: Mapped[List[str]] = mapped_column(ARRAY(String))
    stations: Mapped[List[str]] = mapped_column(ARRAY(String))
    sources: Mapped[List[str]] = mapped_column(ARRAY(String))
    asset: Mapped[str] = mapped_column(String)
    file_size: Mapped[int] = mapped_column(BigInteger)
    data_size: Mapped[int] = mapped_column(BigInteger)
    file_checksum: Mapped[str] = mapped_column(String)
    constraints: Mapped[JSONType] = deferred(mapped_column(JSONType))  # type: ignore

    def __str__(self):
        return pformat({k: v for k, v in self.__dict__.items() if k[0] != "_"})


def row_to_json(row: Base):
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}
