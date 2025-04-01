from datetime import datetime
from pprint import pformat
from typing import List

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    deferred,
    mapped_column,
    relationship,
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
    name: Mapped[str] = mapped_column(
        String, primary_key=True, unique=True, nullable=False
    )
    versions: Mapped[List["CadsDatasetVersion"]] = relationship(
        back_populates="dataset_obj", cascade="all"
    )


class CadsDatasetVersion(Base):
    __tablename__ = "cads_dataset_version"
    dataset: Mapped["CadsDataset"] = mapped_column(
        String, ForeignKey("cads_dataset.name"), nullable=False
    )
    dataset_obj: Mapped["CadsDataset"] = relationship(
        back_populates="versions", cascade="all"
    )
    name: Mapped[str] = mapped_column(String, default="1.0.0", nullable=False)
    deprecated: Mapped[bool] = mapped_column(Boolean, default=False)
    catalogue_entries: Mapped[List["Catalogue"]] = relationship(
        back_populates="dataset_version", cascade="all"
    )
    __table_args__ = (
        PrimaryKeyConstraint("name", "dataset"),
        UniqueConstraint("name", "dataset"),
    )

    def __str__(self):
        return pformat({k: v for k, v in self.__dict__.items() if k[0] != "_"})


class Catalogue(Base):
    """Schema for the catalogue table in the catalogue.

    Each row represents a partition.
    """

    __tablename__ = "catalogue"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dataset: Mapped[str] = mapped_column(String, index=True)
    version: Mapped[str] = mapped_column(String, index=True)
    dataset_version: Mapped["CadsDatasetVersion"] = relationship(
        back_populates="catalogue_entries", cascade="all"
    )
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
    # Ensuring foreign keys reference both parts of the composite key
    __table_args__ = (
        ForeignKeyConstraint(
            ["dataset", "version"],
            ["cads_dataset_version.dataset", "cads_dataset_version.name"],
        ),
    )

    def __str__(self):
        return pformat({k: v for k, v in self.__dict__.items() if k[0] != "_"})


def row_to_json(row: Base):
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}
