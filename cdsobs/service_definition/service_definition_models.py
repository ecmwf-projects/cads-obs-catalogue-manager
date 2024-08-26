import typing
from datetime import datetime

import numpy as np
from pydantic import (
    BaseModel,
    conlist,
    field_validator,
    model_validator,
)
from pydantic_core.core_schema import ValidationInfo

from cdsobs.service_definition.utils import custom_assert
from cdsobs.utils.types import NotEmptyUniqueStrList, StrNotBlank

""" models """


class JoinIds(BaseModel):
    header: StrNotBlank
    data: StrNotBlank


class SpaceColumns(BaseModel):
    x: StrNotBlank
    y: StrNotBlank


class Product(BaseModel):
    group_name: StrNotBlank
    columns: NotEmptyUniqueStrList  # type: ignore


class TimePeriod(BaseModel):
    start: str
    end: str

    @classmethod
    @field_validator("start")
    def valid_start_date(cls, start):
        try:
            datetime.fromisoformat(start.replace("Z", "+00:00"))
        except ValueError:
            custom_assert(False, "Invalid start date")

    @classmethod
    @field_validator("end")
    def end_date(cls, end):
        try:
            datetime.fromisoformat(end.replace("Z", "+00:00"))
        except ValueError:
            custom_assert(end == "current" or False, "Invalid end date")


class TimeColumn(BaseModel):
    name: str = "report_timestamp"
    time_period: TimePeriod | None = None


class UnitChange(BaseModel):
    names: dict[StrNotBlank, StrNotBlank]
    offset: float
    scale: float


class Description(BaseModel, extra="allow"):
    description: str
    dtype: str | None = None
    long_name: StrNotBlank
    units: StrNotBlank | None = None

    @classmethod
    @field_validator("dtype")
    def valid_type(cls, dtype):
        if dtype is not None:
            try:
                np.dtype(dtype)
            except TypeError:
                custom_assert(False, "Invalid data type provided")


Operation = typing.Literal["sum", "sub"]


class CombineColumns(BaseModel):
    out_name: StrNotBlank
    op: Operation
    columns: conlist(str, min_length=1)  # type: ignore


class CdmMapping(BaseModel, extra="forbid"):
    rename: dict[StrNotBlank, StrNotBlank] | None = None
    add: dict[StrNotBlank, StrNotBlank | float | int | bool] | None = None
    unit_changes: dict[StrNotBlank, UnitChange] | None = None
    melt_columns: bool
    combine_columns: CombineColumns | None = None


class SourceDefinition(BaseModel, extra="forbid"):
    cdm_mapping: CdmMapping
    header_columns: list[str] | None = None
    header_table: StrNotBlank | None = None
    data_table: StrNotBlank
    join_ids: JoinIds | None = None
    descriptions: dict[str, Description]
    mandatory_columns: list[str]
    order_by: list[str]
    products: list[Product]
    out_columns_order: NotEmptyUniqueStrList | None = None  # type: ignore
    products_hierarchy: NotEmptyUniqueStrList | None = None  # type: ignore
    space_columns: SpaceColumns | None = None
    time_column: TimeColumn | None = None
    remap_categories: dict[str, dict[str, int]] | None = None
    _errors: bool = False

    @property
    def errors(self):
        return self._errors

    def is_multitable(self):
        return self.header_table is not None

    @classmethod
    @field_validator("products")
    def valid_product_defs(
        cls, products: list[Product], info: ValidationInfo
    ) -> list[Product]:
        for product in products:
            cls._errors = custom_assert(
                product in info.data["descriptions"],
                f"Product {product} must have a description",
                cls._errors,
            )
        return products

    @classmethod
    @field_validator("mandatory_columns")
    def valid_mandatory_column_defs(
        cls, mandatory_columns: list[str], info: ValidationInfo
    ) -> list[str]:
        for mandatory_column in mandatory_columns:
            cls._errors = custom_assert(
                mandatory_column in info.data["descriptions"],
                f"Column {mandatory_column} stated as mandatory is not defined",
                cls._errors,
            )
        return mandatory_columns

    def get_raw_header_columns(self):
        return [
            k for k, v in self.cdm_mapping.rename.items() if v in self.header_columns
        ]

    def get_raw_mandatory_columns(self):
        return [
            k for k, v in self.cdm_mapping.rename.items() if v in self.mandatory_columns
        ]


class ServiceDefinition(BaseModel, extra="forbid"):
    global_attributes: dict
    out_columns_order: NotEmptyUniqueStrList | None = None
    products_hierarchy: NotEmptyUniqueStrList | None = None
    space_columns: SpaceColumns | None = None
    time_column: TimeColumn | None = None
    sources: dict[str, SourceDefinition]
    _errors: bool = False  # to store if there has been errors

    @property
    def errors(self):
        return self._errors

    @classmethod
    @field_validator("out_columns_order")
    def valid_out_columns_order(cls, out_columns_order: list[str]) -> list[str]:
        for c in out_columns_order:
            cls._errors = custom_assert(
                c.replace(" ", "_").lower() == c,
                f"Column name {c} (in out_columns_order) does "
                f"not follow the CDS convention"
                f" (lowercase and _ for spaces)",
                cls._errors,
            )
        return out_columns_order

    @classmethod
    @model_validator(mode="before")
    def valid_sources(cls, service_definition):
        """
        Validate the sources keyword.

        Columns have to be used only in one context, this is, a variable can not be set
        also as a mandatory column.
        """
        for source_name, source_desc in service_definition.get("sources").items():
            product_columns = set()
            match source_desc["products"]:
                case {"columns": columns}:
                    product_columns.update(columns)
            mandatory_columns = set(source_desc["mandatory_columns"])
            repeated = mandatory_columns.intersection(product_columns)
            cls._errors = custom_assert(
                len(repeated) == 0,
                f"Found repeated columns in {source_name}: "
                f"{repeated}."
                f" Columns must be defined either in "
                f"mandatory columns or "
                f"in product columns, not in both.",
            )
        return service_definition

    @classmethod
    @model_validator(mode="before")
    def valid_products(cls, service_definition):
        sources = service_definition.get("sources")
        for source_name, source_def in sources.items():
            products = source_def.products
            if len(products):
                cls._errors = custom_assert(
                    all(
                        [
                            p.group_name in service_definition.get("products_hierarchy")
                            for p in products
                        ]
                    ),
                    f"A group_name in {source_name} is "
                    f"not defined at "
                    f"products_hierarchy",
                    cls._errors,
                )
        return service_definition

    @classmethod
    @model_validator(mode="before")
    def valid_product_columns(cls, service_definition):
        sources = service_definition.get("sources")
        for source, source_desc in sources.items():
            for column_name, desc in source_desc.descriptions.items():
                name_for_output = desc["name_for_output"]
                cls._errors = custom_assert(
                    name_for_output in service_definition.get("out_columns_order"),
                    f"Source {source} is missing the entry "
                    f'{name_for_output}" for column '
                    f'"{column_name}" in the "out_columns_order" '
                    f"list",
                    cls._errors,
                )
        return service_definition

    @classmethod
    @model_validator(mode="before")
    def valid_variables_and_units(cls, service_definition):
        """
        Products are properties of variables.

        Each defined product must be traced back to a column defining a
        variable. For each column of a product we search in each variable
        definition to see if we have a relation. If not an error is raised
        """
        for source, source_desc in service_definition.get("sources").items():
            products_to_check = {}
            variables = []
            for product in service_definition.get("products"):
                match product:
                    case {"group_name": "variables", "columns": columns}:
                        variables = columns
                    case {"group_name": group_name, "columns": columns}:
                        products_to_check[group_name] = columns

            for p, columns in products_to_check.items():
                cond = any(
                    [p in service_definition.get("descriptions")[v] for v in variables]
                )
                cls._errors = custom_assert(
                    cond,
                    f"The product {p} has no "
                    f"variable associated for "
                    f"source {source}",
                    cls._errors,
                )
        return service_definition

    @classmethod
    @model_validator(mode="before")
    def valid_space_columns(cls, service_definition):
        space_columns = service_definition.get("space_columns")
        if space_columns is None:
            for source, source_desc in service_definition.get("sources").items():
                # Space columns need to be defined. If not at top level, they must be
                # defined for every source
                cls._errors = custom_assert(
                    "space_columns" in source_desc.keys(),
                    f"Undefined space_columns for source {source}",
                    cls._errors,
                )

    @classmethod
    @model_validator(mode="before")
    def valid_time_column(cls, service_definition):
        space_columns = service_definition.get("time_column")
        if space_columns is None:
            for source, source_desc in service_definition.get("sources").items():
                # Time column needs to be defined. If not at top level, they must be
                # defined for every source
                cls._errors = custom_assert(
                    "time_column" in source_desc.keys(),
                    f"Undefined time_column for source {source}",
                    cls._errors,
                )
