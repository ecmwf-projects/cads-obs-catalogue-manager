from datetime import datetime
from itertools import product
from typing import Any, List, Literal

import sqlalchemy
from pydantic import BaseModel, field_validator, model_validator
from pydantic_extra_types.semantic_version import SemanticVersion
from sqlalchemy import BinaryExpression, ColumnElement, any_

from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.utils.types import BoundedLat, BoundedLon

RetrieveFormat = Literal["netCDF", "csv"]


class RetrieveParams(BaseModel, extra="ignore"):
    dataset_source: str
    stations: None | List[str] = None
    variables: List[str] | None = None
    latitude_coverage: None | tuple[BoundedLat, BoundedLat] = None
    longitude_coverage: None | tuple[BoundedLon, BoundedLon] = None
    time_coverage: None | tuple[datetime, datetime] = None
    year: None | List[int] = None
    month: None | List[int] = None
    day: None | List[int] = None
    format: RetrieveFormat = "netCDF"
    version: str = "last"

    @classmethod
    @model_validator(mode="before")
    def validate_coverages(cls, values):
        """Check that the coverages are consistent."""
        coverages = ["latitude_coverage", "longitude_coverage", "time_coverage"]
        for coverage in coverages:
            if values[coverage] is not None:
                assert values[coverage][0] < values[coverage][1]
        # Check that year is not set if using time_coverage
        if values["time_coverage"] is not None:
            assert values["year"] is None
        return values

    @classmethod
    @field_validator("version", mode="before")
    def validate_version(cls, value):
        if value != "last":
            assert SemanticVersion.is_valid(value)

    def get_filter_arguments(
        self, dataset: str | None = None
    ) -> List[BinaryExpression | ColumnElement]:
        """
        Transform the retrieve params to a list of SQLalchemy expressions.

        These can be passed to filter. SQLAlchemy will turn these into the WHERE block
        of the query.
        """
        retrieve_params = self.model_dump()
        # Filter out nones, month
        params_to_skip = ["month", "day", "format"]
        retrieve_params_to_filter = {
            rp: rpval
            for rp, rpval in retrieve_params.items()
            if (rpval is not None) and (rp not in params_to_skip)
        }
        filter_arguments: List[BinaryExpression | ColumnElement] = []
        for param, value in retrieve_params_to_filter.items():
            if "coverage" in param:
                # Handle ranges
                # Get the names of the start and end columns in the Catalogue schema
                filter_arg = self._get_coverage_argument(param, value)
            elif param == "year":
                filter_arg = self._get_year_month_argument(retrieve_params)
            else:
                # Handle the parameters that are not ranges
                if isinstance(value, list):
                    # If it is a list, use overlaps operator for the ARRAY postgres type
                    filter_arg = getattr(Catalogue, param).op("&&")(value)
                else:
                    # If is a single value check for equality
                    filter_arg = getattr(Catalogue, param) == value
            filter_arguments.append(filter_arg)
        # Add version
        retrieve_params["version"]
        # Add dataset name too
        if dataset is not None:
            filter_arguments.append(Catalogue.dataset == dataset)
        return filter_arguments

    def _get_year_month_argument(self, retrieve_params: dict) -> BinaryExpression:
        # Handle year and month, ingore day in this part
        # This will filter to keep partitions that containt the year, month
        # combinations specified, using where tsrange >@ any(array[timestamp])
        if retrieve_params["month"] is not None:
            month = retrieve_params["month"]
        else:
            month = range(1, 13)
        datelist = [
            datetime(yy, mm, 1) for yy, mm in product(retrieve_params["year"], month)
        ]
        filter_arg: BinaryExpression = sqlalchemy.func.tsrange(
            Catalogue.time_coverage_start, Catalogue.time_coverage_end
        ).op("@>")(
            any_(datelist)  # type: ignore[arg-type]
        )
        return filter_arg

    def _get_coverage_argument(self, param: str, value: Any) -> BinaryExpression:
        start_column = getattr(Catalogue, param + "_start")
        end_column = getattr(Catalogue, param + "_end")
        if "time" in param:
            # For time we use tsrange
            range_func = sqlalchemy.func.tsrange
        else:
            # For lat and lon we use numrange, it requires to cast to numeric
            # or it won't work
            range_func = sqlalchemy.func.numrange
            start_column = sqlalchemy.cast(start_column, sqlalchemy.Numeric)
            end_column = sqlalchemy.cast(end_column, sqlalchemy.Numeric)
        # Get the start and end values to query for
        start_param_val = value[0]
        end_param_val = value[1]
        # Filter the catalogue entries. && operator checks if two ranges overlap
        filter_arg: BinaryExpression = range_func(start_column, end_column).op("&&")(
            range_func(start_param_val, end_param_val)
        )
        return filter_arg


class RetrieveArgs(BaseModel):
    dataset: str
    params: RetrieveParams
