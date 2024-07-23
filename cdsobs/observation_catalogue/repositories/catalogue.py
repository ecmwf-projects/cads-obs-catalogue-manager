from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.orm import Session

from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.base_repository import BaseRepository
from cdsobs.utils.exceptions import CatalogueException


class CatalogueRepository(BaseRepository):
    """Interface to interact with the catalogue table in the catalogue."""

    def __init__(self, session: Session):
        super().__init__(session, model=Catalogue)

    def get_by_dataset_and_source(
        self,
        dataset: str,
        dataset_source: str,
    ) -> Sequence[Catalogue]:
        results = self.session.scalars(
            sa.select(Catalogue).filter(
                Catalogue.dataset == dataset, Catalogue.dataset_source == dataset_source
            )
        ).all()
        return results

    def get_by_dataset(self, dataset: str) -> Sequence[Catalogue]:
        results = self.session.scalars(
            sa.select(Catalogue).filter(Catalogue.dataset == dataset)
        ).all()
        return list(results)

    def get_dataset_assets(self, dataset: str) -> Sequence[str]:
        results = self.session.scalars(
            sa.select(Catalogue.asset).filter(Catalogue.dataset == dataset)
        ).all()
        if len(results):
            return results
        else:
            return []

    def get_by_filters(
        self,
        filter_args: list[sa.sql.elements.BinaryExpression | sa.ColumnElement],
        skip: int = 0,
        limit: int | None = None,
        sort: bool = False,
    ) -> Sequence[Catalogue]:
        try:
            query = sa.select(Catalogue).filter(*filter_args).offset(skip)
            if sort:
                keys = [
                    Catalogue.time_coverage_start,
                    Catalogue.latitude_coverage_start,
                    Catalogue.longitude_coverage_end,
                ]
                query = query.order_by(*keys)  # type: ignore[arg-type]
            if limit is not None:
                # paginated results
                query = query.limit(limit)
            result = self.session.scalars(query).all()
            return result
        except Exception as e:
            raise CatalogueException(f"Invalid query parameters: \n {e}")

    def get_all_assets(self, skip: int = 0, limit: int = 100) -> Sequence[str]:
        results = self.session.scalars(
            sa.select(Catalogue.asset).offset(skip).limit(limit)
        ).all()
        if len(results):
            return [r[0] for r in results]
        else:
            return []

    def exists_asset(self, asset: str):
        result = self.session.scalars(
            sa.select(Catalogue.id).filter(Catalogue.asset == asset).limit(1)
        ).all()
        return len(result) > 0

    def entry_exists(
        self,
        dataset,
        dataset_source,
        time_coverage_start,
        time_coverage_end,
        longitude_coverage_start,
        longitude_coverage_end,
        latitude_coverage_start,
        latitude_coverage_end,
    ):
        result = self.session.scalars(
            sa.select(Catalogue.id)
            .filter(
                Catalogue.dataset == dataset,
                Catalogue.dataset_source == dataset_source,
                Catalogue.time_coverage_start == time_coverage_start,
                Catalogue.time_coverage_end == time_coverage_end,
                Catalogue.longitude_coverage_start >= longitude_coverage_start,
                Catalogue.longitude_coverage_end <= longitude_coverage_end,
                Catalogue.latitude_coverage_start >= latitude_coverage_start,
                Catalogue.latitude_coverage_end <= latitude_coverage_end,
            )
            .limit(1)
        ).first()
        return result is not None
