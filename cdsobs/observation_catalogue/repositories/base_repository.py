from typing import Any, Sequence

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt, wait_fixed

from cdsobs.observation_catalogue.database import Base
from cdsobs.utils.utils import jsonable_encoder


class BaseRepository:
    """Base interface for interact with the catalogue."""

    def __init__(self, session: Session, model: Any):
        self.session = session
        self.model = model

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
    def get(self, record_id: Any):
        return self.session.scalars(
            sa.select(self.model).filter(self.model.id == record_id).limit(1)
        ).first()

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
    def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Any]:
        return self.session.scalars(
            sa.select(self.model).offset(skip).limit(limit)
        ).all()

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
    def create(self, obj_in: BaseModel) -> Base:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        self.session.add(db_obj)
        self.session.commit()
        self.session.refresh(db_obj)
        return db_obj

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
    def create_many(self, objs_in: list[Any]):
        objs_in_data = [jsonable_encoder(oi) for oi in objs_in]
        db_objs = [self.model(**oid) for oid in objs_in_data]
        self.session.bulk_save_objects(db_objs)
        self.session.commit()

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
    def remove(self, record_id: int) -> Base | None:
        obj = self.session.get(self.model, record_id)
        self.session.delete(obj)
        self.session.commit()
        return obj
