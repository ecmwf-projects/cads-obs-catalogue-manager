from typing import Any, Sequence

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import Session

from cdsobs.observation_catalogue.database import Base


class BaseRepository:
    """Base interface for interact with the catalogue."""

    def __init__(self, session: Session, model: Any):
        self.session = session
        self.model = model

    def get(self, record_id: Any):
        return self.session.scalars(
            sa.select(self.model).filter(self.model.id == record_id).limit(1)
        ).first()

    def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Any]:
        return self.session.scalars(
            sa.select(self.model).offset(skip).limit(limit)
        ).all()

    def create(self, obj_in: BaseModel) -> Base:
        db_obj = self.model(**obj_in.model_dump(mode="json"))  # type: ignore
        self.session.add(db_obj)
        self.session.commit()
        self.session.refresh(db_obj)
        return db_obj

    def create_many(self, objs_in: list[Any]):
        objs_in_data = [oi.model_dump(mode="json") for oi in objs_in]
        db_objs = [self.model(**oid) for oid in objs_in_data]
        self.session.bulk_save_objects(db_objs)
        self.session.commit()

    def remove(self, record_id: int) -> Base | None:
        obj = self.session.get(self.model, record_id)
        self.session.delete(obj)
        self.session.commit()
        return obj
