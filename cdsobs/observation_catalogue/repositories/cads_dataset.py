import sqlalchemy as sa
from sqlalchemy.orm import Session

from cdsobs.observation_catalogue.models import CadsDataset
from cdsobs.observation_catalogue.repositories.base_repository import BaseRepository


class CadsDatasetRepository(BaseRepository):
    """Interface to interact with the cads_dataset table in the catalogue."""

    def __init__(self, session: Session):
        super().__init__(session, model=CadsDataset)

    def create_dataset(self, dataset_name: str):
        if not self.datasets_exists(dataset_name):
            self.session.add(CadsDataset(name=dataset_name, version="1.0"))
            self.session.commit()

    def set_dataset_version(self, dataset_name: str, version: str):
        # Avoid supporting something like semver to keep thigs simple for the moment
        dataset_entry = self.session.scalars(
            sa.select(CadsDataset).filter(CadsDataset.name == dataset_name).limit(1)
        ).first()
        if dataset_entry is None:
            raise RuntimeError(f"Dataset {dataset_name} nor found in the catalogue.")
        else:
            dataset_entry.version = version
            self.session.add(dataset_entry)
            self.session.commit()

    def datasets_exists(self, dataset_name: str) -> bool:
        return (
            self.session.scalar(
                sa.select(sa.func.count())
                .select_from(CadsDataset)
                .filter(CadsDataset.name == dataset_name)
            )
            == 1
        )

    def get_dataset(self, dataset_name: str) -> CadsDataset | None:
        return self.session.scalar(
            sa.select(CadsDataset).filter(CadsDataset.name == dataset_name)
        )

    def delete_dataset(self, dataset_name: str):
        dataset = self.get_dataset(dataset_name)
        self.session.delete(dataset)
