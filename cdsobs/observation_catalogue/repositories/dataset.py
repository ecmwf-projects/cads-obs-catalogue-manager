import sqlalchemy as sa
from sqlalchemy.orm import Session

from cdsobs.observation_catalogue.models import CadsDataset
from cdsobs.observation_catalogue.repositories.base import BaseRepository
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


class CadsDatasetRepository(BaseRepository):
    """Interface to interact with the cads_dataset table in the catalogue."""

    def __init__(self, session: Session):
        super().__init__(session, model=CadsDataset)

    def create_dataset(self, dataset_name: str):
        if not self.dataset_exists(dataset_name):
            logger.info(f"Creating dataset={dataset_name} in the catalogue")
            dataset_version = CadsDataset(name=dataset_name)
            self.session.add(dataset_version)
            self.session.commit()

    def dataset_exists(self, dataset_name: str) -> bool:
        return (
            self.session.scalar(
                sa.select(sa.func.count())
                .select_from(CadsDataset)
                .filter(
                    CadsDataset.name == dataset_name,
                )
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
        self.session.commit()
