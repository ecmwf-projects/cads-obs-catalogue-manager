import sqlalchemy as sa
from pydantic_extra_types.semantic_version import SemanticVersion
from sqlalchemy.orm import Session

from cdsobs.observation_catalogue.models import CadsDatasetVersion
from cdsobs.observation_catalogue.repositories.base import BaseRepository
from cdsobs.observation_catalogue.schemas.version_schema import CadsDatasetVersionSchema
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


class CadsDatasetVersionRepository(BaseRepository):
    """Interface to interact with the cads_dataset table in the catalogue."""

    def __init__(self, session: Session):
        super().__init__(session, model=CadsDatasetVersion)

    def create_dataset_version(self, dataset_name: str, version: str):
        if not self.dataset_version_exists(dataset_name, version=version):
            logger.info(
                f"Creating version {version} for dataset {dataset_name} in the"
                f" catalogue."
            )
            dataset_version = CadsDatasetVersionSchema(
                dataset=dataset_name, version=SemanticVersion.parse(version)
            )
            self.create(dataset_version)

    def dataset_version_exists(self, dataset_name: str, version: str) -> bool:
        return (
            self.session.scalar(
                sa.select(sa.func.count())
                .select_from(CadsDatasetVersion)
                .filter(
                    CadsDatasetVersion.dataset == dataset_name,
                    CadsDatasetVersion.version == version,
                )
            )
            == 1
        )

    def get_dataset(self, dataset_name: str, version: str) -> CadsDatasetVersion | None:
        return self.session.scalar(
            sa.select(CadsDatasetVersion).filter(
                CadsDatasetVersion.dataset == dataset_name,
                CadsDatasetVersion.version == version,
            )
        )

    def delete_dataset(self, dataset_name: str, version: str):
        dataset = self.get_dataset(dataset_name, version)
        self.session.delete(dataset)
        self.session.commit()
