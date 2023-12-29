from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cdsobs.config import DBConfig
from cdsobs.observation_catalogue.models import Base
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def get_session(settings: DBConfig, reset: bool = False) -> Session:
    """Get a session in the catalogue database."""
    engine = create_engine(settings.get_url())  # echo=True for more descriptive logs
    if reset:
        Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    logger.debug(f"Created session in {engine=}")
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()
    return session
