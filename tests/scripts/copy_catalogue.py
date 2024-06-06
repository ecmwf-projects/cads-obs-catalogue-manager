from pathlib import Path

from cdsobs.cli._copy_dataset import catalogue_copy
from cdsobs.config import CDSObsConfig, DBConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.storage import S3Client


def main():
    init_config_yml = Path("cdsobs_config_bopen.yml")
    dataset = "insitu-comprehensive-upper-air-observation-network"
    init_config = CDSObsConfig.from_yaml(init_config_yml)
    init_s3client = S3Client.from_config(init_config.s3config)
    init_catalogue_session = get_session(init_config.catalogue_db)
    entries = CatalogueRepository(init_catalogue_session).get_by_dataset(dataset)
    dest_catalogue_session = get_session(
        DBConfig(
            db_user="cds",
            pwd="quai6Chee0bai&r",
            host="localhost",
            port=35432,
            db_name="obsdev",
        )
    )
    catalogue_copy(
        dest_catalogue_session,
        entries,
        init_s3client,
        dataset,
    )


if __name__ == "__main__":
    main()
