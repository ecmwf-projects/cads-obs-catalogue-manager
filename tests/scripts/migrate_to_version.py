import json

import psycopg2
from sqlalchemy import create_engine, text
from pathlib import Path
from cdsobs.config import CDSObsConfig, DBConfig
from cdsobs.constants import DEFAULT_VERSION
from cdsobs.observation_catalogue.models import Base
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def create_database_if_not_exists(db_config: DBConfig, new_database: str):
    """Connect to a postgres db and create a new db with another name.

    Only if it does not already exists.
    """
    conn = psycopg2.connect(
        dbname=db_config.db_name,
        user=db_config.db_user,
        password=db_config.pwd,
        port=db_config.port,
        host=db_config.host,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{new_database}'")
    exists = cursor.fetchone()
    if not exists:
        logger.info(f"Creating new database {new_database} with the same credentials.")
        cursor.execute(f"CREATE DATABASE {new_database}")
    cursor.close()
    conn.close()


def migrate_to_new_schema(config: CDSObsConfig):
    """Copy catalogue database and assets to the new schema with versions."""
    logger.info("Starting migration")
    s3client = S3Client.from_config(config.s3config)
    # Create connections to both databases
    source_engine = create_engine(
        config.catalogue_db.get_url(), execution_options={"stream_results": True}
    )
    target_db = config.catalogue_db.model_copy()
    target_db.db_name = config.catalogue_db.db_name + "_version"
    create_database_if_not_exists(config.catalogue_db, target_db.db_name)
    target_engine = create_engine(target_db.get_url())

    # Stream rows from source and insert into target
    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        # Create the schema in the new database
        Base.metadata.create_all(target_engine)
        # Stream the dataset table
        dataset_table = source_conn.execution_options(stream_results=True).execute(
            text(
                """
            SELECT DISTINCT name FROM cads_dataset;
        """
            )
        )

        for row in dataset_table:
            logger.info(f"Inserting {row=}")
            target_conn.execute(
                text(
                    "INSERT INTO cads_dataset (name) VALUES (:name) ON CONFLICT DO NOTHING"
                ),
                {"name": row.name},
            )
            # Insert new table dataset version row
            target_conn.execute(
                text(
                    """
                    INSERT INTO cads_dataset_version (dataset, version, deprecated)
                    VALUES (:dataset, :version, :deprecated)
                    ON CONFLICT DO NOTHING
                """
                ),
                {
                    "dataset": row.name,
                    "version": DEFAULT_VERSION,
                    "deprecated": False,
                },
            )
        # Stream catalogue entries
        catalogue_entries = source_conn.execution_options(stream_results=True).execute(
            text(
                """
            SELECT * FROM catalogue;
        """
            )
        )
        for entry in catalogue_entries:
            logger.info(f"Inserting {entry=}")
            bucket, filename = entry.asset.split("/")
            new_filename = filename.replace(
                entry.dataset, entry.dataset + f"_{DEFAULT_VERSION}"
            )
            new_asset = bucket + "/" + new_filename
            s3client.copy_file(bucket, filename, bucket, new_filename)
            logger.info("")
            target_conn.execute(
                text(
                    """
                    INSERT INTO catalogue (
                        dataset,
                        version,
                        dataset_source,
                        time_coverage_start,
                        time_coverage_end,
                        latitude_coverage_start,
                        latitude_coverage_end,
                        longitude_coverage_start,
                        longitude_coverage_end,
                        variables,
                        stations,
                        sources,
                        asset,
                        file_size,
                        data_size,
                        file_checksum,
                        constraints
                    )
                    VALUES (
                        :dataset,
                        :version,
                        :dataset_source,
                        :time_coverage_start,
                        :time_coverage_end,
                        :latitude_coverage_start,
                        :latitude_coverage_end,
                        :longitude_coverage_start,
                        :longitude_coverage_end,
                        :variables,
                        :stations,
                        :sources,
                        :asset,
                        :file_size,
                        :data_size,
                        :file_checksum,
                        :constraints
                    )
                    ON CONFLICT DO NOTHING
                """
                ),
                {
                    "dataset": entry.dataset,
                    "version": DEFAULT_VERSION,
                    "dataset_source": entry.dataset_source,
                    "time_coverage_start": entry.time_coverage_start,
                    "time_coverage_end": entry.time_coverage_end,
                    "latitude_coverage_start": entry.latitude_coverage_start,
                    "latitude_coverage_end": entry.latitude_coverage_end,
                    "longitude_coverage_start": entry.longitude_coverage_start,
                    "longitude_coverage_end": entry.longitude_coverage_end,
                    "variables": entry.variables,
                    "stations": entry.stations,
                    "sources": entry.sources,
                    "asset": new_asset,
                    "file_size": entry.file_size,
                    "data_size": entry.data_size,
                    "file_checksum": entry.file_checksum,
                    "constraints": json.dumps(entry.constraints),
                },
            )
    logger.info("Migration successful")


def test_migrate_to_new_schema(test_config, test_repository):
    migrate_to_new_schema(test_config)


if __name__ == "__main__":
    config_yaml = Path("/path/to/.cdsobs/cdsobs_config.yml")
    migrate_to_new_schema(CDSObsConfig.from_yaml(config_yaml))
