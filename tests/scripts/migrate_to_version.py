import json

from sqlalchemy import create_engine, text

from cdsobs.constants import DEFAULT_VERSION
from cdsobs.observation_catalogue.models import Base
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def main():
    # Create connections to both databases
    source_engine = create_engine(
        "postgresql://docker:docker@localhost:5433/catalogue2",
        execution_options={"stream_results": True},
    )
    target_engine = create_engine(
        "postgresql://docker:docker@localhost:5433/catalogue_version"
    )

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
                    "asset": entry.asset,
                    "file_size": entry.file_size,
                    "data_size": entry.data_size,
                    "file_checksum": entry.file_checksum,
                    "constraints": json.dumps(entry.constraints),
                },
            )


if __name__ == "__main__":
    main()
