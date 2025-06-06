import tempfile
from pathlib import Path

import netCDF4
import numpy

from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import get_logger
from cdsobs.utils.utils import compute_hash, get_file_size

logger = get_logger(__name__)


def main(config):
    dataset = "insitu-comprehensive-upper-air-observation-network"
    s3client = S3Client.from_config(config.s3config)

    with get_session(
        config.catalogue_db
    ) as init_session, tempfile.TemporaryDirectory() as tempdir:
        entries = CatalogueRepository(init_session).get_by_dataset(dataset)
        for entry in entries:
            logger.info(f"Fixing {entry.asset}")
            bucket, asset_name = entry.asset.split("/")
            asset_local_path = Path(tempdir, asset_name)
            s3client.download_file(bucket, asset_name, asset_local_path)
            # Fix file and reupload
            with netCDF4.Dataset(asset_local_path, mode="a") as ncdataset:
                if "homogenisation_adjustment" not in ncdataset.variables:
                    logger.info(
                        "homogenisation_adjustment not present in this file, skipping"
                    )
                    continue
                homogenisation_adjustment_ncvar = ncdataset["homogenisation_adjustment"]
                # Check if it was already fixed
                if hasattr(homogenisation_adjustment_ncvar, "status"):
                    logger.info(f"{entry.asset} is already fixed, skipping")
                    Path(asset_local_path).unlink()
                    continue
                homogenisation_adjustment = homogenisation_adjustment_ncvar[:]
                observed_variable = ncdataset["observed_variable"][:]
                humidity_adjustment = ncdataset["humidity_bias_estimate"][:]

                mask = numpy.isin(observed_variable, (34, 137, 138, 39))
                homogenisation_adjustment[mask] = humidity_adjustment[mask]

                wind_adjustment = ncdataset["wind_bias_estimate"][:]

                mask = numpy.isin(observed_variable, (106, 107, 139, 140))
                homogenisation_adjustment[mask] = wind_adjustment[mask]
                homogenisation_adjustment_ncvar[:] = homogenisation_adjustment
                setattr(
                    ncdataset["homogenisation_adjustment"],
                    "status",
                    "Temperature adjusted with RISE bias estimate, merged with humidity and wind adjustments",
                )
                ncdataset.sync()

            new_checksum = compute_hash(Path(asset_local_path))
            new_file_size = get_file_size(Path(asset_local_path))
            entry.file_checksum = new_checksum
            entry.file_size = new_file_size
            s3client.upload_file(bucket, asset_name, asset_local_path)
            init_session.commit()
            Path(asset_local_path).unlink()

    logger.info("Finished!")


# pytest.mark.skip("Don't needed anymore")
def test_fix_cuon(test_config, test_repository):
    main(test_config)


if __name__ == "__main__":
    config_yml = Path("~/.cdsobs/cdsobs_config.yml").expanduser()
    config = CDSObsConfig.from_yaml(config_yml)
    main(config)
