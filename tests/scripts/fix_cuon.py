import tempfile
from pathlib import Path

import netCDF4

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
                # Check if it was already fixed
                if (
                    "RISE_bias_estimate" not in ncdataset.variables
                    and "profile_id" in ncdataset.variables
                    and "quality_flag" in ncdataset.variables
                    and "homogenisation_method" in ncdataset.variables
                ):
                    logger.info(f"{entry.asset} is already fixed, skipping")
                    Path(asset_local_path).unlink()
                    continue
                if "RISE_bias_estimate" in ncdataset.variables:
                    ncdataset.renameVariable(
                        "RISE_bias_estimate", "homogenisation_adjustment"
                    )
                else:
                    logger.info("RISE_bias_estimate not present in this file, so no homogenisation_adjustment will be written")
                report_id_var = ncdataset.variables["report_id"]
                profile_id = ncdataset.createVariable(
                    "profile_id",
                    datatype=report_id_var.dtype,
                    compression="zlib",
                    chunksizes=report_id_var.chunking(),
                    complevel=1,
                    shuffle=True,
                    dimensions=report_id_var.dimensions,
                )
                profile_id[:] = report_id_var[:]
                quality_flag = ncdataset.createVariable(
                    "quality_flag",
                    datatype="int16",
                    compression="zlib",
                    chunksizes=(report_id_var.chunking()[0],),
                    complevel=1,
                    shuffle=True,
                    dimensions=("observation_id",),
                )
                quality_flag[:] = 2
                homogenisation_method = ncdataset.createVariable(
                    "homogenisation_method",
                    datatype="int16",
                    compression="zlib",
                    chunksizes=(report_id_var.chunking()[0],),
                    complevel=1,
                    shuffle=True,
                    dimensions=("observation_id",),
                )
                homogenisation_method[:] = 14
                report_meaning_of_timestamp = ncdataset.variables[
                    "report_meaning_of_timestamp"
                ]
                report_meaning_of_timestamp[:] = 1
                ncdataset.sync()

            new_checksum = compute_hash(Path(asset_local_path))
            new_file_size = get_file_size(Path(asset_local_path))
            entry.file_checksum = new_checksum
            entry.file_size = new_file_size
            s3client.upload_file(bucket, asset_name, asset_local_path)
            init_session.commit()
            Path(asset_local_path).unlink()
    
    logger.info("Finished!")

def test_fix_cuon(test_config, test_repository):
    main(test_config)


if __name__ == "__main__":
    config_yml = Path("~/.cdsobs/cdsobs_config.yml").expanduser()
    config = CDSObsConfig.from_yaml(config_yml)
    main(config)
