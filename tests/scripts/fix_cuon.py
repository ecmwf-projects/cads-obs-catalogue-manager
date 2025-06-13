import tempfile
from pathlib import Path

import netCDF4
import numpy
import pytest

from cdsobs.cdm.api import read_cdm_code_tables
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
    # Get units mapping
    cdm_code_tables = read_cdm_code_tables(config.cdm_tables_location)
    unit_codes = cdm_code_tables["units"]
    code2unit = unit_codes.table["abbreviation"].to_dict()
    code2unit_vec = numpy.vectorize(code2unit.get)

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
                if "old_units" in ncdataset.variables:
                    logger.info(f"{entry.asset} is already fixed, skipping")
                    continue
                units_ncvar = ncdataset.variables["units"]
                units_int = netCDF4.chartostring(units_ncvar[:]).astype("int")
                ncdataset.renameVariable("units", "old_units")
                ncdataset.renameDimension("string_units", "old_string_units")
                units_char = netCDF4.stringtochar(
                    code2unit_vec(units_int).astype("bytes")
                )
                units_char_len = units_char.shape[1]
                ncdataset.createDimension("string_units", units_char_len)
                ncdataset.createVariable(
                    "units",
                    datatype="S1",
                    compression="zlib",
                    chunksizes=(units_ncvar.chunking()[0], units_char_len),
                    complevel=1,
                    shuffle=True,
                    dimensions=(units_ncvar.dimensions[0], "string_units"),
                )
                ncdataset.variables["units"][:] = units_char
                ncdataset.sync()

            new_checksum = compute_hash(Path(asset_local_path))
            new_file_size = get_file_size(Path(asset_local_path))
            entry.file_checksum = new_checksum
            entry.file_size = new_file_size
            s3client.upload_file(bucket, asset_name, asset_local_path)
            init_session.commit()
            Path(asset_local_path).unlink()

    logger.info("Finished!")


pytest.mark.skip("Don't needed anymore")


def test_fix_cuon(test_config, test_repository):
    main(test_config)


if __name__ == "__main__":
    config_yml = Path("~/.cdsobs/cdsobs_config.yml").expanduser()
    config = CDSObsConfig.from_yaml(config_yml)
    main(config)
