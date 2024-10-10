import os
from pathlib import Path
from typing import Dict, List, Literal, NewType, Optional

import pydantic
import pydantic_settings
import yaml
from pydantic_core.core_schema import ValidationInfo

from cdsobs.cdm.tables import DEFAULT_CDM_TABLES_TO_USE
from cdsobs.utils.exceptions import ConfigError, ConfigNotFound
from cdsobs.utils.types import LatTileSize, LonTileSize, TimeTileSize


def _get_default_cdm_tables_location() -> Path:
    if "CDM_TABLES_LOCATION" in os.environ:
        return Path(os.environ["CDM_TABLES_LOCATION"])
    else:
        return Path.home().joinpath(".cdsobs")


class DBConfig(pydantic.BaseModel):
    db_user: str
    pwd: str
    host: str
    port: int
    db_name: str

    def get_url(self) -> str:
        url = pydantic.PostgresDsn.build(
            scheme="postgresql",
            username=self.db_user,
            password=self.pwd,
            host=self.host,
            port=self.port,
            path=self.db_name,
        )
        return url.unicode_string()

    def __eq__(self, other):
        if not isinstance(other, DBConfig):
            return NotImplemented
        return (
            self.host == other.host
            and self.port == other.port
            and self.db_name == other.db_name
        )


class S3Config(pydantic_settings.BaseSettings):
    access_key: str
    secret_key: str
    host: str
    port: int
    secure: bool
    public_url_endpoint: str | None = None
    namespace: str = ""

    def __eq__(self, other):
        if not isinstance(other, S3Config):
            return NotImplemented
        return self.host == other.host and self.port == other.port


AvailableReaders = Literal[
    "cdsobs.ingestion.readers.sql.read_header_and_data_tables",
    "cdsobs.ingestion.readers.sql.read_singletable_data",
    "cdsobs.ingestion.readers.cuon.read_cuon_netcdfs",
    "cdsobs.ingestion.readers.cuon_np.read_cuon_netcdfs",
    "cdsobs.ingestion.readers.netcdf.read_flat_netcdfs",
]


Source = NewType("Source", str)
Period = NewType("Period", str)

LatTileConfig = (
    LatTileSize | dict[Source | Period, LatTileSize | dict[Period, LatTileSize]]
)
LonTileConfig = (
    LonTileSize | dict[Source | Period, LonTileSize | dict[Period, LonTileSize]]
)


def year_isin_period(year: int, period: str) -> bool:
    year_start, year_end = period.split("-")
    return int(year_start) <= year <= int(year_end)


def get_year_tile_size(tile_size_dict: dict[str, int], year: int) -> int:
    periods = list(tile_size_dict)
    thisyear_period = [p for p in periods if year_isin_period(year, p)][0]
    result = tile_size_dict[thisyear_period]
    return result


class DatasetConfig(pydantic.BaseModel):
    """
    Dataset-specific configuration variables.

    Parameters
    ----------
    name:
      Dataset name (e.g. insitu-comprehensive-upper-air-observation-network)
    lat_tile_size:
      Size of the latitude partitions.
    lon_tile_size:
      Size of the longitude partitions.
    ingestion_db:
      Input database configuration name. In must be defined in the ingestion_databases
      field of the root level (CDSObsConfig). By default is "main". For datasets that do
      not use an ingestion database it will be ignored.
    reader:
      Function to read the input data
    reader_extra_args:
      Optional. Args to pass to the read function (e.g. a directory). Default is None

    """

    name: str
    lat_tile_size: LatTileConfig
    lon_tile_size: LonTileConfig
    time_tile_size: TimeTileSize = "month"
    reader: AvailableReaders | dict[
        str, AvailableReaders
    ] = "cdsobs.ingestion.readers.sql.read_header_and_data_tables"
    available_cdm_tables: List[str] = DEFAULT_CDM_TABLES_TO_USE
    reader_extra_args: Optional[dict[str, str]] = None
    ingestion_db: str = "main"
    read_with_spatial_batches: bool = False

    def get_tile_size(
        self, kind: Literal["lat", "lon"], source: str, year: int
    ) -> LatTileSize | LonTileSize:
        """
        Handle the possible dependency of the tile sizes on year and source.

        Parameters
        ----------
        name:
          Dataset name (e.g. insitu-comprehensive-upper-air-observation-network)
        kind:
          Wether we want the tile size of the longitudes (lon) or latitudes (lat).
        source:
          The dataset source as defined in the service definition file.
        year:
          The year of data that is being processed.

        """
        tile_size_config: LatTileSize | LonTileSize = getattr(self, f"{kind}_tile_size")

        if isinstance(tile_size_config, int):
            # Tile size is always the same
            result = tile_size_config
        else:
            # Tile size depends on the source, the year or both
            if source in tile_size_config:
                # Tile size depends on the source
                source_tile_size = tile_size_config[source]
                if isinstance(source_tile_size, int):
                    # Tile size depends on source only
                    result = source_tile_size
                else:
                    # Tile size depends on the source and year
                    result = get_year_tile_size(source_tile_size, year)
            else:
                # Tile size depends on the year
                result = get_year_tile_size(tile_size_config, year)

        return result


class CDSObsConfig(pydantic.BaseModel):
    """
    Global configuration of the CADS Observation Catalogue Manager.

    It includes a list of dataset-specific configuration parameters.

    """

    catalogue_db: DBConfig
    s3config: S3Config
    ingestion_databases: Dict[str, DBConfig]
    datasets: List[DatasetConfig]
    cdm_tables_location: Path = _get_default_cdm_tables_location()

    @classmethod
    def from_yaml(cls, config_file: Path) -> "CDSObsConfig":
        """Parse from a YAML file."""
        if config_file is None:
            config_file = Path.home().joinpath(".cdsobs/cdsobs_config.yml")
        if not config_file.exists():
            raise ConfigError("Config file not found")
        with config_file.open() as f:
            config_dict = yaml.safe_load(f)
        return cls(**config_dict)

    @pydantic.field_validator("datasets")
    def ingestion_db_is_defined(cls, v: List[DatasetConfig], info: ValidationInfo):
        for dataset in v:
            if dataset.ingestion_db not in info.data["ingestion_databases"]:
                raise KeyError(
                    f"{dataset.ingestion_db} must be defined in the ingestion databases mapping."
                )
        return v

    def get_dataset(self, name: str) -> DatasetConfig:
        """Return the dataset-specific configuration parameters for a given dataset."""
        return [d for d in self.datasets if d.name == name][0]

    def get_dataset_ingestion_db(self, name: str) -> DBConfig:
        return self.ingestion_databases[self.get_dataset(name).ingestion_db]


def validate_config(config_file: Path):
    """Validate the configuration YAML."""
    with config_file.open() as f:
        config_dict = yaml.safe_load(f)
    return CDSObsConfig(**config_dict)


def read_and_validate_config(cdsobs_config_yml: Path | None) -> CDSObsConfig:
    # read and validate config yaml
    if cdsobs_config_yml is None:
        cdsobs_config_yml = Path.home().joinpath(".cdsobs/cdsobs_config.yml")
    if not Path(cdsobs_config_yml).exists():
        raise ConfigNotFound()
    config = validate_config(cdsobs_config_yml)
    return config
