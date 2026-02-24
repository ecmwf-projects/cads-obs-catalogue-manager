import os
from pathlib import Path
from typing import Dict

import pydantic
import pydantic_settings
import yaml

from cdsobs.utils.exceptions import ConfigError, ConfigNotFound


def _get_default_cdm_tables_location() -> Path:
    return _get_default_location("CDM_TABLES_LOCATION")


def _get_default_cads_obs_config_location() -> Path:
    return _get_default_location("CADS_OBS_INSITU_LOCATION")


def _get_default_location(env_varname: str) -> Path:
    if env_varname in os.environ:
        return Path(os.environ[env_varname])
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

    def __eq__(self, other: object) -> bool:
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, S3Config):
            return NotImplemented
        return (
            self.host == other.host
            and self.port == other.port
            and self.access_key == other.access_key
        )


class CDSObsConfig(pydantic.BaseModel):
    """Global configuration of the CADS Observation Catalogue Manager."""

    catalogue_db: DBConfig
    s3config: S3Config
    ingestion_databases: Dict[str, DBConfig]
    cdm_tables_location: Path = _get_default_cdm_tables_location()
    cads_obs_config_location: Path = _get_default_cads_obs_config_location()

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


def validate_config(config_file: Path) -> CDSObsConfig:
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
