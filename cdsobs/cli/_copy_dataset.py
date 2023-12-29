from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, Sequence

import requests
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session
from typer import Option

from cdsobs.cli._utils import CliException, ConfigNotFound, config_yml_typer
from cdsobs.config import CDSObsConfig
from cdsobs.observation_catalogue.database import get_session
from cdsobs.observation_catalogue.models import Catalogue
from cdsobs.observation_catalogue.repositories.cads_dataset import CadsDatasetRepository
from cdsobs.observation_catalogue.repositories.catalogue import CatalogueRepository
from cdsobs.observation_catalogue.schemas.catalogue import CatalogueSchema
from cdsobs.storage import S3Client
from cdsobs.utils.logutils import ConfigError


def copy_dataset(
    cdsobs_config_yml: Path = config_yml_typer,
    dataset: str = Option(..., help="dataset to copy"),
    dest_config_yml: Optional[Path] = Option(
        None,
        help="Path to the cdsobs_config.yml with destination database credentials. "
        "Must contain both s3 and catalogue db credentials. If not provided, "
        "the dataset will be copied inside the initial databases",
    ),
    dest_dataset: Optional[str] = Option(
        None,
        help=(
            "Destination name for the dataset. If not provided, the original name"
            " will be used."
        ),
    ),
):
    """
    Copy all catalogue datasets entries and its S3 assets.

    Choose to copy inside the original databases with a new name (dest_dataset) or
    provide a different Catalogue DB and S3 credentials to insert copies.
    """
    _copy_dataset_impl(cdsobs_config_yml, dataset, dest_config_yml, dest_dataset)


def _copy_dataset_impl(cdsobs_config_yml, dataset, dest_config_yml, dest_dataset):
    check_params(dest_config_yml, dataset, dest_dataset)

    try:
        init_config = CDSObsConfig.from_yaml(cdsobs_config_yml)
    except ConfigError:
        raise ConfigNotFound("Invalid initial configuration file")

    if dest_config_yml is not None:
        try:
            dest_config = CDSObsConfig.from_yaml(dest_config_yml)
        except ConfigError:
            raise ConfigNotFound("Invalid destination configuration file")
        copy_outside(init_config, dest_config, dataset, dest_dataset)
    else:
        copy_inside(init_config, dataset, dest_dataset)


def check_params(dest_config_yml, dataset, dest_dataset):
    """
    Check parameters logic before doing things.

    Parameters
    ----------
    dest_config_yml:
      destination db credentials
    dataset:
      inital dataset name
    dest_dataset:
      destination dataset name
    -------

    """
    if dest_config_yml is None and dest_dataset is None:
        raise CliException(
            "Destination config and/or destination dataset name must be provided"
        )
    if dest_dataset is not None:
        if (
            not len(dest_dataset)
            or dest_dataset.replace(" ", "_").lower() != dest_dataset
        ):
            raise CliException(
                "Dataset name does not follow CDS conventions (lowercase and _ for "
                "spaces) or is empty"
            )

        if dest_dataset == dataset:
            raise CliException(
                "Destination dataset must be different from original"
                " dataset. To preserve dataset name, do not fill this "
                "parameter"
            )


def copy_inside(init_config, dataset, dest_dataset):
    """
    Copy inside.

    Copy Catalogue DB entries changing dataset name and copy S3 objects into a
    different bucket

    Parameters
    ----------
    init_config:
      initial database configuration
    dataset:
      old dataset name
    dest_dataset:
      new dataset name
    -------

    """
    init_s3client = S3Client.from_config(init_config.s3config)
    with get_session(init_config.catalogue_db) as init_session:
        entries = CatalogueRepository(init_session).get_by_dataset(dataset)
        new_assets = s3_copy(init_s3client, entries, dest_dataset)
        try:
            catalogue_copy(init_session, entries, new_assets, dest_dataset)
        except (Exception, KeyboardInterrupt):
            s3_rollback(init_s3client, new_assets)
            raise


def copy_outside(init_config, dest_config, dataset, dest_dataset):
    """
    Copy outside.

    Compare init_config and dest_config. If the Catalogue DB and/or the S3 are
    different, export to the new destination. Otherwise, same behaviour as in
    copy_inside function.

    Parameters
    ----------
    init_config:
      initial database configuration
    dest_config:
      destination database configuration
    dataset:
      old dataset name
    dest_dataset:
      destination dataset name
    -------

    """
    s3client = S3Client.from_config(init_config.s3config)
    with get_session(init_config.catalogue_db) as init_session:
        entries = CatalogueRepository(init_session).get_by_dataset(dataset)
    if init_config.s3config == dest_config.s3config:
        new_assets = s3_copy(s3client, entries, dest_dataset)
    else:
        # get new destination client as current client
        s3client = S3Client.from_config(dest_config.s3config)
        new_assets = s3_export(s3client, entries, dest_dataset)
    try:
        if init_config.catalogue_db == dest_config.catalogue_db:
            catalogue_copy(init_session, entries, new_assets, dest_dataset)
        else:
            # open new destination session
            with get_session(dest_config.catalogue_db) as dest_session:
                catalogue_copy(dest_session, entries, new_assets, dest_dataset)
    except (Exception, KeyboardInterrupt):
        s3_rollback(s3client, new_assets)
        raise


def catalogue_copy(
    catalogue_session: Session, entries: Sequence[Catalogue], new_assets, dest_dataset
):
    # Create the dataset in the CadsDatasets table
    cads_dataset_repo = CadsDatasetRepository(catalogue_session)
    cads_dataset_repo.create_dataset(dest_dataset)
    # copy dataset entries but with different dataset name and asset.
    new_schemas = []
    for entry, asset in zip(entries, new_assets):
        # This is needed to load the constraints as it is a deferred attribute.
        # However if we load them the other attributes will dissappear from __dict__
        # There is no way apparently if doing this better in sqlalchemy
        entry_dict = {
            col.name: getattr(entry, col.name) for col in entry.__table__.columns
        }
        entry_dict_json = jsonable_encoder(entry_dict)
        entry_dict_json.pop("id")
        entry_dict_json.pop("dataset")
        entry_dict_json.pop("asset")
        new_schemas.append(
            CatalogueSchema(dataset=dest_dataset, asset=asset, **entry_dict_json)
        )
    CatalogueRepository(catalogue_session).create_many(new_schemas)


def s3_copy(s3client, entries, dest_dataset):
    """Copy into another bucket."""
    assets = [e.asset for e in entries]
    new_assets = []
    try:
        for asset in assets:
            bucket, name = asset.split("/")
            s3client.create_directory(dest_dataset)
            s3client.copy_file(bucket, name, dest_dataset, name)
            new_assets.append(s3client.get_asset(dest_dataset, name))
    except (Exception, KeyboardInterrupt):
        s3_rollback(s3client, new_assets)
        raise
    return new_assets


def s3_rollback(s3_client, assets):
    for asset in assets:
        bucket, name = asset.split("/")[-2:]
        s3_client.delete_file(bucket, name)


def s3_export(s3client: S3Client, entries, dest_dataset):
    """Download from one S3 and upload to another."""
    object_urls = [s3client.get_url_from_asset(e.asset) for e in entries]
    new_assets = []
    try:
        dest_bucket = s3client.get_bucket_name(dest_dataset)
        s3client.create_directory(dest_bucket)
        for object_url in object_urls:
            old_bucket, name = object_url.split("/")[-2:]
            response = requests.get(object_url, stream=True)
            response.raise_for_status()
            with (
                NamedTemporaryFile(dir="/dev/shm") as ntf,
                BytesIO(response.content) as bc,
            ):
                ntf.write(bc.read())
                ntf.flush()
                new_assets.append(
                    s3client.upload_file(dest_bucket, name, Path(ntf.name))
                )
        return new_assets
    except (Exception, KeyboardInterrupt):
        s3_rollback(s3client, new_assets)
        raise
