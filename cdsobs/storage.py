import json
from abc import ABC, abstractmethod
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from cdsobs.config import S3Config
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


class StorageClient(ABC):
    """Abstract interface for an storage client."""

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def base(self):
        pass

    @abstractmethod
    def get_object_url(self, directory: str, name: str):
        pass

    @abstractmethod
    def get_asset(self, directory: str, name: str):
        pass

    @abstractmethod
    def get_url_from_asset(self, asset: str):
        pass

    @abstractmethod
    def list_buckets(self):
        pass

    @abstractmethod
    def list_directory_objects(self, bucket: str):
        pass

    @abstractmethod
    def create_directory(self, name: str):
        pass

    @abstractmethod
    def upload_file(
        self, destination_bucket: str, object_name: str, file_to_upload: Path
    ):
        pass

    @abstractmethod
    def delete_file(self, destination_bucket: str, object_name: str):
        pass

    @abstractmethod
    def copy_file(
        self,
        init_bucket: str,
        init_name: str,
        destination_bucket: str,
        destination_name: str | None = None,
    ):
        pass

    @abstractmethod
    def object_exists(self, bucket, name):
        pass

    def get_bucket_name(self, dataset_name: str, max_allowed_bucket_length: int = 63):
        pass


class S3Client(StorageClient):
    def __init__(
        self,
        host: str,
        port: int,
        access_key: str,
        secret_key: str,
        namespace: str,
        secure: bool = False,
        public_url_endpoint: str | None = None,
    ):
        schema: str = "https" if secure else "http"
        self._base: str = f"{schema}://{host}:{port}"
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=self._base,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            use_ssl=secure,
        )
        self.public_url_endpoint = public_url_endpoint
        if self.public_url_endpoint is None:
            self.public_url_base = self.base
        else:
            self.public_url_base = f"{self.base}/{self.public_url_endpoint}"
        self.namespace = namespace

    @property
    def base(self) -> str:
        return self._base

    def get_object_url(self, bucket_name: str, name: str) -> str:
        return f"{self.public_url_base}/{bucket_name}/{name}"

    def get_asset(self, bucket_name: str, name: str) -> str:
        """Return asset as stored on the catalogue db."""
        return f"{bucket_name}/{name}"

    def get_url_from_asset(self, asset: str) -> str:
        bucket_name, name = asset.split("/")
        return self.get_object_url(bucket_name, name)

    def list_buckets(self) -> list[str]:
        return [b.name for b in self.s3.buckets.all()]

    def list_directory_objects(self, bucket: str) -> list[str]:
        return [o.key for o in self.s3.Bucket(bucket).objects.all()]

    def create_directory(self, bucket_name: str):
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError:
            self.s3.create_bucket(Bucket=bucket_name)
            policy = self.get_read_only_policy(bucket_name)
            self.s3.meta.client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
        else:
            logger.info("Bucket exists")

    def upload_file(
        self, destination_bucket: str, object_name: str, file_to_upload: Path
    ) -> str:
        self.s3.Bucket(destination_bucket).upload_file(str(file_to_upload), object_name)
        return self.get_asset(destination_bucket, object_name)

    def download_file(self, bucket_name, object_name, ofile):
        self.s3.Object(bucket_name, object_name).download_file(ofile)

    def delete_file(self, destination_bucket: str, object_name: str):
        self.s3.Object(destination_bucket, object_name).delete()

    def delete_bucket(self, bucket: str):
        self.s3.Bucket(bucket).delete()

    def copy_file(
        self,
        init_bucket: str,
        init_name: str,
        destination_bucket: str,
        destination_name: str | None = None,
    ):
        if destination_name is None:
            destination_name = init_name
        copy_source = {"Bucket": init_bucket, "Key": init_name}
        self.s3.Bucket(destination_bucket).copy(copy_source, destination_name)

    def object_exists(self, bucket, name) -> bool:
        try:
            # (head request, it doesn't load the full object)
            self.s3.Object(bucket, name).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise
        else:
            return True

    @staticmethod
    def get_read_only_policy(bucket_name: str) -> str:
        """
        Set read only policy for anonymous users.

        So credentials are not required to download the objects.
        """
        policy = {
            "Statement": [
                {
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                    ],
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Resource": f"arn:aws:s3:::{bucket_name}",
                    "Sid": "",
                },
                {
                    "Action": [
                        "s3:GetObject",
                    ],
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                    "Sid": "",
                },
            ],
            "Version": "2012-10-17",
        }
        return json.dumps(policy)

    def get_bucket_name(
        self, dataset_name: str, max_allowed_bucket_length: int = 63
    ) -> str:
        """Will truncate the dataset name if it is longer than 63 characters."""
        new_name = self.namespace + "-" + dataset_name
        if len(new_name) > max_allowed_bucket_length:
            logger.warning(
                f"By default bucket is named as the env + dataset, but {new_name} is"
                f"too long (>{max_allowed_bucket_length} characters). It will be "
                f"truncated."
            )
        return new_name[:max_allowed_bucket_length]

    @staticmethod
    def from_config(config: S3Config) -> "S3Client":
        client = S3Client(
            config.host,
            config.port,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            public_url_endpoint=config.public_url_endpoint,
            namespace=config.namespace,
        )
        return client
