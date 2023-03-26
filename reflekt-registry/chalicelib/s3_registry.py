import io
import json
import logging
import os
from pprint import pprint

import boto3

_REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET")
_SCHEMA_REGISTRY = None

logger = logging.getLogger("app")


class S3SchemaRegistry:
    """Class to interface with a schema registry hosted in S3.

    Schema registry is an S3 bucket with a folder structure mimicking a
    Reflekt project (https://github.com/GClunies/Reflekt). Schemas are accessed
    by their schema ID.
    """

    def __init__(self):
        """Initialize S3 client and logger from AWS Chalice App.

        Args:
            logger (logging.Logger): Logger from AWS Chalice App.
            debug (bool): Whether to enable debug logging. Defaults to False.
        """
        self.s3 = boto3.client("s3")
        # self.log = logger
        # self.debug = debug

    def get_schema(self, schema_id: str) -> dict:
        """Get schema from registry based on provided schema ID.

        Args:
            schema_id (str): The schema ID.

        Returns:
            dict: The schema.
        """
        app.log.debug(f"Searching schema registry for schema ID: {schema_id}")
        object_key = f"schemas/{schema_id}"

        app.log.debug(
            f"Get schema from S3 bucket: {_REGISTRY_BUCKET} at path: {object_key}"
        )
        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket=_REGISTRY_BUCKET, Key=object_key, Fileobj=bytes_buffer
        )
        byte_value = bytes_buffer.getvalue()
        schema = json.loads(byte_value.decode("utf-8"))
        app.log.debug(f"Loaded schema from S3. Schema is: {schema}")

        return schema


def get_s3_schema_registry() -> S3SchemaRegistry:
    """Get an S3 schema registry instance, initializing it if necessary.

    Returns:
        S3SchemaRegistry: The S3 schema registry interface class to get schemas
            from S3 (uses boto3).
    """
    global _SCHEMA_REGISTRY

    if _SCHEMA_REGISTRY is None:  # Initialize schema registry if necessary
        _SCHEMA_REGISTRY = S3SchemaRegistry()

    return _SCHEMA_REGISTRY
