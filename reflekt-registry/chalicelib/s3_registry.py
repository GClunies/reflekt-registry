import io
import json
import os

import boto3
from loguru import logger

REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET")
SCHEMA_REGISTRY = None


class S3SchemaRegistry:
    """Class to interface with a schema registry hosted in S3.

    Schema registry is an S3 bucket with a folder structure mimicking a
    Reflekt project (https://github.com/GClunies/Reflekt). Schemas are accessed
    by their schema ID.
    """

    def __init__(self):
        """Initialize S3 client for S3 schema registry."""
        self.s3 = boto3.client("s3")

    def get_schema(self, schema_id: str) -> dict:
        """Get schema from registry based on provided schema ID.

        Args:
            schema_id (str): The schema ID.

        Returns:
            dict: The schema.
        """
        logger.debug(f"Searching schema registry for schema ID: {schema_id}")

        object_key = f"schemas/{schema_id}"
        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket=REGISTRY_BUCKET, Key=object_key, Fileobj=bytes_buffer
        )
        byte_value = bytes_buffer.getvalue()
        schema = json.loads(byte_value.decode("utf-8"))

        logger.debug(f"Found schema in S3 at '{REGISTRY_BUCKET}/{object_key}'.")
        logger.debug(f"Schema is:\n{schema}")

        return schema


def get_s3_schema_registry() -> S3SchemaRegistry:
    """Get an S3 schema registry instance, initializing it if necessary.

    Returns:
        S3SchemaRegistry: The S3 schema registry interface class to get schemas
            from S3 (uses boto3).
    """
    logger.debug("Getting schema registry...")
    global SCHEMA_REGISTRY

    logger.debug("Checking for existing schema registry connection...")

    if SCHEMA_REGISTRY is None:  # Initialize schema registry if necessary
        logger.debug("Schema registry connection not found. Initializing...")
        SCHEMA_REGISTRY = S3SchemaRegistry()
        logger.debug("Schema registry initialized.")
    else:
        logger.debug("Schema registry already initialized.")

    return SCHEMA_REGISTRY
