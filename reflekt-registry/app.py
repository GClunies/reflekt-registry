import json
import os
from datetime import datetime
from pathlib import Path

import boto3
import segment.analytics as segment_analytics
from chalice import Chalice

# from chalicelib.schema_registry import S3SchemaRegistry
from dateutil import parser
from jsonschema import Draft7Validator

# from segment.analytics.request import APIError

# COnfigure AWS Chalice app
app = Chalice(app_name="reflekt-registry")
app.debug = True if os.environ.get("DEBUG") == "true" else False

_SCHEMA_REGISTRY = None
_SUPPORTED_SCHEMA_EXTENSIONS = [".json"]


class S3SchemaRegistry:
    """Class for interacting with a simple S3 schema registry."""

    def __init__(self):
        """Initialize the S3SchemaRegistry class."""
        self._client = boto3.client("s3")

    def get_schema(self, schema_id: str) -> dict:
        """Get the schema from local cache or schema registry.

        Schemas are cached locally in /tmp/<schema_id>.json for performance.

        Args:
            schema_id (str): The schema ID.

        Returns:
            dict: The schema.
        """
        bucket = os.environ.get("REGISTRY_BUCKET")
        key = f"schemas/{schema_id}"
        tmp_file = Path(f"/tmp/{schema_id}")

        app.log.debug(f"BUCKET: {bucket}")
        app.log.debug(f"KEY: {key}")
        app.log.debug(f"TMP_FILE: {str(tmp_file)}")

        if tmp_file.exists():
            with open(tmp_file, "r") as schema_file:
                schema = json.load(schema_file)
        else:
            response = self._client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            schema = json.loads(content)

            if not tmp_file.parent.exists():
                tmp_file.parent.mkdir(parents=True)

            with open(tmp_file, "w") as schema_file:  # Cache locally
                json.dump(schema, schema_file)

        return schema


def get_schema_registry() -> S3SchemaRegistry:
    """Get the schema registry.

    Returns:
        SchemaRegistry: The schema registry.
    """
    global _SCHEMA_REGISTRY

    # Check if schema registry is already initialized. If not, initialize it
    if _SCHEMA_REGISTRY is None:
        _SCHEMA_REGISTRY = S3SchemaRegistry()

    return _SCHEMA_REGISTRY


# API routes
@app.route("/")
def index():
    """Return a welcome message.

    Returns:
        str: The welcome message.
    """
    return "🪞 Reflekt registry running! 🪞"


def validate_properties(schema_id: str, properties: dict):
    """Validate event properties against schema in registry.

    Args:
        schema_id (str): The schema ID.
        properties (dict): The event properties.

    Returns:
        bool: True if valid, False otherwise.
    """
    errors = []
    schema_registry = get_schema_registry()
    schema = schema_registry.get_schema(schema_id)
    app.log.debug(f"SCHEMA PROPERTIES: {schema['properties']}")
    app.log.debug(f"EVENT PROPERTIES: {properties}")
    validator = Draft7Validator(schema=schema)

    if not validator.is_valid(instance=properties):
        errors = [
            f"{error.absolute_path[0]}: {error.message}"
            for error in validator.iter_errors(instance=properties)
        ]
        return False, errors
    else:
        return True, errors


def log_segment_error(error):
    """Log debugging error for Segment. Passed to Segment client.

    Args:
        error (Any): The error.
    """
    app.log.error("An error occurred sending events to Segment:", error)


# Configure the Segment client
segment_analytics.debug = True if app.debug else False
segment_analytics.on_error = log_segment_error


@app.route("/v1/batch", methods=["POST"])
def proxy_segment_v1_batch() -> None:
    """Forward the event from Lambda API proxy to Segment."""
    tracks = app.current_request.json_body["batch"]

    for track in tracks:  # Queue up the events
        event = track.get("event", None)
        anonymous_id = track.get("anonymousId", None)
        user_id = track.get("userId", None)
        context = track.get("context", {})
        integrations = track.get("integrations", {})
        properties = track.get("properties", {})
        # timestamp = parser.parse(track.get("timestamp", None))

        if app.debug:
            timestamp = datetime.now()
        else:
            timestamp = parser.parse(track.get("timestamp", None))

        # Get the schema ID from the event properties
        schema_id = properties.get("schema_id", None)
        app.log.debug(f"Schema ID: {schema_id}")
        # Validate the event properties against the schema from the registry
        is_valid, schema_errors = validate_properties(
            schema_id=schema_id, properties=properties
        )

        if is_valid:
            segment_analytics.write_key = os.environ.get("SEGMENT_WRITE_KEY", "")
            segment_analytics.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )
            segment_analytics.flush()  # Flush queued events to Segment

        else:
            segment_analytics.write_key = os.environ.get(
                "SEGMENT_WRITE_KEY_INVALID", ""
            )
            # Append schema errors to event properties
            properties["validation_errors"] = schema_errors
            segment_analytics.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )
            segment_analytics.flush()  # Flush queued events to Segment
