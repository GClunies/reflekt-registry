import json
import os
from datetime import datetime
from pathlib import Path

import boto3

# import segment.analytics as segment_analytics
from chalice import Chalice

# from chalicelib.schema_registry import S3SchemaRegistry
from dateutil import parser
from jsonschema import Draft7Validator
from segment.analytics import Client as SegmentClient

# from segment.analytics.request import APIError

# COnfigure AWS Chalice app
app = Chalice(app_name="reflekt-registry")
app.debug = True if os.environ.get("DEBUG") == "true" else False

_SCHEMA_REGISTRY = None
_SUPPORTED_SCHEMA_EXTENSIONS = [".json"]


# Segment Configuration
def log_segment_error(error):
    """Log debugging error for Segment. Passed to Segment client.

    Args:
        error (Any): The error.
    """
    app.log.error("An error occurred sending events to Segment:", error)


SEGMENT_WRITE_KEY = os.environ.get("SEGMENT_WRITE_KEY")
SEGMENT_WRITE_KEY_INVALID = os.environ.get("SEGMENT_WRITE_KEY_INVALID")


valid_client = SegmentClient(
    SEGMENT_WRITE_KEY,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)

invalid_client = SegmentClient(
    SEGMENT_WRITE_KEY_INVALID,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)


# Schema Registry
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
            with open(tmp_file, "r", encoding="utf-8") as schema_file:
                schema = json.load(schema_file)

            app.log.debug(f"Found schema in cache: {str(tmp_file)}")

        else:
            response = self._client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            schema = json.loads(content)

            app.log.debug(f"Found schema bucket: {bucket} at path: {key}")

            if not tmp_file.parent.exists():
                tmp_file.parent.mkdir(parents=True)

            with open(tmp_file, "w", encoding="utf-8") as schema_file:  # Cache locally
                json.dump(schema, schema_file)

            app.log.debug(f"Cached schema locally at: {str(tmp_file)}")

        return schema


def get_schema_registry() -> S3SchemaRegistry:
    """Get the schema registry.

    Returns:
        S3SchemaRegistry: The S3 schema registry.
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


def validate_properties(schema_id: str, event_properties: dict):
    """Validate event properties against schema in registry.

    Args:
        schema_id (str): The schema ID.
        event_properties (dict): The event properties.

    Returns:
        bool: True if valid, False otherwise.
    """
    errors = []
    schema_registry = get_schema_registry()
    schema = schema_registry.get_schema(schema_id)

    # HACK:
    # For unknown reasons, validator.is_valid() errors when validating
    # against the schema_id property (something to di with backslashes at start
    # and end fo string). This is a workaround to remove the schema_id from
    # the schema and event properties before validating.
    schema["properties"].pop("schema_id", None)
    schema["required"] = [prop for prop in schema["required"] if prop != "schema_id"]
    event_properties.pop("schema_id", None)

    validator = Draft7Validator(schema=schema)

    if not validator.is_valid(event_properties):
        errors = [
            f"{error.message}"
            for error in sorted(validator.iter_errors(event_properties), key=str)
        ]
        return False, errors
    else:
        return True, errors


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
        # Validate the event properties against the schema from the registry
        is_valid, schema_errors = validate_properties(
            schema_id=schema_id, event_properties=properties
        )

        if is_valid:
            valid_client.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )
            valid_client.flush()  # Shutdown the client

        else:
            properties["validation_errors"] = schema_errors
            invalid_client.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )
            invalid_client.flush()  # Shutdown the client
