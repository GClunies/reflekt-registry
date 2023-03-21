import json
import os
from datetime import datetime
from pathlib import Path
from pprint import pprint

import boto3
from chalice import Chalice

# from chalicelib.schema_registry import S3SchemaRegistry
from dateutil import parser
from jsonschema import Draft7Validator
from segment.analytics import Client as SegmentClient

# ----- AWS CHALICE APP CONFIG -----
app = Chalice(app_name="reflekt-registry")
app.debug = True if os.environ.get("DEBUG") == "true" else False

_SCHEMA_REGISTRY = None
_REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET")


# ----- SEGMENT CLIENT CONFIGURATION -----
def log_segment_error(error):
    """Log Segment error for debugging.

    This function is passed to the Segment client as the on_error callback.

    Args:
        error (Any): The Segment error.
    """
    app.log.error("An error occurred sending events to Segment:", error)


_SEGMENT_WRITE_KEY_VALID = os.environ.get("SEGMENT_WRITE_KEY")
_SEGMENT_WRITE_KEY_INVALID = os.environ.get("SEGMENT_WRITE_KEY_INVALID")


valid_client = SegmentClient(
    _SEGMENT_WRITE_KEY_VALID,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)

invalid_client = SegmentClient(
    _SEGMENT_WRITE_KEY_INVALID,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)


# ----- S3 SCHEMA REGISTRY -----
class S3SchemaRegistry:
    """Class to interface with a schema registry hosted in S3.

    Here, the schema registry is simply an S3 bucket with a folder structure
    mimicking a reflekt project (https://github.com/GClunies/Reflekt). Schemas
    are accessed by their schema ID.

    This class also handles local caching of schemas for performance.
    """

    def __init__(self):
        """Initialize an S3 client."""
        self._client = boto3.client("s3")

    def get_schema(self, schema_id: str) -> dict:
        """Get corresponding schema from schema registry for a given schema ID.

        For performance, schemas are cached locally in /tmp/<schema_id>.json.

        Args:
            schema_id (str): The schema ID.

        Returns:
            dict: The schema.
        """
        app.log.debug(f"Searching schema registry for schema ID: {schema_id}")
        key = f"schemas/{schema_id}"
        tmp_file = Path(f"/tmp/{schema_id}")

        if tmp_file.exists():  # Load schema from cache
            app.log.debug(f"Get schema from cache at: {str(tmp_file)}")

            with tmp_file.open("r", encoding="utf-8") as schema_file:
                schema = json.load(schema_file)

            app.log.debug("Loaded schema from cache. Schema is:")
            pprint(schema) if app.debug else None  # Pretty print schema

        else:
            app.log.debug(
                f"Get schema from S3 bucket: {_REGISTRY_BUCKET} at path: {key}"
            )
            response = self._client.get_object(Bucket=_REGISTRY_BUCKET, Key=key)
            content = response["Body"].read().decode("utf-8")
            schema = json.loads(content)

            app.log.debug("Loaded schema from S3. Schema is:")
            pprint(schema) if app.debug else None  # Pretty print schema

            if not tmp_file.parent.exists():
                tmp_file.parent.mkdir(parents=True)

            app.log.debug(f"Caching schema at: {str(tmp_file)}")

            with open(tmp_file, "w", encoding="utf-8") as schema_file:  # Cache locally
                json.dump(schema, schema_file)

            app.log.debug("Cached schema for future use")

        return schema


def get_schema_registry() -> S3SchemaRegistry:
    """Get an instance of S3SchemaRegistry class, initializing if necessary.

    S3SchemaRegistry is used to access schemas from S3 via boto3 S3 client.

    Returns:
        S3SchemaRegistry: Instance of S3SchemaRegistry class.
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
    """Validate event properties against the schema in the registry.

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
        event = track.get("event")
        anonymous_id = track.get("anonymousId")
        user_id = track.get("userId")
        context = track.get("context", {})
        integrations = track.get("integrations", {})
        properties = track.get("properties", {})

        # For local debugging, don't require timestamp in the request. Set for developer
        if app.debug and track.get("timestamp") is None:
            timestamp = datetime.now()
        else:
            timestamp = parser.parse(track.get("timestamp"))

        # Get the schema ID from the event properties
        schema_id = properties.get("schema_id", None)
        # Validate the event properties against the schema from the registry
        is_valid, schema_errors = validate_properties(
            schema_id=schema_id, event_properties=properties
        )

        # Send the event to the appropriate Segment source based on validation
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
