import os

import boto3
import segment.analytics as segment_analytics
from chalice import Chalice
from chalicelib.db import DynamoRegistryDB, RegistryDB
from dateutil import parser

# from segment.analytics.request import APIError

# COnfigure AWS Chalice app
app = Chalice(app_name="reflekt-registry")
app.debug = True if os.environ.get("DEBUG") == "true" else False

_REGISTRY_DB = None
_SUPPORTED_SCHEMA_EXTENSIONS = [".json"]


def get_registry_db() -> RegistryDB:
    """Get the registry database.

    Returns:
        RegistryDB: The registry database.
    """
    global _REGISTRY_DB

    if _REGISTRY_DB is None:
        _REGISTRY_DB = DynamoRegistryDB(
            boto3.resource("dynamodb").Table(os.environ.get("REGISTRY_TABLE"))
        )

    return _REGISTRY_DB


# API routes
@app.route("/")
def index():
    """Return a welcome message.

    Returns:
        str: The welcome message.
    """
    return "Reflekt registry up and running!"


def validate_event_json():
    """Validate event JSON properties against schema stored in registry.

    Returns:
        bool: True if valid, False otherwise.
    """
    # TODO - validate the event JSON
    pass


def on_segment_error(error):
    """Log debugging error for Segment. Passed to Segment client.

    Args:
        error (Any): The error.
    """
    app.log.error("An error occurred sending events to Segment:", error)


# Configure the Segment client
segment_analytics.write_key = os.environ.get("SEGMENT_WRITE_KEY", "")
segment_analytics.debug = True if app.debug else False
segment_analytics.on_error = on_segment_error


@app.route("/v1/batch", methods=["POST"])
def proxy_segment_v1_batch() -> None:
    """Forward the event from Lambda API proxy to Segment."""

    tracks = app.current_request.json_body["batch"]

    for track in tracks:  # Queue up the events
        segment_analytics.track(
            event=track.get("event", None),
            anonymous_id=track.get("anonymousId", None),
            user_id=track.get("userId", None),
            context=track.get("context", {}),
            timestamp=parser.parse(track.get("timestamp", None)),
            integrations=track.get("integrations", {}),
            properties=track.get("properties", {}),
        )

    segment_analytics.flush()  # Flush queued events to Segment
