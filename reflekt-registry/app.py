import os
from datetime import datetime

from chalice import Chalice
from chalicelib.validator import validate_event
from dateutil import parser
from segment.analytics import Client as SegmentClient

# ----- AWS CHALICE APP CONFIG -----
app = Chalice(app_name="reflekt-registry")
app.debug = True if os.environ.get("DEBUG") == "true" else False

_SCHEMA_REGISTRY = None

# ----- SEGMENT CLIENT CONFIGURATION -----
_SEGMENT_WRITE_KEY_VALID = os.environ.get("SEGMENT_WRITE_KEY_VALID")
_SEGMENT_WRITE_KEY_INVALID = os.environ.get("SEGMENT_WRITE_KEY_INVALID")


def log_segment_error(error):
    """Log Segment error for debugging.

    This function is passed to the Segment client as the on_error callback.

    Args:
        error (Any): The Segment error.
    """
    app.log.error("Segment error:", error)


valid_s3_client = SegmentClient(
    _SEGMENT_WRITE_KEY_VALID,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)

invalid_s3_client = SegmentClient(
    _SEGMENT_WRITE_KEY_INVALID,
    debug=True if app.debug else False,
    on_error=log_segment_error,
)


# ----- API ROUTES -----
@app.route("/")
def index():
    """Return a welcome message.

    Returns:
        str: The welcome message.
    """
    return "🪞 Reflekt registry running! 🪞"


@app.route("/validate/segment", methods=["POST"])
def validate_segment() -> None:
    """Validate Segment event(s) and forward to Segment.

    Validated events are sent to the Segment source specified by the
    SEGMENT_WRITE_KEY_VALID environment variable.

    Invalid events are sent to the Segment source specified by the
    SEGMENT_WRITE_KEY_INVALID environment variable.
    """
    tracks = app.current_request.json_body["batch"]

    for track in tracks:  # Queue up the events
        if "schema_id" not in track["properties"]:  # Check for schema ID
            app.log.error("Event missing `schema_id` property. Skipping event:", track)
            continue  # Next loop iteration

        schema_id = track["properties"]["schema_id"]
        event = track.get("event")
        anonymous_id = track.get("anonymousId")
        user_id = track.get("userId")
        context = track.get("context", {})
        integrations = track.get("integrations", {})
        properties = track.get("properties", {})
        app.log.debug(f"Event properties are: {properties}")

        # For simple debugging, timestamp not required. Set for developer
        timestamp = (
            datetime.now()
            if app.debug and track.get("timestamp") is None
            else parser.parse(track.get("timestamp"))
        )

        # Validate the event properties against the schema from the registry
        is_valid, schema_errors = validate_event(
            schema_id=schema_id, event_properties=properties
        )

        if is_valid:  # Send to valid Segment source
            valid_s3_client.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )

        else:  # Send to invalid Segment source
            properties["validation_errors"] = schema_errors
            invalid_s3_client.track(
                event=event,
                anonymous_id=anonymous_id,
                user_id=user_id,
                context=context,
                timestamp=timestamp,
                integrations=integrations,
                properties=properties,
            )

    # Flush clients after processing all events
    valid_s3_client.flush()
    invalid_s3_client.flush()
