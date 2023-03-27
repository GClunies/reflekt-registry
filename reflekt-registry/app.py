import os
import sys
from datetime import datetime

from chalice import Chalice
from chalicelib.validator import validate_event
from dateutil import parser
from loguru import logger
from rich.logging import RichHandler
from rich.traceback import install
from segment.analytics import Client as SegmentClient

# Chalice app config
app = Chalice(app_name="reflekt-registry")

# Environment Config
DEBUG = True if os.environ.get("DEBUG") == "true" else False
LOGGING_LEVEL = "DEBUG" if DEBUG else "INFO"
SHOW_LOCALS = True if os.environ.get("SHOW_LOCALS") == "true" else False
install(show_locals=SHOW_LOCALS)  # Rich traceback config

# Logging Config
logger.remove()  # Remove default loguru logger
logger.add(sys.stderr, level=LOGGING_LEVEL)
logger.configure(  # Loguru config
    handlers=[
        {
            "sink": RichHandler(
                rich_tracebacks=True,
                markup=True,
                show_path=False,
                log_time_format="[%X]",
                omit_repeated_times=False,
                tracebacks_show_locals=SHOW_LOCALS,
            ),
            "format": "{message}",
        }
    ],
)


# Segment Clients Config
SEGMENT_CONSUMER_WRITE_KEY = os.environ.get("SEGMENT_CONSUMER_WRITE_KEY")
SEGMENT_DEAD_LETTER_WRITE_KEY = os.environ.get("SEGMENT_DEAD_LETTER_WRITE_KEY")


def log_segment_error(error):
    """Log Segment error for debugging.

    This function is passed to the Segment client as the on_error callback.

    Args:
        error (Any): The Segment error.
    """
    logger.error("Segment error:", error)


# Send valid events to this Segment source
segment_consumer = SegmentClient(
    SEGMENT_CONSUMER_WRITE_KEY,
    debug=DEBUG,
    on_error=log_segment_error,
)

# Send invalid events to this Segment source
segment_dead_letter = SegmentClient(
    SEGMENT_DEAD_LETTER_WRITE_KEY,
    debug=DEBUG,
    on_error=log_segment_error,
)


# API Routes
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
    logger.debug(f"Received {len(tracks)} events. Beginning validation...")

    for track in tracks:  # Queue up the events
        if "schema_id" in track["properties"]:  # Check for schema ID
            schema_id = track["properties"]["schema_id"]
            event = track.get("event")
            anonymous_id = track.get("anonymousId")
            user_id = track.get("userId")
            context = track.get("context", {})
            integrations = track.get("integrations", {})
            properties = track.get("properties", {})

            logger.debug(f"Event properties are: {properties}")

            # For simple debugging, timestamp not required. Set for developer
            timestamp = (
                datetime.now()
                if DEBUG and track.get("timestamp") is None
                else parser.parse(track.get("timestamp"))
            )

            # Validate the event properties against the schema from the registry
            logger.debug(f"Validating event '{track['event']}'...")
            is_valid, schema_errors = validate_event(
                schema_id=schema_id, event_properties=properties
            )

            if is_valid:  # Send to valid Segment source
                logger.debug(
                    f"Event '{track['event']}' PASSED validation. "
                    f"Sending to Segment consumer source..."
                )
                segment_consumer.track(
                    event=event,
                    anonymous_id=anonymous_id,
                    user_id=user_id,
                    context=context,
                    timestamp=timestamp,
                    integrations=integrations,
                    properties=properties,
                )

            else:  # Send to invalid Segment source
                logger.debug(
                    f"Event '{track['event']}' FAILED validation. "
                    f"Sending to Segment dead-letter source..."
                )
                properties["validation_errors"] = schema_errors
                segment_dead_letter.track(
                    event=event,
                    anonymous_id=anonymous_id,
                    user_id=user_id,
                    context=context,
                    timestamp=timestamp,
                    integrations=integrations,
                    properties=properties,
                )

        else:
            logger.error(
                f"Event '{track['event']}' missing required property `schema_id`. "
                f"Sending to Segment dead letter source ..."
            )
            schema_errors = {"'schema_id' is a required property"}
            properties["validation_errors"] = schema_errors
            segment_dead_letter.track(
                event=track.get("event"),
                anonymous_id=track.get("anonymousId"),
                user_id=track.get("userId"),
                context=track.get("context", {}),
                timestamp=parser.parse(track.get("timestamp")),
                integrations=track.get("integrations", {}),
                properties=track.get("properties", {}),
            )

    # Flush clients after processing all events
    logger.debug("Flushing Segment clients...")
    segment_consumer.flush()
    segment_dead_letter.flush()
    logger.debug("Done!")

    return None


# TODO - pass back a response for testing
