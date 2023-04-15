import os
import sys
from datetime import datetime

from basicauth import decode
from chalice import AuthResponse, AuthRoute, Chalice, Response
from chalicelib.errors import log_segment_error
from chalicelib.validator import validate_event
from dateutil import parser
from loguru import logger
from rich.logging import RichHandler
from rich.traceback import install
from segment.analytics import Client as SegmentClient
from segment.analytics.request import APIError

# Chalice app config
app = Chalice(app_name="reflekt-registry")

# Environment Config
DEBUG = True if os.environ.get("DEBUG") == "true" else False
LOGGING_LEVEL = "DEBUG" if DEBUG else "INFO"
SHOW_LOCALS = True if os.environ.get("SHOW_LOCALS") == "true" else False
install(show_locals=SHOW_LOCALS)  # Rich traceback config
REGISTRY_WRITE_KEY = os.environ.get("REGISTRY_WRITE_KEY")
SEGMENT_WRITE_KEY_VALID = os.environ.get("SEGMENT_WRITE_KEY_VALID")
SEGMENT_WRITE_KEY_INVALID = os.environ.get("SEGMENT_WRITE_KEY_INVALID")


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


# Authentication Config
@app.authorizer()
def segment_basic_auth(auth_request) -> AuthResponse:
    """Authenticate the request using Basic Auth.

    Args:
        auth_request (AuthRequest): The request to authenticate.

    Returns:
        AuthResponse: The response to the authentication request.
    """
    username, password = decode(auth_request.token)

    # Check if the segment write key is valid
    if username == REGISTRY_WRITE_KEY and password == "":
        return AuthResponse(routes="/*", principal_id=username)

    return AuthResponse(routes=[], principal_id=None)


# API Routes
@app.route("/")
def index():
    """Return a welcome message.

    Returns:
        Response: The response to the request.
    """
    return Response(
        status_code=200,
        body={"hello": "Reflekt serverless registry!"},
        headers={"Content-Type": "application/json"},
    )


def health_check() -> None:
    """Return a health check response.

    Returns:
        Response: The response to the request.
    """
    return Response(
        status_code=200,
        body="🪞 Reflekt Registry running! 🪞",
        headers={"Content-Type": "text/plain"},
    )


# Segment client config
segment_client_valid = SegmentClient(  # Valid events to this Segment source
    SEGMENT_WRITE_KEY_VALID,
    debug=DEBUG,
    on_error=log_segment_error,
)
segment_client_invalid = SegmentClient(  # Invalid events to this Segment source
    SEGMENT_WRITE_KEY_INVALID,
    debug=DEBUG,
    on_error=log_segment_error,
)


# TODO: maybe we should handle each track individually instead of batching?
# Segment routes
@app.route("/v1/batch", methods=["POST"], authorizer=segment_basic_auth)
def validate_segment() -> None:
    """Validate Segment event(s) and forward to Segment Consumer.

    Validated events are sent to the Segment source specified by the
    SEGMENT_WRITE_KEY_VALID environment variable.

    Invalid events are sent to the Segment source specified by the
    SEGMENT_WRITE_KEY_INVALID environment variable.

    Returns:
        None: The response is sent to the Segment source.
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
                    f"Sending to VALID Segment source..."
                )
                segment_client_valid.track(
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
                    f"Sending to INVALID Segment source..."
                )
                properties["validation_errors"] = schema_errors
                segment_client_invalid.track(
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
                f"Sending to INVALID Segment source..."
            )
            schema_errors = {"'schema_id' is a required property"}
            properties["validation_errors"] = schema_errors
            segment_client_invalid.track(
                event=track.get("event"),
                anonymous_id=track.get("anonymousId"),
                user_id=track.get("userId"),
                context=track.get("context", {}),
                timestamp=parser.parse(track.get("timestamp")),
                integrations=track.get("integrations", {}),
                properties=track.get("properties", {}),
            )

    # Flush clients after processing all events
    try:
        logger.debug("Flushing Segment clients...")
        segment_client_valid.flush()
        segment_client_invalid.flush()
        logger.debug("Done!")

        return Response(
            status_code=200,
            body="data uploaded successfully",
            headers={"Content-Type": "text/plain"},
        )

    except APIError as e:
        logger.error(f"Error flushing Segment clients: {e}")

        return Response(
            status_code=e.code,
            body={
                "code": e.code,
                "message": (
                    f"Reflekt Registry received a Segment API error while "
                    f"sending events to Segment. Error message: {e.message}"
                ),
            },
            headers={"Content-Type": "application/json"},
        )
