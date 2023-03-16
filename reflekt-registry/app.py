import boto3
import segment.analytics as segment_analytics
from chalice import Chalice
from dateutil import parser

# from segment.analytics.request import APIError

app = Chalice(app_name="reflekt-registry")
app.debug = True

if app.debug:
    segment_analytics.debug = True

segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"


# Tracking error handling
def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


segment_analytics.on_error = on_error


@app.route("/")
def index():
    """Return a welcome message.

    Returns:
        str: The welcome message.
    """
    return "Hi from Reflekt!"


def validate_event_json():
    """Validate event JSON properties against schema stored in registry.

    Returns:
        bool: True if valid, False otherwise.
    """
    # TODO - validate the event JSON
    pass


@app.route("/v1/batch", methods=["POST"])
def proxy_segment_v1_batch():
    """Forward the event from Lambda API proxy to Segment.

    Returns:
        dict: The forwarded event.
    """
    tracks = app.current_request.json_body["batch"]

    for track in tracks:  # Queue up the events
        segment_analytics.track(
            event=track.get("event", None),
            anonymous_id=track.get("anonymousId", None),
            user_id=track.get("userId", None),
            context=track.get("context", {}),
            timestamp=parser.parse(track.get("timestamp", None)),
            integrations=track.get("integrations", {}),
        )

    segment_analytics.flush()  # Flush queued events to Segment
