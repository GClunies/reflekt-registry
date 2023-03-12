import boto3
import segment.analytics as segment_analytics
from chalice import Chalice

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


@app.route("/v1/batch", methods=["POST"])
def segment_proxy(event, context):  # TODO - looks at this later for test_app.py
    """Forward the event from Lambda API proxy to Segment.

    Returns:
        dict: The forwarded event.
    """
    event_json = app.current_request.json_body
    segment_analytics.track(
        event_json["userId"],
        event_json["event"],
        event_json["properties"],
    )
    segment_analytics.flush()  # Required to send the event to Segment
    return event_json
