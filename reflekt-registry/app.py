from datetime import datetime

import boto3
import segment.analytics as segment_analytics
from chalice import Chalice

app = Chalice(app_name="reflekt-registry")
app.debug = True


# Tracking error handling
def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"
segment_analytics.on_error = on_error

if app.debug:
    segment_analytics.debug = True


@app.route("/")
def index():
    """Return a simple hello world JSON response.

    Returns:
        dict: A simple JSON response.
    """
    return {"reflekt": "registry"}


def forward_event_to_segment(event_json):
    """Forward event JSON to the Segment.

    Args:
        event_json (dict): The event JSON to forward.
    """
    segment_analytics.track(
        user_id=event_json["userId"],
        event=event_json["event"],
        properties=event_json["properties"],
    )
    segment_analytics.flush()


@app.route("/v1/batch", methods=["POST"])
def segment_validate():
    """Validate a Segment event against a provided schema_id."""
    event_json = app.current_request.json_body
    forward_event_to_segment(event_json)
    return {"status": "ok"}


@app.route("/today")
def today():
    """Return today's date.

    Returns:
        str: Today's date.
    """
    return datetime.today().strftime("%Y-%m-%d")


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
