import requests
import segment.analytics as segment_analytics
from app import app
from chalice.test import Client

dummy_event = {
    "userId": "test_user_id",
    "event": "test_event",
    "properties": {"test_property": "test_value"},
}


def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


def test_segment_proxy_client():
    """Test the segment_proxy() lambda function on `/v1/batch` route."""
    with Client(app) as client:
        result = client.lambda_.invoke("segment_proxy", payload=dummy_event)
        assert result.json_body == dummy_event


# def test_segment_proxy_requests():
#     """Test the /v1/batch route."""
#     response = requests.post(
#         "https://1sv9dp6zp7.execute-api.us-west-1.amazonaws.com/api/v1/batch",
#         json_body=dummy_event,
#     )
#     assert response.json() == dummy_event


# def test_segment_proxy_track():
#     """Test the /v1/batch route."""
#     segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"
#     segment_analytics.host = (
#         "https://1sv9dp6zp7.execute-api.us-west-1.amazonaws.com/api/"
#     )
#     segment_analytics.debug = True
#     segment_analytics.on_error = on_error
#     response = segment_analytics.track(
#         dummy_event["userId"],
#         dummy_event["event"],
#         dummy_event["properties"],
#     )
#     segment_analytics.flush()  # Required to send the event to Segment
#     segment_analytics.shutdown()
#     assert response == dummy_event
