import segment.analytics as segment_analytics

# from app import app
# from chalice.test import Client


def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


def test_proxy_segment_v1_batch():
    """Test the /v1/batch route."""
    segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"
    segment_analytics.host = (
        "https://ihaf299g16.execute-api.us-west-1.amazonaws.com/api/"
    )
    segment_analytics.debug = True
    segment_analytics.on_error = on_error
    segment_analytics.sync_mode = True
    response = segment_analytics.track(
        user_id="test_user_1",
        event="test_event",
        properties={
            "schema_id": "segment/demo/Test_event/1-0.json",
            "test_property": "test_value",
        },
    )
    assert response is None  # Returns None if tracks were successful
