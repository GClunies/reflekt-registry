import segment.analytics as segment_analytics

# from app import app
# from chalice.test import Client


def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


# def test_segment_proxy_requests():
#     """Test the /v1/batch route."""
#     response = requests.post(
#         "https://1sv9dp6zp7.execute-api.us-west-1.amazonaws.com/api/v1/batch",
#         json=dummy_event,
#         auth=HTTPBasicAuth("lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH", ""),
#     )
#     assert response.json() == dummy_event


def test_proxy_segment_v1_batch():
    """Test the /v1/batch route."""
    # TODO - maybe unset the write_key below???
    segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"
    segment_analytics.host = (
        "https://1sv9dp6zp7.execute-api.us-west-1.amazonaws.com/api/"
    )
    segment_analytics.debug = True
    segment_analytics.on_error = on_error
    segment_analytics.sync_mode = True
    response = segment_analytics.track(
        user_id="test_user_id_2",
        event="test_event",
        properties={"test_property": "test_value"},
    )
    assert response is None
    # segment_analytics.flush()  # Required to send the event to Segment
    # segment_analytics.shutdown()
    # assert response == {"status": "OK"}
