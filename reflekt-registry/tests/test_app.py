import segment.analytics as segment_analytics


def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


def test_segment_event_valid():
    """Test valid event is proxied to VALID Segment source."""
    segment_analytics.write_key = "devWriteKeyABC123"
    segment_analytics.host = (
        "https://ihaf299g16.execute-api.us-west-1.amazonaws.com/api"
    )
    segment_analytics.debug = True
    segment_analytics.on_error = on_error
    segment_analytics.sync_mode = True
    res = segment_analytics.track(
        user_id="test_user_1",
        event="Test Event",
        properties={
            "schema_id": "segment/demo/Test_Event/1-0.json",
            "test_property": "test_value",
        },
    )

    assert res is None


def test_segment_event_invalid():
    """Test invalid event is proxied to INVALID Segment source."""
    segment_analytics.write_key = "devWriteKeyABC123"
    segment_analytics.host = (
        "https://ihaf299g16.execute-api.us-west-1.amazonaws.com/api"
    )
    segment_analytics.debug = True
    segment_analytics.on_error = on_error
    segment_analytics.sync_mode = True
    res = segment_analytics.track(
        user_id="test_user_1",
        event="Test Event",
        properties={
            "schema_id": "segment/demo/Test_Event/1-0.json",
            "test_property": "test_value",
            "invalid_property": "invalid_value",
        },
    )

    assert res is None
