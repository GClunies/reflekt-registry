"""Test reflekt-registry proxy for Segment."""
import segment.analytics as segment_analytics


# Segment tracking error handling
def on_error(error):
    """Log debugging error from Segment/Rudderstack.

    Args:
        error (Any): The error.
    """
    print("An error occurred:", error)


segment_analytics.write_key = "lwbDU8gfUFNfUrL8F3lWuCyhDtFstLiH"
segment_analytics.host = "https://1sv9dp6zp7.execute-api.us-west-1.amazonaws.com/api/"
segment_analytics.debug = True
segment_analytics.on_error = on_error
segment_analytics.sync_mode = True

# TODO - figure out how to proxy the track call to the reflekt-registry REST API
# https://www.nickearl.net/2018/11/10/how-to-proxy-segment-tracking-through-charles/
# https://www.scrapingbee.com/blog/python-requests-proxy/

if __name__ == "__main__":
    print("Tracking...")
    response = segment_analytics.track(
        user_id="test_user_id_2",
        event="test_event",
        properties={"test_property": "test_value"},
    )
    print("Response is:", response)
    print("Done.")
