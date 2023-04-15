from loguru import logger


def log_segment_error(error):
    """Log Segment error for debugging.

    This function is passed to the Segment client as the on_error callback.

    Args:
        error (Any): The Segment error.
    """
    logger.error("Segment error:", error)
