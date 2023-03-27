from chalicelib.s3_registry import get_s3_schema_registry
from jsonschema import Draft7Validator


def validate_event(schema_id: str, event_properties: dict) -> tuple[bool, list]:
    """Validate event properties against the schema in the registry.

    Args:
        schema_id (str): The schema ID.
        event_properties (dict): The event properties.

    Returns:
        bool: True if valid, False otherwise.
        errors (list): List of errors if invalid, empty list otherwise.
    """
    errors = []
    schema_registry = get_s3_schema_registry()
    schema = schema_registry.get_schema(schema_id)
    validator = Draft7Validator(schema=schema)

    if not validator.is_valid(event_properties):
        errors = [
            f"{error.message}"
            for error in sorted(validator.iter_errors(event_properties), key=str)
        ]
        return False, errors
    else:
        return True, errors
