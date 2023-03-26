import json
from pathlib import Path
from pprint import pprint

from jsonschema import Draft7Validator

json_file = Path.cwd() / "test.json"

event = {
    "schema_id": "segment/demo/Test_event/1-0.json",
    "test_property": "test_value",
}

with json_file.open(encoding="utf-8") as f:
    schema = json.load(f)

# pprint(schema)

validator = Draft7Validator(schema=schema)

if not validator.is_valid(event):
    errors = [
        f"{error.message}" for error in sorted(validator.iter_errors(event), key=str)
    ]
    print(errors)


# segment/demo/Test_event/1-0.json
