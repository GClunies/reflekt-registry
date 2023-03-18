class RegistryDB(object):  # noqa: D101
    def list_schemas(self):  # noqa: D102
        pass

    def get_schema(self, schema_id):  # noqa: D102
        pass

    def add_schema(self, schema_id, schema):  # noqa: D102
        pass

    def update_schema(self, schema_id, schema):  # noqa: D102
        pass

    def delete_schema(self, schema_id):  # noqa: D102
        pass


class DynamoRegistryDB(RegistryDB):
    """Class for interacting with the schema registry database."""

    def __init__(self, table_resource: str):
        """Initialize the DynamoRegistryDB class.

        Args:
            table_resource (str): The table resource.
        """
        self._table = table_resource

    def list_schemas(self) -> list:
        """List the schemas in the registry database.

        Returns:
            list: The list of schemas.
        """
        schemas = []
        # TODO - list the schemas
        return schemas

    def add_schema(self, schema_id: str, schema: dict) -> None:
        """Add the schema to the registry database.

        Args:
            schema_id (str): The schema ID.
            schema (dict): The schema.
        """
        pass

    def get_schema(self, schema_id: str) -> dict:
        """Get the schema from the registry database.

        Args:
            schema_id (str): The schema ID.

        Returns:
            dict: The schema.
        """
        pass

    def update_schema(self, schema_id: str, schema: dict) -> None:
        """Update the schema in the registry database.

        Args:
            schema_id (str): The schema ID.
            schema (dict): The schema.
        """
        pass

    def delete_schema(self, schema_id: str) -> None:
        """Delete the schema from the registry database.

        Args:
            schema_id (str): The schema ID.
        """
        pass
