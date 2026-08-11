"""Shared exception classes for schema and table operations."""


class SchemaRegistryError(Exception):
    """Base exception for schema registry domain errors."""
    pass


class RegistryNotFoundError(SchemaRegistryError):
    """Raised when the Glue Schema Registry does not exist."""

    def __init__(self, registry_name: str):
        self.registry_name = registry_name
        super().__init__(
            f"Registry '{registry_name}' not found. Create it first with Terraform."
        )


class SchemaNotFoundError(SchemaRegistryError):
    """Raised when a schema does not exist in the registry."""

    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        super().__init__(f"Schema '{schema_name}' not found")


class InvalidVersionError(SchemaRegistryError):
    """Raised when a requested schema version is invalid."""

    def __init__(self, schema_name: str, version: str):
        self.schema_name = schema_name
        self.version = version
        super().__init__(f"Invalid version format: '{version}'")


class VersionNotFoundError(SchemaRegistryError):
    """Raised when a requested schema version does not exist."""

    def __init__(self, schema_name: str, version: str, latest: int):
        self.schema_name = schema_name
        self.version = version
        self.latest = latest
        super().__init__(
            f"Version '{version}' not found (latest: {latest})"
        )


class TableCreationError(SchemaRegistryError):
    """Raised when Iceberg table creation fails."""

    def __init__(self, message: str):
        super().__init__(message)


class TableNotFoundError(SchemaRegistryError):
    """Raised when a table does not exist."""

    def __init__(self, message: str):
        super().__init__(message)


class InvalidTableError(SchemaRegistryError):
    """Raised when a table definition is invalid."""

    def __init__(self, message: str):
        super().__init__(message)