# Reusable Library (lib)

Core, reusable modules for schema registry and Iceberg table operations. Extracted from microservices to be used by Airflow DAGs and other components.

## Modules

### `models.py`
Domain models for contracts and tables:
- `DataContract` - Represents a data contract with schema definition
- `ColumnDefinition` - Column specification in a contract
- `ContractMetadata` - Metadata (owner, steward, SLA info)
- `IcebergTable` - Iceberg table model
- `Column` - Immutable column definition
- `DataType` - Enum of supported data types
- `SchemaChange` - Tracks schema evolution

### `exceptions.py`
Custom exception classes:
- `SchemaRegistryError` - Base exception
- `RegistryNotFoundError` - Glue registry doesn't exist
- `SchemaNotFoundError` - Schema not in registry
- `TableCreationError` - Iceberg table creation failed
- `TableNotFoundError` - Table doesn't exist
- `InvalidTableError` - Table validation failed
- Plus others for version errors

### `converters.py`
Schema format conversion utilities:
- `contract_to_avro()` - Convert contract to AVRO JSON
- `avro_type_to_data_type()` - Convert AVRO types to contract types
- `map_type_to_avro()` - Map contract types to AVRO
- `map_type_to_glue()` - Map contract types to AWS Glue types
- `extract_metadata_from_doc()` - Extract metadata from AVRO documentation

### `aws_glue.py`
Unified AWS Glue adapter (schema registry + Iceberg):

**Schema Registry Operations:**
- `register_schema()` - Register or update a schema with compatibility checking
- `get_schema_versions()` - Get all versions of a schema
- `list_schemas()` - List all schemas in the registry

**Iceberg Table Operations:**
- `create_table()` - Create an Iceberg table in Glue Catalog
- `get_table()` - Retrieve table metadata
- `update_table()` - Update table schema
- `table_exists()` - Check if table exists
- `create_database_if_not_exists()` - Ensure database exists

**Key Features:**
- Unified boto3 client initialization
- Built-in schema compatibility validation
- Automatic schema version polling (wait for PENDING status)
- Detailed diff analysis for compatibility violations
- AWS credentials from environment (AWS_DEFAULT_REGION, TF_VAR_registry_name)

### `validator.py`
Contract validation without external dependencies:
- `ContractValidator.validate_dict()` - Validate raw dictionary
- `ContractValidator.validate_contract()` - Validate DataContract model
- `ContractValidator.load_and_validate_json()` - Load and validate JSON file
- `ContractValidator.create_contract()` - Build contract with validation

Comprehensive error reporting with specific field errors.

## Usage

### Registering a Schema
```python
from lib.models import DataContract, ColumnDefinition, ContractMetadata
from lib.aws_glue import AwsGlueAdapter

contract = DataContract(
    contract_id="users-v1",
    name="Users",
    description="User records",
    columns=[
        ColumnDefinition(name="id", data_type="string", nullable=False),
        ColumnDefinition(name="email", data_type="string"),
    ]
)

adapter = AwsGlueAdapter()
schema_arn = adapter.register_schema(contract)
```

### Creating an Iceberg Table
```python
from lib.models import IcebergTable, Column, DataType

table = IcebergTable(
    table_name="users",
    contract_id="users-v1",
    version="1.0.0",
    columns=[
        Column(name="id", data_type=DataType.STRING),
        Column(name="email", data_type=DataType.STRING),
    ]
)

adapter = AwsGlueAdapter()
await adapter.create_table(table)
```

### Validating a Contract
```python
from lib.validator import ContractValidator

is_valid, contract, errors = ContractValidator.create_contract({
    "contract_id": "users-v1",
    "name": "Users",
    "description": "User records",
    "columns": [
        {"name": "id", "data_type": "string", "nullable": False},
        {"name": "email", "data_type": "string"},
    ]
})

if not is_valid:
    print("Validation errors:", errors)
```

## Design Principles

1. **No External Dependencies** - Models and validators use only Pydantic + stdlib
2. **AWS Abstraction** - Single adapter hides boto3 complexity
3. **Type Safety** - Enums and dataclasses prevent invalid states
4. **Error Reporting** - Descriptive exceptions with context
5. **Reusability** - Used by Airflow DAGs, FastAPI services, and scripts

## Migration Notes

This module consolidates code from:
- `registry_api/adapters/` - Schema registry operations
- `registry_api/domain/models.py` - Contract models
- `iceberg_creation_service/adapters/` - Iceberg operations
- `iceberg_creation_service/domain/models.py` - Table models

Services can now:
```python
# Instead of: from registry_api.adapters.aws_glue import GlueSchemaRegistryAdapter
from lib.aws_glue import AwsGlueAdapter
```

## Testing

All modules are independently testable:
```bash
# Validator (no AWS calls)
pytest lib/test_validator.py

# Models (pure data classes)
pytest lib/test_models.py

# Converters (pure functions)
pytest lib/test_converters.py

# Adapter (requires AWS credentials/mocking)
pytest lib/test_aws_glue.py
```
