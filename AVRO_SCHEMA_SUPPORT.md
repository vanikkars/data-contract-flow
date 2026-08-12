# AVRO Schema Support

This schema registry now fully supports **AVRO format** as the primary input format for all data contracts. You can use either the traditional JSON contract format or native AVRO schema format.

## What Changed

### 1. Contracts are now in AVRO Format

All contracts in the `contracts/` directory have been converted to AVRO schema format:

```json
{
  "type": "record",
  "name": "Users",
  "namespace": "com.example.schema",
  "doc": "Schema for user records\n\nMetadata:\n  Data Owner: User Management (user-mgmt@company.com)\n  Data Steward: Data Engineering (data-engineering@company.com)\n  SLA Uptime: 99.95%\n  SLA Max Latency: 5000ms",
  "fields": [
    {
      "name": "user_name",
      "type": "string",
      "doc": "Username for the user"
    },
    {
      "name": "email",
      "type": ["null", "string"],
      "default": null,
      "doc": "Email address of the user"
    }
  ]
}
```

### 2. Registry API Now Accepts AVRO Schemas

The `POST /api/v1/schemas` and `PUT /api/v1/schemas/{schema_name}` endpoints now accept both formats:

#### Option 1: Traditional DataContract Format (JSON)
```json
{
  "contract_id": "users-v1",
  "name": "Users",
  "description": "Schema for user records",
  "version": "1.0.0",
  "columns": [
    {
      "name": "user_name",
      "data_type": "string",
      "nullable": false,
      "description": "Username"
    }
  ],
  "metadata": {
    "data_owner": "User Management",
    "data_owner_email": "user-mgmt@company.com",
    "data_steward": "Data Engineering",
    "data_steward_email": "data-engineering@company.com",
    "sla_uptime_percentage": 99.95,
    "sla_max_latency_ms": 5000
  }
}
```

#### Option 2: AVRO Schema Format (Native)
```json
{
  "type": "record",
  "name": "Users",
  "namespace": "com.example.schema",
  "doc": "Schema for user records\n\nMetadata:\n  Data Owner: User Management (user-mgmt@company.com)\n  Data Steward: Data Engineering (data-engineering@company.com)\n  SLA Uptime: 99.95%\n  SLA Max Latency: 5000ms",
  "fields": [
    {
      "name": "user_name",
      "type": "string",
      "doc": "Username"
    }
  ]
}
```

## Key Features

### Type Mapping

The converter automatically maps between AVRO types and contract data types:

| AVRO Type | Contract Type |
|-----------|---------------|
| `string` | `string` |
| `int`, `long` | `integer` |
| `float`, `double` | `number` |
| `boolean` | `boolean` |
| `bytes` | `string` |
| `["null", "T"]` | nullable field |

### Nullable Field Support

AVRO union types with `null` are automatically recognized as nullable fields:

```json
{
  "name": "optional_email",
  "type": ["null", "string"],
  "default": null,
  "doc": "Optional email"
}
```

Converts to:
```json
{
  "name": "optional_email",
  "data_type": "string",
  "nullable": true,
  "description": "Optional email"
}
```

### Metadata Extraction

Metadata can be embedded in the AVRO schema's `doc` field using this format:

```
Description text here

Metadata:
  Data Owner: Name (email@company.com)
  Data Steward: Name (email@company.com)
  SLA Uptime: 99.95%
  SLA Max Latency: 5000ms
```

The converter automatically extracts and parses this metadata into the contract's metadata object.

## Using the API

### Create a Schema with AVRO Format

```bash
curl -X POST http://localhost:8000/api/v1/schemas \
  -H "Content-Type: application/json" \
  -d @contracts/all/user/01/user_v1.json
```

### Update a Schema with AVRO Format

```bash
curl -X PUT http://localhost:8000/api/v1/schemas/users-v1 \
  -H "Content-Type: application/json" \
  -d @contracts/all/user/01/user_v1.json
```

## Internal Architecture

### New Components

1. **AvroSchema Model** (`registry_api/domain/models.py`)
   - Pydantic model representing AVRO schema structure
   - Supports type validation

2. **Converters** (`registry_api/adapters/outbound/aws_glue/converters.py`)
   - `avro_to_data_contract()`: Converts AVRO schema to DataContract
   - `avro_type_to_data_type()`: Maps AVRO types to contract types
   - `extract_metadata_from_doc()`: Parses metadata from AVRO doc field

3. **Router Updates** (`registry_api/adapters/inbound/http/router.py`)
   - POST and PUT endpoints accept `Union[DataContract, AvroSchema]`
   - Automatic conversion to DataContract internally

### Data Flow

```
AVRO Schema Input
    ↓
AvroSchema Model (validation)
    ↓
avro_to_data_contract() (conversion)
    ↓
DataContract Model
    ↓
RegisterSchemaUseCase (existing logic)
    ↓
contract_to_avro() (generates AVRO for AWS Glue)
    ↓
AWS Glue Schema Registry
```

## Migration Guide

### For Contract Files

If you have existing DataContract JSON files, they will still work. However, to leverage AVRO benefits:

1. Use the conversion script: `python scripts/convert_to_avro.py`
2. Review the generated AVRO schemas
3. Commit the new AVRO versions to your repository

### For API Consumers

No changes required! Your existing code using the traditional format will continue to work. You can gradually migrate to AVRO format:

```python
# Old way (still works)
contract = {
    "contract_id": "users-v1",
    "columns": [...]
}
response = requests.post("http://localhost:8000/api/v1/schemas", json=contract)

# New way (with AVRO)
avro_schema = {
    "type": "record",
    "name": "Users",
    "fields": [...]
}
response = requests.post("http://localhost:8000/api/v1/schemas", json=avro_schema)
```

## Testing

Run the AVRO converter tests:

```bash
pytest tests/test_avro_converter.py -v
```

Tests cover:
- Basic AVRO to DataContract conversion
- Metadata extraction from doc strings
- AVRO type to contract type mapping
- Nullable field detection

## Benefits

1. **Standards Compliance**: AVRO is an industry standard format
2. **Better Type Safety**: Native AVRO type system
3. **Direct Compatibility**: Schemas ready for AVRO consumers
4. **Documentation**: Rich `doc` fields support detailed documentation
5. **Flexibility**: Accept both formats - migrate at your own pace

## See Also

- [AVRO Schema Specification](https://avro.apache.org/docs/current/spec.html)
- [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
- `registry_api/adapters/outbound/aws_glue/converters.py` - Conversion logic
- `tests/test_avro_converter.py` - Conversion tests