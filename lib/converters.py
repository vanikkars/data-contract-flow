"""Converters for transforming between different schema formats."""

import json
import re
from typing import Any, List, Dict, Tuple
from lib.models import DataContract, ColumnDefinition, ContractMetadata


def avro_type_to_data_type(avro_type: Any) -> Tuple[str, bool]:
    """Convert AVRO type to contract data type.

    Returns:
        Tuple of (data_type, nullable)
    """
    nullable = False

    if isinstance(avro_type, list):
        non_null_types = [t for t in avro_type if t != "null"]
        nullable = "null" in avro_type

        if len(non_null_types) == 1:
            avro_type = non_null_types[0]
        elif len(non_null_types) > 1:
            avro_type = non_null_types[0]
        else:
            avro_type = "string"

    type_map = {
        "string": "string",
        "int": "integer",
        "long": "integer",
        "float": "number",
        "double": "number",
        "boolean": "boolean",
        "bytes": "string",
        "null": "string",
    }

    data_type = type_map.get(str(avro_type), "string")
    return data_type, nullable


def extract_metadata_from_doc(doc_string: str) -> Dict[str, Any]:
    """Extract metadata from AVRO doc string.

    Expects format:
        Description...

        Metadata:
          Data Owner: Name (email)
          Data Steward: Name (email)
          SLA Uptime: 99.95%
          SLA Max Latency: 5000ms
    """
    metadata = {
        "data_owner": "Data Team",
        "data_owner_email": "data-team@company.com",
        "data_steward": "Data Engineering",
        "data_steward_email": "data-engineering@company.com",
        "sla_uptime_percentage": 99.95,
        "sla_max_latency_ms": 5000,
    }

    if not doc_string:
        return metadata

    owner_match = re.search(r"Data Owner:\s+(.+?)\s+\((.+?)\)", doc_string)
    if owner_match:
        metadata["data_owner"] = owner_match.group(1)
        metadata["data_owner_email"] = owner_match.group(2)

    steward_match = re.search(r"Data Steward:\s+(.+?)\s+\((.+?)\)", doc_string)
    if steward_match:
        metadata["data_steward"] = steward_match.group(1)
        metadata["data_steward_email"] = steward_match.group(2)

    uptime_match = re.search(r"SLA Uptime:\s+([\d.]+)%", doc_string)
    if uptime_match:
        metadata["sla_uptime_percentage"] = float(uptime_match.group(1))

    latency_match = re.search(r"SLA Max Latency:\s+(\d+)ms", doc_string)
    if latency_match:
        metadata["sla_max_latency_ms"] = int(latency_match.group(1))

    return metadata


def contract_to_avro(contract: DataContract) -> str:
    """Convert a DataContract to AVRO schema format.

    Args:
        contract: DataContract model instance

    Returns:
        JSON string of AVRO schema
    """
    fields = []
    for col in contract.columns:
        field = {
            "name": col.name,
            "type": map_type_to_avro(col.data_type),
        }
        if col.description:
            field["doc"] = col.description
        if col.nullable:
            field["type"] = ["null", field["type"]]
            field["default"] = None
        fields.append(field)

    doc = contract.description or ""
    if contract.metadata:
        doc += f"\n\nMetadata:\n"
        doc += f"  Data Owner: {contract.metadata.data_owner} ({contract.metadata.data_owner_email})\n"
        doc += f"  Data Steward: {contract.metadata.data_steward} ({contract.metadata.data_steward_email})\n"
        doc += f"  SLA Uptime: {contract.metadata.sla_uptime_percentage}%\n"
        doc += f"  SLA Max Latency: {contract.metadata.sla_max_latency_ms}ms"

    schema = {
        "type": "record",
        "name": contract.name.replace(" ", ""),
        "namespace": "com.example.schema",
        "doc": doc,
        "fields": fields,
    }

    return json.dumps(schema)


def map_type_to_glue(data_type: str) -> str:
    """Map contract data types to AWS Glue types.

    Args:
        data_type: Contract data type

    Returns:
        Glue/Hive type
    """
    type_map = {
        "string": "string",
        "integer": "bigint",
        "number": "double",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "object": "string",
        "array": "array<string>",
    }
    return type_map.get(data_type, "string")


def map_type_to_avro(data_type: str) -> str:
    """Map contract data types to AVRO types.

    Args:
        data_type: Contract data type

    Returns:
        AVRO type
    """
    type_map = {
        "string": "string",
        "integer": "int",
        "number": "double",
        "boolean": "boolean",
        "date": "string",
        "timestamp": "string",
        "object": "string",
        "array": "array",
    }
    return type_map.get(data_type, "string")