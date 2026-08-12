#!/usr/bin/env python3
"""Script to convert data contracts to Avro schema format."""

import json
import os
from pathlib import Path


def map_type_to_avro(data_type: str) -> str:
    """Map contract data types to AVRO types."""
    type_map = {
        "string": "string",
        "integer": "int",
        "number": "double",
        "boolean": "boolean",
        "date": "string",
        "timestamp": "string",
        "object": "string",
        "array": "array",
        "int": "int",
    }
    return type_map.get(data_type, "string")


def contract_to_avro(contract: dict) -> dict:
    """Convert a DataContract JSON to AVRO schema format."""
    fields = []
    for col in contract.get("columns", []):
        field = {
            "name": col["name"],
            "type": map_type_to_avro(col["data_type"]),
        }
        if col.get("description"):
            field["doc"] = col["description"]

        if col.get("nullable", True):
            field["type"] = ["null", field["type"]]
            field["default"] = None

        fields.append(field)

    # Build comprehensive documentation including metadata
    doc = contract.get("description", "")
    metadata = contract.get("metadata", {})
    if metadata:
        doc += f"\n\nMetadata:\n"
        doc += f"  Data Owner: {metadata.get('data_owner')} ({metadata.get('data_owner_email')})\n"
        doc += f"  Data Steward: {metadata.get('data_steward')} ({metadata.get('data_steward_email')})\n"
        doc += f"  SLA Uptime: {metadata.get('sla_uptime_percentage')}%\n"
        doc += f"  SLA Max Latency: {metadata.get('sla_max_latency_ms')}ms"

    schema = {
        "type": "record",
        "name": contract["name"].replace(" ", ""),
        "namespace": "com.example.schema",
        "doc": doc,
        "fields": fields,
    }

    return schema


def convert_contract(input_path: Path) -> dict:
    """Convert a contract file to Avro format."""
    with open(input_path, 'r') as f:
        contract = json.load(f)

    return contract_to_avro(contract)


def main():
    """Convert all contracts to Avro format."""
    contracts_dir = Path("/Users/vanik_kars/Documents/learning/python/fast_api/schema-registry/contracts/all")

    for contract_file in contracts_dir.glob("**/*.json"):
        if contract_file.name.endswith("_avro.json"):
            continue

        try:
            print(f"Converting {contract_file.relative_to(contracts_dir)}...")
            avro_schema = convert_contract(contract_file)

            # Save as _avro.json
            avro_file = contract_file.with_stem(contract_file.stem + "_avro")
            with open(avro_file, 'w') as f:
                json.dump(avro_schema, f, indent=2)

            print(f"  ✓ Created {avro_file.name}")

        except Exception as e:
            print(f"  ✗ Error: {e}")


if __name__ == "__main__":
    main()
