"""Contract validation logic for schema definitions."""

import json
from typing import Tuple, List, Dict
from lib.models import DataContract, ColumnDefinition, ContractMetadata


class ContractValidator:
    """Validator for data contracts with comprehensive error reporting."""

    @staticmethod
    def validate_dict(contract_dict: Dict) -> Tuple[bool, List[str]]:
        """Validate a contract dictionary.

        Args:
            contract_dict: Dictionary representation of contract

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        required_fields = ["contract_id", "name", "description", "columns"]
        for field in required_fields:
            if field not in contract_dict:
                errors.append(f"Missing required field: '{field}'")

        if not contract_dict.get("columns"):
            errors.append("Contract must have at least one column")
        elif not isinstance(contract_dict.get("columns"), list):
            errors.append("'columns' must be a list")
        else:
            for i, col in enumerate(contract_dict["columns"]):
                if not isinstance(col, dict):
                    errors.append(f"Column {i} is not a dict")
                    continue

                if "name" not in col:
                    errors.append(f"Column {i} missing 'name' field")
                if "data_type" not in col:
                    errors.append(f"Column {i} missing 'data_type' field")

                valid_types = {
                    "string", "integer", "number", "boolean",
                    "date", "timestamp", "object", "array"
                }
                if col.get("data_type") not in valid_types:
                    errors.append(
                        f"Column '{col.get('name')}' has invalid data_type: "
                        f"'{col.get('data_type')}'. Must be one of: {', '.join(valid_types)}"
                    )

        if contract_dict.get("version") and not isinstance(contract_dict.get("version"), str):
            errors.append("'version' must be a string")

        return len(errors) == 0, errors

    @staticmethod
    def validate_contract(contract: DataContract) -> Tuple[bool, List[str]]:
        """Validate a DataContract model instance.

        Args:
            contract: DataContract model instance

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        if not contract.contract_id.strip():
            errors.append("contract_id cannot be empty")

        if not contract.name.strip():
            errors.append("name cannot be empty")

        if not contract.description.strip():
            errors.append("description cannot be empty")

        if not contract.columns:
            errors.append("Contract must have at least one column")

        column_names = set()
        for i, col in enumerate(contract.columns):
            if not col.name.strip():
                errors.append(f"Column {i} has empty name")
            elif col.name in column_names:
                errors.append(f"Duplicate column name: '{col.name}'")
            column_names.add(col.name)

            if not col.data_type.strip():
                errors.append(f"Column '{col.name}' has empty data_type")

            valid_types = {
                "string", "integer", "number", "boolean",
                "date", "timestamp", "object", "array"
            }
            if col.data_type not in valid_types:
                errors.append(
                    f"Column '{col.name}' has invalid data_type: '{col.data_type}'. "
                    f"Must be one of: {', '.join(valid_types)}"
                )

        return len(errors) == 0, errors

    @staticmethod
    def load_and_validate_json(json_path: str) -> Tuple[bool, Dict, List[str]]:
        """Load and validate a contract JSON file.

        Args:
            json_path: Path to contract JSON file

        Returns:
            Tuple of (is_valid, contract_dict, error_messages)
        """
        errors = []
        contract_dict = {}

        try:
            with open(json_path, 'r') as f:
                contract_dict = json.load(f)
        except FileNotFoundError:
            errors.append(f"File not found: {json_path}")
            return False, {}, errors
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON in {json_path}: {str(e)}")
            return False, {}, errors

        is_valid, validation_errors = ContractValidator.validate_dict(contract_dict)
        errors.extend(validation_errors)

        return is_valid and len(errors) == 0, contract_dict, errors

    @staticmethod
    def create_contract(contract_dict: Dict) -> Tuple[bool, DataContract, List[str]]:
        """Create a DataContract from dictionary with validation.

        Args:
            contract_dict: Dictionary representation of contract

        Returns:
            Tuple of (is_valid, contract_or_none, error_messages)
        """
        errors = []
        is_valid, dict_errors = ContractValidator.validate_dict(contract_dict)
        errors.extend(dict_errors)

        if not is_valid:
            return False, None, errors

        try:
            columns = [
                ColumnDefinition(
                    name=col["name"],
                    data_type=col["data_type"],
                    nullable=col.get("nullable", True),
                    description=col.get("description", "")
                )
                for col in contract_dict.get("columns", [])
            ]

            metadata_dict = contract_dict.get("metadata", {})
            metadata = ContractMetadata(**metadata_dict) if metadata_dict else ContractMetadata()

            contract = DataContract(
                contract_id=contract_dict["contract_id"],
                name=contract_dict["name"],
                description=contract_dict["description"],
                version=contract_dict.get("version", "1.0.0"),
                columns=columns,
                metadata=metadata,
            )

            is_valid, contract_errors = ContractValidator.validate_contract(contract)
            errors.extend(contract_errors)

            return is_valid and len(errors) == 0, contract if is_valid else None, errors

        except Exception as e:
            errors.append(f"Failed to create contract: {str(e)}")
            return False, None, errors