"""Airflow tasks for contract validation, schema registration, and table creation."""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any

from lib.models import DataContract, IcebergTable, Column, DataType
from lib.aws_glue import AwsGlueAdapter
from lib.validator import ContractValidator
from lib.exceptions import SchemaRegistryError

logger = logging.getLogger(__name__)


class ContractTasks:
    """Collection of Airflow tasks for contract processing."""

    @staticmethod
    def fetch_contract_files(repo_path: str = "/app", contracts_dir: str = "contracts/current", changed_files: List[str] = None, process_all: bool = False) -> List[str]:
        """Fetch contract files from the repository.

        Args:
            repo_path: Base repository path
            contracts_dir: Relative path to contracts directory
            changed_files: List of changed file paths (if provided and process_all=False, only these files are processed)
            process_all: If True, process all contracts regardless of changed_files

        Returns:
            List of contract file paths
        """
        contracts_path = Path(repo_path) / contracts_dir

        if not contracts_path.exists():
            logger.warning(f"Contracts directory not found: {contracts_path}")
            return []

        if process_all:
            # Process all contracts
            contract_files = sorted([str(f) for f in contracts_path.glob("**/*.json")])
            logger.info(f"Processing ALL {len(contract_files)} contract files (process_all=true)")
        elif changed_files:
            # Filter to only .json files in contracts directory
            contract_files = [
                str(Path(repo_path) / f)
                for f in changed_files
                if f.endswith('.json') and f.startswith(contracts_dir)
            ]
            logger.info(f"Processing {len(contract_files)} changed contract files")
        else:
            # No changed files and no process_all flag - process nothing
            contract_files = []
            logger.info("No changed files and process_all not set, processing zero contracts")

        return contract_files

    @staticmethod
    def validate_contract(contract_path: str) -> Dict[str, Any]:
        """Validate a contract JSON file.

        Args:
            contract_path: Path to contract JSON file

        Returns:
            Dictionary with validation result

        Raises:
            ValueError: If contract is invalid
        """
        logger.info(f"🔍 Validating contract: {contract_path}")

        is_valid, contract_dict, errors = ContractValidator.load_and_validate_json(contract_path)

        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"❌ Validation failed: {error_msg}")
            raise ValueError(f"Contract validation failed: {error_msg}")

        logger.info(f"✅ Contract validated: {contract_dict.get('contract_id')}")

        return {
            "file_path": contract_path,
            "contract_id": contract_dict.get("contract_id"),
            "name": contract_dict.get("name"),
            "contract_dict": contract_dict,
        }

    @staticmethod
    def register_schema(contract_path: str) -> Dict[str, Any]:
        """Register a contract as a schema in AWS Glue Schema Registry.

        Args:
            contract_path: Path to contract JSON file

        Returns:
            Dictionary with registration result

        Raises:
            SchemaRegistryError: If registration fails
        """
        logger.info(f"📝 Registering schema from: {contract_path}")

        # Validate contract first
        is_valid, contract_dict, errors = ContractValidator.load_and_validate_json(contract_path)
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"❌ Contract validation failed: {error_msg}")
            raise ValueError(f"Contract validation failed: {error_msg}")

        # Create DataContract from validated dict
        _, contract, creation_errors = ContractValidator.create_contract(contract_dict)
        if not contract:
            error_msg = "; ".join(creation_errors)
            logger.error(f"❌ Failed to create contract model: {error_msg}")
            raise ValueError(f"Failed to create contract model: {error_msg}")

        # Register with AWS Glue
        try:
            adapter = AwsGlueAdapter()
            schema_arn = adapter.register_schema(contract)

            logger.info(f"✅ Schema registered: {contract.contract_id} (ARN: {schema_arn})")

            return {
                "file_path": contract_path,
                "contract_id": contract.contract_id,
                "schema_arn": schema_arn,
                "status": "registered",
            }
        except SchemaRegistryError as e:
            logger.error(f"❌ Schema registration failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error during schema registration: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def create_iceberg_table(contract_path: str) -> Dict[str, Any]:
        """Create an Iceberg table in AWS Glue Catalog.

        Args:
            contract_path: Path to contract JSON file

        Returns:
            Dictionary with table creation result

        Raises:
            Exception: If table creation fails
        """
        logger.info(f"📊 Creating Iceberg table from: {contract_path}")

        # Validate and load contract
        is_valid, contract_dict, errors = ContractValidator.load_and_validate_json(contract_path)
        if not is_valid:
            error_msg = "; ".join(errors)
            logger.error(f"❌ Contract validation failed: {error_msg}")
            raise ValueError(f"Contract validation failed: {error_msg}")

        _, contract, creation_errors = ContractValidator.create_contract(contract_dict)
        if not contract:
            error_msg = "; ".join(creation_errors)
            logger.error(f"❌ Failed to create contract model: {error_msg}")
            raise ValueError(f"Failed to create contract model: {error_msg}")

        # Convert to IcebergTable
        try:
            columns = []
            for col in contract.columns:
                try:
                    data_type = DataType[col.data_type.upper()]
                except KeyError:
                    logger.warning(f"Unknown data type '{col.data_type}' for column '{col.name}', using STRING")
                    data_type = DataType.STRING

                columns.append(Column(
                    name=col.name,
                    data_type=data_type,
                    description=col.description
                ))

            table = IcebergTable(
                table_name=contract.contract_id.replace("-", "_").lower(),
                contract_id=contract.contract_id,
                version=contract.version,
                columns=columns,
                description=contract.description,
                data_owner=contract.metadata.data_owner if contract.metadata else None,
                data_steward=contract.metadata.data_steward if contract.metadata else None,
            )

            table.validate()

            # Get existing table if it exists to detect changes
            adapter = AwsGlueAdapter()
            existing_table = adapter.get_table(table.table_name)

            if existing_table:
                logger.info(f"📋 Table exists: {table.table_name}, checking for changes")

                # Compare columns
                old_col_names = {col.name for col in existing_table.columns}
                new_col_names = {col.name for col in table.columns}

                added = new_col_names - old_col_names
                removed = old_col_names - new_col_names

                if added or removed or existing_table.version != table.version:
                    logger.info(f"✏️  Updating table: +{len(added)} -{len(removed)} columns")
                    adapter.update_table(table)
                    status = "updated"
                else:
                    logger.info(f"✔️  No schema changes detected for {table.table_name}")
                    status = "unchanged"
            else:
                logger.info(f"✨ Creating new table: {table.table_name}")
                adapter.create_database_if_not_exists(table.database_name)
                adapter.create_table(table)
                status = "created"

            logger.info(f"✅ Iceberg table {status}: {table.table_name}")

            return {
                "file_path": contract_path,
                "contract_id": contract.contract_id,
                "table_name": table.table_name,
                "status": status,
                "columns_count": len(table.columns),
            }

        except Exception as e:
            logger.error(f"❌ Iceberg table creation failed: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def collect_results(validation_results: List[Dict], schema_results: List[Dict],
                       table_results: List[Dict]) -> Dict[str, Any]:
        """Collect and format results from all tasks.

        Args:
            validation_results: List of validation results
            schema_results: List of schema registration results
            table_results: List of table creation results

        Returns:
            Aggregated results dictionary
        """
        logger.info("📊 Collecting results from all tasks")

        # Build contract -> results mapping
        results_by_contract = {}

        for result in validation_results:
            contract_id = result.get("contract_id")
            if contract_id:
                if contract_id not in results_by_contract:
                    results_by_contract[contract_id] = {}
                results_by_contract[contract_id]["validation"] = result

        for result in schema_results:
            contract_id = result.get("contract_id")
            if contract_id:
                if contract_id not in results_by_contract:
                    results_by_contract[contract_id] = {}
                results_by_contract[contract_id]["schema"] = result

        for result in table_results:
            contract_id = result.get("contract_id")
            if contract_id:
                if contract_id not in results_by_contract:
                    results_by_contract[contract_id] = {}
                results_by_contract[contract_id]["table"] = result

        # Count statuses
        total = len(results_by_contract)
        validation_passed = sum(1 for r in validation_results if r.get("status") == "valid")
        schema_registered = sum(1 for r in schema_results if r.get("status") == "registered")
        tables_created = sum(1 for r in table_results if r.get("status") == "created")
        tables_updated = sum(1 for r in table_results if r.get("status") == "updated")

        logger.info(
            f"Results: {validation_passed}/{total} validated, "
            f"{schema_registered} schemas registered, "
            f"{tables_created} tables created, {tables_updated} updated"
        )

        return {
            "total_contracts": total,
            "validation_passed": validation_passed,
            "schema_registered": schema_registered,
            "tables_created": tables_created,
            "tables_updated": tables_updated,
            "results_by_contract": results_by_contract,
            "all_results": {
                "validation": validation_results,
                "schema": schema_results,
                "table": table_results,
            }
        }
