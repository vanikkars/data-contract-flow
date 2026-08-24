"""Unified AWS Glue adapter for schema registry and Iceberg operations."""

import os
import boto3
import logging
import time
import json
from typing import Optional, Tuple, Dict, Any, List

from lib.models import DataContract, IcebergTable, Column, DataType
from lib.exceptions import (
    RegistryNotFoundError,
    SchemaNotFoundError,
    TableCreationError,
    TableNotFoundError,
)
from lib.converters import contract_to_avro
from lib.schema_validator import SchemaSafetyValidator, DownstreamImpactAnalyzer

logger = logging.getLogger(__name__)

# Compatibility modes considered strong enough for a source-aligned raw layer.
#
# FULL_ALL forbids both field removal and required-field addition, and checks
# transitively against every prior version — not just the latest. Transitivity
# matters for Iceberg because a table retains data written under every
# historical schema version, so a non-transitive check can approve a change
# that breaks readers of data still sitting in old snapshots.
SAFE_COMPATIBILITY_MODES = {"FULL_ALL", "FULL"}

# BACKWARD is the registry default and explicitly permits field deletion, which
# is a guaranteed downstream SQL break. Kept usable but never silently.
WEAK_COMPATIBILITY_MODES = {
    "BACKWARD": "permits field deletion — downstream SQL queries will fail",
    "BACKWARD_ALL": "permits field deletion — downstream SQL queries will fail",
    "FORWARD": "permits adding required fields — INSERT statements will fail",
    "FORWARD_ALL": "permits adding required fields — INSERT statements will fail",
    "NONE": "no compatibility checking at all",
    "DISABLED": "no compatibility checking at all",
}

DEFAULT_COMPATIBILITY = "FULL_ALL"


class AwsGlueAdapter:
    """Unified adapter for AWS Glue Schema Registry and Iceberg operations."""

    def __init__(self, region: str = None, registry_name: str = None, enforce_sql_safety: bool = True, engine: str = "iceberg"):
        """Initialize the adapter.

        Args:
            region: AWS region (defaults to AWS_DEFAULT_REGION env var or us-east-1)
            registry_name: Name of Glue Schema Registry (defaults to schema-registry)
            enforce_sql_safety: If True, enforce SQL-safety checks on schema changes
            engine: Target SQL engine (iceberg, athena, redshift, spark)
        """
        self.region = region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.registry_name = registry_name or os.getenv("TF_VAR_registry_name", "schema-registry")
        self.glue = boto3.client("glue", region_name=self.region)
        self.sts = boto3.client("sts", region_name=self.region)
        self.enforce_sql_safety = enforce_sql_safety
        self.engine = engine
        self.sql_validator = SchemaSafetyValidator(strict_mode=enforce_sql_safety, engine=engine)
        self.impact_analyzer = DownstreamImpactAnalyzer(self.glue)

    # ============================================================================
    # Schema Registry Operations
    # ============================================================================

    def register_schema(
        self,
        contract: DataContract,
        data_format: str = "AVRO",
        compatibility: str = DEFAULT_COMPATIBILITY,
        enforce_sql_safety: bool = None,
    ) -> str:
        """Register a data contract as a schema in the registry.

        Args:
            contract: The data contract to register
            data_format: Schema format (AVRO, PROTOBUF, JSON)
            compatibility: Compatibility mode. Defaults to FULL_ALL; anything
                weaker is permitted but logged as a warning (see
                WEAK_COMPATIBILITY_MODES)
            enforce_sql_safety: If True, enforce SQL-safety checks (overrides instance setting)

        Returns:
            Schema ARN

        Raises:
            RegistryNotFoundError: If registry does not exist
            ValueError: If schema registration fails or SQL-safety violated
        """
        schema_name = contract.contract_id
        description = contract.description or f"Schema for {schema_name}"
        schema_definition = contract_to_avro(contract)

        self._warn_on_weak_compatibility(schema_name, compatibility)

        # Use instance setting if not overridden
        enforce_sql_safety = enforce_sql_safety if enforce_sql_safety is not None else self.enforce_sql_safety

        try:
            registry = self.glue.get_registry(
                RegistryId={"RegistryName": self.registry_name}
            )
            registry_arn = registry["RegistryArn"]
        except self.glue.exceptions.EntityNotFoundException:
            raise RegistryNotFoundError(self.registry_name)

        tags = {
            "ManagedBy": "airflow-dag",
            "Source": "data-contract",
            "ContractVersion": contract.version,
        }
        if contract.metadata:
            tags.update({
                "DataOwner": contract.metadata.data_owner,
                "DataOwnerEmail": contract.metadata.data_owner_email,
                "DataSteward": contract.metadata.data_steward,
                "DataStewardEmail": contract.metadata.data_steward_email,
                "SLAUptimePercentage": str(contract.metadata.sla_uptime_percentage),
                "SLAMaxLatencyMs": str(contract.metadata.sla_max_latency_ms),
            })

        try:
            existing = self.glue.get_schema(
                SchemaId={
                    "RegistryName": self.registry_name,
                    "SchemaName": schema_name,
                }
            )

            try:
                current_version_resp = self.glue.get_schema_version(
                    SchemaId={
                        "RegistryName": self.registry_name,
                        "SchemaName": schema_name,
                    },
                    SchemaVersionNumber={"LatestVersion": True},
                )
                current_schema_def = current_version_resp.get("SchemaDefinition", "")

                logger.info(
                    f"Current schema def length: {len(current_schema_def)}, "
                    f"New schema def length: {len(schema_definition)}"
                )

                if current_schema_def != schema_definition:
                    logger.info(f"📝 Schema definition CHANGED for {schema_name}")

                    # SQL-SAFETY VALIDATION (NEW)
                    if enforce_sql_safety:
                        current_schema = json.loads(current_schema_def)
                        new_schema = json.loads(schema_definition)

                        is_sql_safe, sql_violations = self.sql_validator.validate_schema_change(
                            current_schema, new_schema, compatibility
                        )

                        if not is_sql_safe:
                            error_summary = "\n".join(
                                f"  [{v['type'].upper()}] {v['message']}"
                                for v in sql_violations
                                if v["type"] == "error"
                            )
                            logger.error(f"❌ SQL-Safety violations for {schema_name}:\n{error_summary}")

                            raise ValueError(
                                f"Schema change violates SQL-safety requirements. "
                                f"Changes could break downstream SQL queries:\n{error_summary}"
                            )

                        # Log warnings but continue
                        warnings = [v for v in sql_violations if v["type"] == "warning"]
                        if warnings:
                            logger.warning(f"⚠️  SQL-Safety warnings for {schema_name}:")
                            for v in warnings:
                                logger.warning(f"  [WARNING] {v['message']}")

                        # Analyze downstream impact
                        impact = self.impact_analyzer.analyze_impact(schema_name)
                        if impact["affected_tables"]:
                            logger.info(
                                f"📊 This change affects {len(impact['affected_tables'])} table(s): "
                                f"{', '.join(t['name'] for t in impact['affected_tables'])}"
                            )

                    try:
                        version_result = self.glue.register_schema_version(
                            SchemaId={
                                "RegistryName": self.registry_name,
                                "SchemaName": schema_name,
                            },
                            SchemaDefinition=schema_definition,
                        )
                        version_number = version_result.get('VersionNumber')
                        version_status = version_result.get('Status', 'AVAILABLE')

                        logger.info(
                            f"Registered new version {version_number} for schema {schema_name}, "
                            f"status: {version_status}"
                        )

                        error_details = None
                        if version_status == 'PENDING':
                            logger.info(f"⏳ Waiting for schema validation...")
                            final_status, error_details = self._wait_for_version_validation(
                                schema_name, version_number
                            )
                            logger.info(f"Schema validation completed with status: {final_status}")
                            version_status = final_status

                        if version_status == 'FAILURE':
                            detailed_msg = self._analyze_schema_diff(
                                current_schema_def, schema_definition, compatibility
                            )
                            error_msg = f"Version {version_number} marked as FAILURE (compatibility violation)"
                            error_msg += f"\n{detailed_msg}"

                            if error_details:
                                detail_msg = error_details.get('ErrorMessage', 'No details available')
                                detail_type = error_details.get('ErrorCode', 'UNKNOWN')
                                logger.error(f"AWS Glue Error ({detail_type}): {detail_msg}")
                            else:
                                logger.error(f"❌ Schema registration FAILED for {schema_name}")

                            raise ValueError(f"Schema change violates {compatibility} compatibility:\n{error_msg}")

                    except ValueError as e:
                        logger.error(f"❌ Compatibility violation: {str(e)}")
                        raise e
                    except Exception as e:
                        logger.error(f"Failed to register new schema version: {str(e)}", exc_info=True)
                        raise
                else:
                    logger.info(f"📋 Schema definition UNCHANGED for {schema_name}")

                schema_arn = existing.get("SchemaArn", "")
                if schema_arn and tags:
                    try:
                        self.glue.tag_resource(ResourceArn=schema_arn, TagsToAdd=tags)
                        logger.info(f"Updated tags for schema {schema_name}")
                    except Exception as tag_err:
                        logger.warning(f"Could not update tags for {schema_name}: {tag_err}")

                response = self.glue.get_schema(
                    SchemaId={
                        "RegistryName": self.registry_name,
                        "SchemaName": schema_name,
                    }
                )
            except ValueError:
                raise
            except Exception as e:
                logger.warning(f"Error while processing schema {schema_name}: {str(e)}", exc_info=True)
                response = existing

        except self.glue.exceptions.EntityNotFoundException:
            response = self.glue.create_schema(
                RegistryId={"RegistryName": self.registry_name},
                SchemaName=schema_name,
                DataFormat=data_format,
                Compatibility=compatibility,
                Description=description,
                SchemaDefinition=schema_definition,
                Tags=tags,
            )

        schema_arn = response.get("SchemaArn", "")
        logger.info(f"✅ Successfully registered schema {schema_name} with ARN: {schema_arn}")
        return schema_arn

    def get_schema_versions(self, schema_name: str) -> Optional[dict]:
        """Get version information for a schema including schema definition.

        Args:
            schema_name: Name of the schema

        Returns:
            Schema details or None if not found
        """
        try:
            schema_response = self.glue.get_schema(
                SchemaId={
                    "RegistryName": self.registry_name,
                    "SchemaName": schema_name,
                }
            )

            latest_version = schema_response.get("LatestSchemaVersion", 0)
            schema_def = None

            if latest_version > 0:
                try:
                    version_response = self.glue.get_schema_version(
                        SchemaId={
                            "RegistryName": self.registry_name,
                            "SchemaName": schema_name,
                        },
                        SchemaVersionNumber={"LatestVersion": True},
                    )
                    schema_def = version_response.get("SchemaDefinition", "")
                except Exception as e:
                    logger.warning(f"Could not fetch schema definition for {schema_name}: {e}")
                    schema_def = None

            schema_content = None
            if schema_def:
                try:
                    schema_content = json.loads(schema_def)
                except Exception:
                    schema_content = schema_def

            tags = {}
            try:
                schema_arn = schema_response.get("SchemaArn", "")
                if schema_arn:
                    tags_response = self.glue.get_tags(ResourceArn=schema_arn)
                    tags = tags_response.get("Tags", {})
            except Exception as e:
                logger.warning(f"Could not fetch tags for {schema_name}: {e}")

            metadata = {
                "data_owner": tags.get("DataOwner"),
                "data_owner_email": tags.get("DataOwnerEmail"),
                "data_steward": tags.get("DataSteward"),
                "data_steward_email": tags.get("DataStewardEmail"),
                "sla_uptime_percentage": self._parse_float(tags.get("SLAUptimePercentage")),
                "sla_max_latency_ms": self._parse_int(tags.get("SLAMaxLatencyMs")),
                "contract_version": tags.get("ContractVersion"),
                "managed_by": tags.get("ManagedBy"),
                "source": tags.get("Source"),
            }

            return {
                "schema_name": schema_name,
                "latest_version": latest_version,
                "next_version": schema_response.get("NextSchemaVersion", 0),
                "checkpoint": schema_response.get("SchemaCheckpoint", ""),
                "status": schema_response.get("SchemaStatus", "AVAILABLE"),
                "created_time": schema_response.get("CreatedTime"),
                "updated_time": schema_response.get("UpdatedTime"),
                "arn": schema_response.get("SchemaArn"),
                "description": schema_response.get("Description", ""),
                "data_format": schema_response.get("DataFormat", "AVRO"),
                "compatibility": schema_response.get("Compatibility", "FORWARD_ALL"),
                "metadata": metadata,
                "schema": schema_content,
            }
        except Exception:
            return None

    def list_schemas(self) -> List[dict]:
        """List all schemas in the registry."""
        try:
            response = self.glue.list_schemas(
                RegistryId={"RegistryName": self.registry_name}
            )
            return response.get("Schemas", [])
        except self.glue.exceptions.EntityNotFoundException:
            return []

    # ============================================================================
    # Iceberg Table Operations
    # ============================================================================

    async def create_table(self, table: IcebergTable) -> None:
        """Create a new Iceberg table.

        Args:
            table: IcebergTable model instance

        Raises:
            TableCreationError: If creation fails
        """
        try:
            logger.debug(f"Creating table in Glue: {table.table_name}")

            if not table.s3_location:
                account_id = self.sts.get_caller_identity()["Account"]
                table.s3_location = (
                    f"s3://iceberg-data-{account_id}-{self.region}/"
                    f"{table.database_name}/{table.table_name}"
                )

            self.glue.create_table(
                DatabaseName=table.database_name,
                TableInput={
                    "Name": table.table_name,
                    "Description": table.description or f"Iceberg table for {table.contract_id}",
                    "StorageDescriptor": {
                        "Columns": table.to_glue_columns(),
                        "Location": table.s3_location,
                        "InputFormat": "org.apache.iceberg.mr.hive.IcebergInputFormat",
                        "OutputFormat": "org.apache.iceberg.mr.hive.IcebergOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.iceberg.serde.IcebergSerDe",
                        },
                    },
                    "PartitionKeys": [],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": table.get_table_parameters(),
                },
            )
            logger.info(f"Table created in Glue: {table.table_name}")

        except self.glue.exceptions.AlreadyExistsException:
            logger.debug(f"Table already exists: {table.table_name}")
        except Exception as e:
            logger.error(f"Failed to create table: {str(e)}")
            raise TableCreationError(f"Failed to create table: {str(e)}")

    async def get_table(self, table_name: str, database_name: str = "iceberg_tables") -> Optional[IcebergTable]:
        """Get table from Glue and reconstruct as IcebergTable object."""
        try:
            logger.debug(f"Retrieving table {table_name} from database {database_name}")
            response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
            table_data = response["Table"]

            columns = []
            for col in table_data.get("StorageDescriptor", {}).get("Columns", []):
                try:
                    data_type = DataType[col["Type"].upper()]
                except (KeyError, ValueError):
                    logger.warning(f"Unknown data type {col['Type']}, skipping column {col['Name']}")
                    continue

                columns.append(Column(
                    name=col["Name"],
                    data_type=data_type,
                    description=col.get("Comment")
                ))

            params = table_data.get("Parameters", {})
            version = params.get("iceberg_table_version")
            if not version:
                raise ValueError(f"Missing required parameter 'iceberg_table_version' for table {table_name}")

            logger.debug(f"Successfully retrieved table {table_name} with {len(columns)} columns")

            return IcebergTable(
                table_name=table_name,
                contract_id=params.get("contract_id", table_name),
                version=version,
                columns=columns,
                database_name=database_name,
                description=table_data.get("Description"),
                data_owner=params.get("data_owner"),
                data_steward=params.get("data_steward"),
                s3_location=table_data.get("StorageDescriptor", {}).get("Location")
            )
        except self.glue.exceptions.EntityNotFoundException:
            logger.warning(f"Table not found in Glue: {table_name} in database {database_name}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving table {table_name}: {str(e)}", exc_info=True)
            return None

    async def table_exists(self, table_name: str, database_name: str = "iceberg_tables") -> bool:
        """Check if table exists in Glue."""
        try:
            self.glue.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except self.glue.exceptions.EntityNotFoundException:
            return False
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            raise TableCreationError(f"Failed to check table existence: {str(e)}")

    async def update_table(self, table: IcebergTable) -> None:
        """Update table schema."""
        try:
            response = self.glue.get_table(
                DatabaseName=table.database_name,
                Name=table.table_name,
            )
            current_table = response["Table"]

            self.glue.update_table(
                DatabaseName=table.database_name,
                TableInput={
                    "Name": table.table_name,
                    "Description": current_table.get("Description", ""),
                    "StorageDescriptor": {
                        **current_table["StorageDescriptor"],
                        "Columns": table.to_glue_columns(),
                    },
                    "PartitionKeys": current_table.get("PartitionKeys", []),
                    "TableType": current_table.get("TableType", "EXTERNAL_TABLE"),
                    "Parameters": {
                        **current_table.get("Parameters", {}),
                        "iceberg_table_version": str(table.version),
                    },
                },
            )
            logger.info(f"Table schema updated: {table.table_name}")

        except self.glue.exceptions.EntityNotFoundException:
            raise TableNotFoundError(f"Table not found: {table.table_name}")
        except Exception as e:
            logger.error(f"Failed to update table: {str(e)}")
            raise TableCreationError(f"Failed to update table: {str(e)}")

    async def create_database_if_not_exists(self, database_name: str) -> None:
        """Create database if needed."""
        try:
            self.glue.create_database(DatabaseInput={"Name": database_name})
            logger.info(f"Database created: {database_name}")
        except self.glue.exceptions.AlreadyExistsException:
            logger.debug(f"Database already exists: {database_name}")
        except Exception as e:
            logger.error(f"Failed to create database: {str(e)}")
            raise TableCreationError(f"Failed to create database: {str(e)}")

    # ============================================================================
    # Private Helper Methods
    # ============================================================================

    def _warn_on_weak_compatibility(self, schema_name: str, compatibility: str) -> None:
        """Log a loud warning when registering under a mode weaker than FULL_ALL.

        The mode is not overridden — the caller may have a deliberate reason —
        but the SQL-safety validator becomes the only thing standing between a
        weak mode and a downstream break, so the choice is never silent.
        """
        mode = (compatibility or "").upper()

        if mode in SAFE_COMPATIBILITY_MODES:
            return

        reason = WEAK_COMPATIBILITY_MODES.get(mode, "not a recognised strong mode")
        logger.warning(
            f"⚠️  Schema '{schema_name}' uses compatibility mode {mode}: {reason}. "
            f"Recommended: {DEFAULT_COMPATIBILITY}. SQL-safety validation is now the "
            f"only gate protecting downstream consumers."
        )

    def _wait_for_version_validation(
        self, schema_name: str, version_number: int, timeout: int = 60
    ) -> Tuple[str, Optional[Dict]]:
        """Wait for AWS Glue to complete schema version validation."""
        start_time = time.time()
        poll_interval = 1

        while time.time() - start_time < timeout:
            try:
                version_resp = self.glue.get_schema_version(
                    SchemaId={
                        "RegistryName": self.registry_name,
                        "SchemaName": schema_name,
                    },
                    SchemaVersionNumber={"VersionNumber": version_number},
                )
                status = version_resp.get('Status', 'PENDING')
                error_details = version_resp.get('VersionFailureDetails')

                if status != 'PENDING':
                    if error_details:
                        logger.info(f"Version {version_number} validation completed with status: {status}")
                        logger.error(f"Compatibility error details: {error_details}")
                    else:
                        logger.info(f"Version {version_number} validation completed with status: {status}")
                    return status, error_details

                logger.debug(f"Version {version_number} still PENDING, waiting...")
                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error checking version status: {str(e)}", exc_info=True)
                raise

        error_msg = f"Schema version validation timed out after {timeout} seconds"
        logger.error(f"❌ {error_msg}")
        raise TimeoutError(error_msg)

    def _analyze_schema_diff(
        self, old_schema_def: str, new_schema_def: str, compatibility: str
    ) -> str:
        """Analyze differences between old and new schema."""
        try:
            old_schema = json.loads(old_schema_def)
            new_schema = json.loads(new_schema_def)

            old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
            new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

            changes = []

            removed = set(old_fields.keys()) - set(new_fields.keys())
            if removed:
                changes.append(
                    f"❌ Removed fields (not allowed in {compatibility}): {', '.join(sorted(removed))}"
                )

            modified = []
            for field_name in old_fields.keys() & new_fields.keys():
                old_type = old_fields[field_name].get("type")
                new_type = new_fields[field_name].get("type")
                if old_type != new_type:
                    modified.append(f"{field_name}: {old_type} → {new_type}")
            if modified:
                changes.append(
                    f"❌ Modified field types (not allowed in {compatibility}): {', '.join(modified)}"
                )

            added_required = []
            for field_name in set(new_fields.keys()) - set(old_fields.keys()):
                field_type = new_fields[field_name].get("type")
                if not (isinstance(field_type, list) and "null" in field_type):
                    default = new_fields[field_name].get("default")
                    if default is None:
                        added_required.append(field_name)
            if added_required:
                changes.append(
                    f"⚠️  Added required fields without defaults: {', '.join(added_required)}"
                )

            added_optional = []
            for field_name in set(new_fields.keys()) - set(old_fields.keys()):
                field_type = new_fields[field_name].get("type")
                if isinstance(field_type, list) and "null" in field_type:
                    added_optional.append(field_name)
            if added_optional:
                changes.append(f"✅ Added optional fields (allowed): {', '.join(added_optional)}")

            if not changes:
                return "Unknown schema difference"

            return "\n".join(changes)

        except Exception as e:
            logger.warning(f"Could not analyze schema diff: {str(e)}")
            return "Could not determine specific schema changes"

    def _parse_float(self, value: Optional[str]) -> Optional[float]:
        """Parse string to float, return None if invalid."""
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_int(self, value: Optional[str]) -> Optional[int]:
        """Parse string to int, return None if invalid."""
        if not value:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None