"""SQL-safety validation for schema changes in Glue schema registry.

This module ensures schema evolution is safe for downstream SQL workloads
(Athena, Redshift, Spark SQL) in addition to Avro compatibility.
"""

import json
import logging
from typing import Dict, Any, List, Tuple, Optional, Set
from enum import Enum

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Classification of schema changes."""
    SAFE = "safe"
    UNSAFE = "unsafe"
    RISKY = "risky"
    REQUIRES_MIGRATION = "requires_migration"


class SqlReservedWords:
    """SQL reserved words that cannot be column names without quoting."""
    RESERVED = {
        'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'cross', 'on',
        'group', 'by', 'having', 'order', 'asc', 'desc', 'limit', 'offset', 'union',
        'all', 'distinct', 'insert', 'update', 'delete', 'table', 'create', 'drop',
        'alter', 'truncate', 'and', 'or', 'not', 'in', 'is', 'null', 'like', 'between',
        'case', 'when', 'then', 'else', 'end', 'with', 'as', 'values', 'default',
        'constraint', 'primary', 'key', 'foreign', 'references', 'unique', 'check',
        'index', 'view', 'function', 'procedure', 'trigger', 'database', 'schema',
    }


class SchemaSafetyValidator:
    """Validates schema changes for SQL-safety across SQL dialects."""

    def __init__(self, strict_mode: bool = True):
        """
        Initialize validator.

        Args:
            strict_mode: If True, reject risky changes; if False, warn only
        """
        self.strict_mode = strict_mode

    def validate_schema_change(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        compatibility_mode: str = "FORWARD_ALL",
    ) -> Tuple[bool, List[Dict[str, str]]]:
        """
        Validate a schema change for SQL-safety.

        Args:
            old_schema: Current AVRO schema
            new_schema: Proposed AVRO schema
            compatibility_mode: Glue compatibility mode

        Returns:
            (is_safe, list_of_violations)
            - is_safe: True if change is SQL-safe
            - violations: List of dicts with keys:
              - type: "error" or "warning"
              - field: Column name affected (if any)
              - message: Human-readable violation message
        """
        violations = []

        # Extract fields from schemas
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        # Check 1: Removed fields
        violations.extend(self._check_removed_fields(old_fields, new_fields))

        # Check 2: Type changes
        violations.extend(
            self._check_type_changes(old_fields, new_fields, compatibility_mode)
        )

        # Check 3: New required fields without defaults
        violations.extend(self._check_new_required_fields(old_fields, new_fields))

        # Check 4: Column reordering
        violations.extend(self._check_column_order(old_fields, new_fields))

        # Check 5: SQL reserved word conflicts
        violations.extend(self._check_reserved_words(new_fields))

        # Check 6: Field name changes (renaming)
        violations.extend(self._check_field_renames(old_fields, new_fields))

        # Determine if safe
        has_errors = any(v["type"] == "error" for v in violations)
        is_safe = not has_errors

        if violations:
            logger.warning(f"Schema change validation found {len(violations)} issue(s)")
            for v in violations:
                level = "ERROR" if v["type"] == "error" else "WARNING"
                logger.warning(f"  [{level}] {v['message']}")

        return is_safe, violations

    def _check_removed_fields(self, old_fields: Dict, new_fields: Dict) -> List[Dict]:
        """Check for removed fields (unsafe in SQL)."""
        violations = []
        removed = set(old_fields.keys()) - set(new_fields.keys())

        for field_name in removed:
            violations.append({
                "type": "error",
                "field": field_name,
                "message": (
                    f"Field '{field_name}' removed — SQL queries referencing this "
                    f"column will fail. Use DEPRECATED marker instead, then remove "
                    f"after grace period (30+ days)."
                ),
            })

        return violations

    def _check_type_changes(
        self, old_fields: Dict, new_fields: Dict, compatibility_mode: str
    ) -> List[Dict]:
        """Check for unsafe type changes."""
        violations = []

        for field_name in old_fields.keys() & new_fields.keys():
            old_type = old_fields[field_name].get("type")
            new_type = new_fields[field_name].get("type")

            if old_type == new_type:
                continue

            is_safe, reason = self._is_type_change_safe(old_type, new_type)

            if not is_safe:
                violations.append({
                    "type": "error",
                    "field": field_name,
                    "message": f"Type change {self._format_type(old_type)} → "
                    f"{self._format_type(new_type)}: {reason}",
                })

        return violations

    def _is_type_change_safe(self, old_type: Any, new_type: Any) -> Tuple[bool, str]:
        """Determine if a type change is safe for SQL workloads."""

        # Normalize union types (e.g., ["null", "string"])
        old_base = self._normalize_type(old_type)
        new_base = self._normalize_type(new_type)

        if old_base == new_base:
            return True, "No type change"

        # Safe widening changes
        safe_widening = {
            ("int", "long"): "numeric widening is safe",
            ("float", "double"): "numeric widening is safe",
            ("int", "double"): "numeric widening is safe",
            ("int", "string"): "can cast to string safely",
        }

        if (old_base, new_base) in safe_widening:
            if self.strict_mode:
                return False, "type changes require table recreation"
            return True, safe_widening[(old_base, new_base)]

        # Unsafe narrowing changes
        unsafe_changes = {
            ("long", "int"): "data loss risk on large numbers (>2B)",
            ("double", "float"): "precision loss; existing data won't fit",
            ("string", "int"): "existing string data won't parse to int",
            ("bytes", "string"): "encoding issues; data may be corrupted",
            ("double", "int"): "data loss on non-integer values",
        }

        if (old_base, new_base) in unsafe_changes:
            return False, unsafe_changes[(old_base, new_base)]

        # Other changes require migration planning
        return False, f"type change requires data migration (schema versioning needed)"

    def _check_new_required_fields(self, old_fields: Dict, new_fields: Dict) -> List[Dict]:
        """Check for new required fields without defaults."""
        violations = []

        for field_name in set(new_fields.keys()) - set(old_fields.keys()):
            field_def = new_fields[field_name]
            field_type = field_def.get("type")
            is_nullable = self._is_nullable_type(field_type)
            has_default = "default" in field_def

            if not is_nullable and not has_default:
                violations.append({
                    "type": "error",
                    "field": field_name,
                    "message": (
                        f"New required field '{field_name}' without default — "
                        f"existing SQL INSERT queries will fail. "
                        f"Add a default value or mark as nullable."
                    ),
                })
            elif not is_nullable and has_default:
                violations.append({
                    "type": "warning",
                    "field": field_name,
                    "message": (
                        f"New required field '{field_name}' with default — "
                        f"existing rows will need backfill."
                    ),
                })

        return violations

    def _check_column_order(self, old_fields: Dict, new_fields: Dict) -> List[Dict]:
        """Check for column reordering (dangerous in SQL)."""
        violations = []

        old_order = list(old_fields.keys())
        new_order = list(new_fields.keys())

        # Extract common fields in their respective orders
        common_old = [f for f in old_order if f in new_fields]
        common_new = [f for f in new_order if f in old_fields]

        if common_old != common_new:
            violations.append({
                "type": "warning",
                "field": None,
                "message": (
                    "Column order changed — positional SQL queries "
                    "(SELECT * or SELECT col1, col2...) may return columns in "
                    "different order. Verify downstream queries use explicit column "
                    "names, not positional references."
                ),
            })

        return violations

    def _check_reserved_words(self, new_fields: Dict) -> List[Dict]:
        """Check for SQL reserved word conflicts."""
        violations = []

        for field_name in new_fields.keys():
            if field_name.lower() in SqlReservedWords.RESERVED:
                violations.append({
                    "type": "warning",
                    "field": field_name,
                    "message": (
                        f"Column '{field_name}' is a SQL reserved word. "
                        f"Must use backticks or double quotes in queries. "
                        f"Recommended: rename to avoid confusion."
                    ),
                })

        return violations

    def _check_field_renames(self, old_fields: Dict, new_fields: Dict) -> List[Dict]:
        """Detect potential field renames (same position, different name)."""
        violations = []

        old_order = list(old_fields.keys())
        new_order = list(new_fields.keys())

        # Find fields that exist in old but not in new
        removed = set(old_fields.keys()) - set(new_fields.keys())
        added = set(new_fields.keys()) - set(old_fields.keys())

        # Heuristic: if one field removed and one added at same position, might be rename
        if len(removed) == 1 and len(added) == 1:
            removed_name = list(removed)[0]
            added_name = list(added)[0]
            removed_idx = old_order.index(removed_name)
            added_idx = new_order.index(added_name)

            if removed_idx == added_idx:
                violations.append({
                    "type": "error",
                    "field": removed_name,
                    "message": (
                        f"Possible field rename: '{removed_name}' → '{added_name}' "
                        f"at position {removed_idx}. All downstream SQL queries "
                        f"referencing '{removed_name}' will fail. "
                        f"Plan consumer migration or use alias columns."
                    ),
                })

        return violations

    def _normalize_type(self, avro_type: Any) -> str:
        """Extract base type from AVRO type (handles unions)."""
        if isinstance(avro_type, list):
            # Union type like ["null", "string"]
            non_null = [t for t in avro_type if t != "null"]
            if non_null:
                return non_null[0] if isinstance(non_null[0], str) else str(non_null[0])
            return "null"
        return str(avro_type)

    def _is_nullable_type(self, avro_type: Any) -> bool:
        """Check if a type is nullable (union with null)."""
        if isinstance(avro_type, list):
            return "null" in avro_type
        return False

    def _format_type(self, avro_type: Any) -> str:
        """Format AVRO type for display."""
        if isinstance(avro_type, list):
            return " | ".join(avro_type)
        return str(avro_type)


class DownstreamImpactAnalyzer:
    """Analyzes which downstream tables/views are affected by schema changes."""

    def __init__(self, glue_client):
        self.glue = glue_client

    def analyze_impact(
        self,
        schema_name: str,
        database_name: str = "iceberg_tables",
    ) -> Dict[str, Any]:
        """
        Analyze downstream impact of a schema change.

        Returns:
            Dict with:
            - affected_tables: List of table names using this schema
            - affected_columns: Which columns from each table could break
            - breaking_changes: Summary of what breaks
            - migration_complexity: "low", "medium", "high"
        """
        impact = {
            "schema_name": schema_name,
            "affected_tables": [],
            "affected_columns": [],
            "breaking_changes": [],
            "migration_complexity": "low",
        }

        try:
            # Find all tables in the database
            response = self.glue.get_tables(DatabaseName=database_name)
            tables = response.get("TableList", [])

            for table in tables:
                params = table.get("Parameters", {})
                # Match by contract_id or table name
                if (
                    params.get("contract_id") == schema_name
                    or table["Name"] == schema_name
                ):
                    impact["affected_tables"].append({
                        "name": table["Name"],
                        "database": database_name,
                        "column_count": len(table.get("StorageDescriptor", {}).get("Columns", [])),
                        "partition_keys": table.get("PartitionKeys", []),
                    })

        except Exception as e:
            logger.warning(f"Could not analyze downstream impact: {e}")

        return impact


class SchemaMigrationPlanner:
    """Plans safe schema evolution with coordination periods."""

    @staticmethod
    def plan_field_removal(
        field_name: str,
        grace_period_days: int = 30,
        current_version: str = "1.0.0",
    ) -> Dict[str, Any]:
        """Plan removal of a field with grace period."""
        from datetime import datetime, timedelta

        removal_date = datetime.utcnow() + timedelta(days=grace_period_days)

        return {
            "type": "field_removal",
            "field_name": field_name,
            "status": "planning",
            "grace_period_days": grace_period_days,
            "removal_date": removal_date.isoformat(),
            "phases": [
                {
                    "day": 0,
                    "phase": "deprecation",
                    "action": "Add DEPRECATED marker to field doc",
                    "version": f"{current_version} (bump patch)",
                },
                {
                    "day": 1,
                    "phase": "notification",
                    "action": "Notify downstream consumers via GitHub/email",
                    "version": "same",
                },
                {
                    "day": grace_period_days,
                    "phase": "removal",
                    "action": "Remove field from schema and table",
                    "version": f"{current_version} (bump minor)",
                },
            ],
        }

    @staticmethod
    def plan_type_change(
        field_name: str,
        old_type: str,
        new_type: str,
        has_data: bool = True,
    ) -> Dict[str, Any]:
        """Plan a type change with migration strategy."""

        return {
            "type": "type_change",
            "field_name": field_name,
            "old_type": old_type,
            "new_type": new_type,
            "status": "planning",
            "requires_data_migration": has_data,
            "phases": [
                {
                    "phase": "preparation",
                    "action": "Add new column with new type (if data migration needed)",
                    "duration_hours": 1,
                },
                {
                    "phase": "backfill",
                    "action": f"Backfill {field_name}_new from {field_name}",
                    "duration_hours": "variable (depends on data size)",
                } if has_data else None,
                {
                    "phase": "validation",
                    "action": "Verify data integrity in new column",
                    "duration_hours": 1,
                },
                {
                    "phase": "cutover",
                    "action": f"Swap columns or update application queries",
                    "duration_hours": 1,
                    "downtime_expected": True,
                },
                {
                    "phase": "cleanup",
                    "action": f"Remove old {field_name} column after grace period",
                    "duration_hours": 1,
                },
            ],
        }


def validate_schema_contract(schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate that a schema meets basic SQL-safety requirements.

    Args:
        schema: AVRO schema dict

    Returns:
        (is_valid, list_of_issues)
    """
    issues = []

    # Check required fields
    if "fields" not in schema:
        issues.append("Missing 'fields' array in schema")
        return False, issues

    # Check each field
    for field in schema["fields"]:
        if "name" not in field:
            issues.append("Field missing 'name'")
        if "type" not in field:
            issues.append(f"Field '{field.get('name')}' missing 'type'")

        field_name = field.get("name", "unknown")

        # Check for reserved words
        if field_name.lower() in SqlReservedWords.RESERVED:
            issues.append(
                f"Field '{field_name}' is a SQL reserved word (use backticks in queries)"
            )

        # Check for invalid characters
        if not field_name.replace("_", "").replace("-", "").isalnum():
            issues.append(f"Field '{field_name}' contains invalid characters (use alphanumeric + underscore)")

    return len(issues) == 0, issues