"""Tests for SQL-safety schema validation."""

import pytest
from lib.schema_validator import (
    SchemaSafetyValidator,
    ChangeType,
    validate_schema_contract,
)


@pytest.fixture
def validator():
    return SchemaSafetyValidator(strict_mode=True)


@pytest.fixture
def base_schema():
    """Base schema for comparison."""
    return {
        "type": "record",
        "name": "UserEvent",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "timestamp", "type": "string"},
        ],
    }


class TestSafeSchemaChanges:
    """Test changes that are SQL-safe."""

    def test_add_optional_field(self, validator, base_schema):
        """Adding an optional field should be safe."""
        new_schema = {
            **base_schema,
            "fields": [
                *base_schema["fields"],
                {"name": "event_metadata", "type": ["null", "string"], "default": None},
            ],
        }

        is_safe, violations = validator.validate_schema_change(base_schema, new_schema)
        assert is_safe, f"Should be safe but got violations: {violations}"

    def test_add_field_with_default(self, validator, base_schema):
        """Adding a required field with default is risky but allowable."""
        new_schema = {
            **base_schema,
            "fields": [
                *base_schema["fields"],
                {"name": "status", "type": "string", "default": "active"},
            ],
        }

        is_safe, violations = validator.validate_schema_change(base_schema, new_schema)
        # Should be risky but SQL-safe with backfill
        warnings = [v for v in violations if v["type"] == "warning"]
        assert any("default" in v["message"] for v in warnings)


class TestUnsafeSchemaChanges:
    """Test changes that are NOT SQL-safe."""

    def test_remove_field(self, validator, base_schema):
        """Removing a field is unsafe."""
        new_schema = {
            **base_schema,
            "fields": base_schema["fields"][:2],  # Remove timestamp
        }

        is_safe, violations = validator.validate_schema_change(base_schema, new_schema)
        assert not is_safe
        assert any("timestamp" in v["message"] and v["type"] == "error" for v in violations)

    def test_narrow_type_change(self, validator, base_schema):
        """Narrowing type (long to int) is unsafe."""
        new_schema = {
            **base_schema,
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "count", "type": "long"},
            ],
        }
        base_with_long = {
            **base_schema,
            "fields": [
                *base_schema["fields"],
                {"name": "count", "type": "long"},
            ],
        }

        # Change long to int (narrowing)
        schema_with_int = {
            **base_with_long,
            "fields": [
                base_with_long["fields"][0],
                base_with_long["fields"][1],
                base_with_long["fields"][2],
                {"name": "count", "type": "int"},
            ],
        }

        is_safe, violations = validator.validate_schema_change(base_with_long, schema_with_int)
        assert not is_safe
        assert any("data loss" in v["message"].lower() for v in violations)

    def test_new_required_field_without_default(self, validator, base_schema):
        """Adding required field without default breaks inserts."""
        new_schema = {
            **base_schema,
            "fields": [
                *base_schema["fields"],
                {"name": "required_field", "type": "string"},  # No default!
            ],
        }

        is_safe, violations = validator.validate_schema_change(base_schema, new_schema)
        assert not is_safe
        assert any("required" in v["message"].lower() and v["type"] == "error" for v in violations)


class TestTypeChanges:
    """Test type change safety assessment."""

    def test_numeric_widening_safe(self, validator):
        """int -> long widening should be safe (with data migration)."""
        old_schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "count", "type": "int"}],
        }
        new_schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "count", "type": "long"}],
        }

        is_safe, violations = validator.validate_schema_change(old_schema, new_schema)
        # In strict mode, type changes require table recreation
        # but this is the safe direction for numeric change
        assert len(violations) > 0  # Will warn about type change in strict mode

    def test_string_to_int_unsafe(self, validator):
        """string -> int is unsafe (parse failures)."""
        old_schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "value", "type": "string"}],
        }
        new_schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "value", "type": "int"}],
        }

        is_safe, violations = validator.validate_schema_change(old_schema, new_schema)
        assert not is_safe
        assert any("won't parse" in v["message"].lower() for v in violations)


class TestColumnOrder:
    """Test detection of column reordering."""

    def test_reorder_detection(self, validator, base_schema):
        """Reordering columns should trigger a warning."""
        new_schema = {
            **base_schema,
            "fields": [
                base_schema["fields"][2],  # timestamp first
                base_schema["fields"][0],  # user_id second
                base_schema["fields"][1],  # event_type third
            ],
        }

        is_safe, violations = validator.validate_schema_change(base_schema, new_schema)
        assert any("order changed" in v["message"].lower() for v in violations)


class TestReservedWords:
    """Test detection of SQL reserved words."""

    def test_reserved_word_warning(self, validator):
        """Using SQL reserved words should trigger warning."""
        schema = {
            "type": "record",
            "name": "BadSchema",
            "fields": [
                {"name": "select", "type": "string"},  # Reserved!
                {"name": "user_id", "type": "string"},
            ],
        }

        is_safe, violations = validator.validate_schema_change({}, schema)
        # Is safe but has warning
        assert any("reserved word" in v["message"].lower() for v in violations)


class TestContractValidation:
    """Test contract-level schema validation."""

    def test_valid_schema_contract(self):
        """Valid contract passes validation."""
        schema = {
            "type": "record",
            "name": "GoodSchema",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
            ],
        }

        is_valid, issues = validate_schema_contract(schema)
        assert is_valid
        assert len(issues) == 0

    def test_schema_with_reserved_words(self):
        """Schema with reserved words gets a warning."""
        schema = {
            "type": "record",
            "name": "BadSchema",
            "fields": [
                {"name": "select", "type": "string"},
            ],
        }

        is_valid, issues = validate_schema_contract(schema)
        assert any("reserved word" in issue.lower() for issue in issues)

    def test_schema_missing_fields(self):
        """Schema without fields array is invalid."""
        schema = {
            "type": "record",
            "name": "NoFields",
        }

        is_valid, issues = validate_schema_contract(schema)
        assert not is_valid
        assert any("fields" in issue.lower() for issue in issues)


class TestComplexScenarios:
    """Test complex, realistic schema evolution scenarios."""

    def test_user_event_v1_to_v2(self, validator):
        """Real scenario: user event schema v1 -> v2."""
        v1_schema = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "properties", "type": ["null", "string"], "default": None},
            ],
        }

        # Safe v2: add optional fields, mark some as deprecated
        v2_schema = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {
                    "name": "event_id",
                    "type": "string",
                    "doc": "Unique event identifier",
                },
                {
                    "name": "user_id",
                    "type": "string",
                    "doc": "User ID",
                },
                {
                    "name": "event_type",
                    "type": "string",
                    "doc": "Type of event",
                },
                {
                    "name": "timestamp",
                    "type": "string",
                    "doc": "DEPRECATED: Use timestamp_utc instead",
                },
                {
                    "name": "timestamp_utc",
                    "type": "string",
                    "doc": "Event timestamp in UTC",
                },
                {
                    "name": "properties",
                    "type": ["null", "string"],
                    "default": None,
                    "doc": "DEPRECATED: Use event_metadata instead",
                },
                {
                    "name": "event_metadata",
                    "type": ["null", "string"],
                    "default": None,
                    "doc": "Structured event metadata",
                },
                {
                    "name": "session_id",
                    "type": ["null", "string"],
                    "default": None,
                    "doc": "Session identifier",
                },
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1_schema, v2_schema)
        # Should be safe (only additions and deprecations)
        errors = [v for v in violations if v["type"] == "error"]
        assert len(errors) == 0, f"Should be safe but got errors: {errors}"

    def test_breaking_column_rename(self, validator):
        """Unsafe scenario: rename field without coordination."""
        v1_schema = {
            "type": "record",
            "name": "UserProfile",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "full_name", "type": "string"},  # Will be renamed
            ],
        }

        v2_schema = {
            "type": "record",
            "name": "UserProfile",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "name", "type": "string"},  # Renamed from full_name
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1_schema, v2_schema)
        assert not is_safe
        # Should detect field removal and rename
        assert any("rename" in v["message"].lower() for v in violations)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])