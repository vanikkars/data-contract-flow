"""Tests for critical bug fixes in SQL-safety schema validation.

These tests verify that the following critical bugs have been fixed:
1. Type change validation broken in strict mode (now engine-aware)
2. Field rename detection using position heuristic (now semantic)
3. Reserved word check case-insensitive (fixed)
4. Nullable→non-nullable not detected (now detected)
5. Union type analysis incomplete (placeholder for future work)
"""

import pytest
from lib.schema_validator import SchemaSafetyValidator


class TestBugFix1TypeChangeValidation:
    """Bug #1: Type change validation broken in strict mode (FIXED)."""

    def test_iceberg_type_widening_allowed(self):
        """Iceberg supports metadata-only type promotion (no recreation)."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        old_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "int"}],
        }
        new_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "long"}],
        }

        is_safe, violations = validator.validate_schema_change(old_schema, new_schema)

        # Should be allowed (Iceberg metadata-only change)
        assert is_safe, "Iceberg type widening should be allowed"

        # Should have no violations
        errors = [v for v in violations if v["type"] == "error"]
        assert len(errors) == 0, "Iceberg type widening should not have errors"

    def test_other_engines_type_widening_warns(self):
        """Other SQL engines require migration planning for type widening."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="athena")

        old_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "int"}],
        }
        new_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "long"}],
        }

        is_safe, violations = validator.validate_schema_change(old_schema, new_schema)

        # Should be allowed but with warning
        assert is_safe, "Type widening should be allowed (with migration plan)"

        # Should have warning about migration
        warnings = [v for v in violations if v["type"] == "warning"]
        assert any("migration" in v["message"].lower() for v in warnings)

    def test_type_narrowing_still_blocked(self):
        """Unsafe type narrowing should still be blocked."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        old_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "long"}],
        }
        new_schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": "count", "type": "int"}],
        }

        is_safe, violations = validator.validate_schema_change(old_schema, new_schema)

        # Should NOT be allowed (data loss risk)
        assert not is_safe, "Type narrowing should be blocked"

        # Should have error
        errors = [v for v in violations if v["type"] == "error"]
        assert any("data loss" in v["message"].lower() for v in errors)


class TestBugFix2FieldRenameDetection:
    """Bug #2: Field rename detection using position (FIXED)."""

    def test_rename_with_explicit_marker(self):
        """Rename with documentation marker should be detected."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        v1 = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "full_name", "type": "string"}],
        }
        v2 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string", "doc": "renamed from full_name"}
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1, v2)

        # Should be blocked (requires migration)
        assert not is_safe, "Rename should be detected and blocked"

        # Should mention rename
        assert any("rename" in v["message"].lower() for v in violations)

    def test_rename_with_type_match(self):
        """Rename with exact type match should be detected."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        v1 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "timestamp", "type": "string"},
                {"name": "user_id", "type": "string"},
            ],
        }
        v2 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "event_time", "type": "string"},  # Same type, different name
                {"name": "user_id", "type": "string"},
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1, v2)

        # Should be blocked (semantic rename)
        assert not is_safe, "Type-matched rename should be detected"
        assert any("rename" in v["message"].lower() for v in violations)

    def test_position_coincidence_not_flagged(self):
        """Position match without type match should NOT be flagged as rename."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        v1 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "timestamp", "type": "string"},      # pos 0
                {"name": "user_id", "type": "string"},        # pos 1
            ],
        }
        v2 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "timestamp", "type": "string"},
                {"name": "session_id", "type": "int"},  # Different type, same position
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1, v2)

        # Should NOT flag as rename (different types)
        # Will flag as removal + addition + new required field, not rename
        rename_errors = [v for v in violations if "rename" in v["message"].lower()]
        assert len(rename_errors) == 0, "Position match with different types should not flag as rename"


class TestBugFix3ReservedWords:
    """Bug #3: Reserved word check case-insensitive (FIXED)."""

    def test_reserved_word_lowercase(self):
        """Lowercase reserved words should be caught."""
        validator = SchemaSafetyValidator(strict_mode=False)

        schema = {
            "type": "record",
            "name": "Data",
            "fields": [{"name": "select", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change({}, schema)

        # Should warn
        assert any("reserved word" in v["message"].lower() for v in violations)

    def test_reserved_word_uppercase(self):
        """Uppercase reserved words should be caught."""
        validator = SchemaSafetyValidator(strict_mode=False)

        schema = {
            "type": "record",
            "name": "Data",
            "fields": [{"name": "SELECT", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change({}, schema)

        # Should warn (was missing before fix)
        assert any("reserved word" in v["message"].lower() for v in violations)

    def test_reserved_word_mixed_case(self):
        """Mixed case reserved words should be caught."""
        validator = SchemaSafetyValidator(strict_mode=False)

        schema = {
            "type": "record",
            "name": "Data",
            "fields": [{"name": "FrOm", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change({}, schema)

        # Should warn (was missing before fix)
        assert any("reserved word" in v["message"].lower() for v in violations)


class TestBugFixGap1NullablePromotion:
    """Gap #1: Nullable→Non-Nullable promotion not detected (FIXED)."""

    def test_nullable_to_non_nullable_error(self):
        """Promotion from nullable to non-nullable should error."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        nullable_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "status", "type": ["null", "string"], "default": None}],
        }
        non_nullable_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "status", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change(
            nullable_schema, non_nullable_schema
        )

        # Should be blocked (data loss)
        assert not is_safe, "Nullable promotion should be blocked"

        # Should have error
        errors = [v for v in violations if v["type"] == "error"]
        assert any("promoted" in v["message"].lower() for v in errors)
        assert any("nullable" in v["message"].lower() for v in errors)

    def test_nullable_to_non_nullable_mentions_solution(self):
        """Error message should explain the solution."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        nullable_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "email", "type": ["null", "string"], "default": None}],
        }
        non_nullable_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "email", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change(
            nullable_schema, non_nullable_schema
        )

        # Should mention backfill solution
        error_msgs = [v["message"] for v in violations if v["type"] == "error"]
        assert any("backfill" in msg.lower() for msg in error_msgs)

    def test_non_nullable_stays_non_nullable(self):
        """Non-nullable field staying non-nullable should be OK."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        schema1 = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "user_id", "type": "string"}],
        }
        schema2 = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "user_id", "type": "string"}],
        }

        is_safe, violations = validator.validate_schema_change(schema1, schema2)

        # Should be fine (no changes)
        assert is_safe
        assert len(violations) == 0


class TestIntegration:
    """Integration tests for multiple bug fixes."""

    def test_complex_schema_evolution(self):
        """Real-world scenario: multiple changes in one schema update."""
        validator = SchemaSafetyValidator(strict_mode=True, engine="iceberg")

        v1 = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "status", "type": ["null", "string"], "default": None},
                {"name": "count", "type": "int"},
            ],
        }

        v2 = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "timestamp_utc", "type": "string", "doc": "renamed from timestamp"},
                # Made non-nullable - BUG SHOULD CATCH THIS
                {"name": "status", "type": "string"},
                # Type widening - OK for Iceberg
                {"name": "count", "type": "long"},
                # New optional field - OK
                {"name": "session_id", "type": ["null", "string"], "default": None},
            ],
        }

        is_safe, violations = validator.validate_schema_change(v1, v2)

        # Should catch nullable promotion and rename
        assert not is_safe, "Should detect data loss and breaking changes"

        # Should have errors for nullable promotion and field removal
        error_msgs = [v["message"] for v in violations if v["type"] == "error"]
        assert any("promoted" in msg for msg in error_msgs), "Should catch nullable promotion"
        assert any("rename" in msg.lower() for msg in error_msgs), "Should catch rename"

        # Type widening should be OK (warning or none for Iceberg)
        type_change_msgs = [v["message"] for v in violations if "int → long" in v["message"]]
        # Should be fine - no error
        assert len(type_change_msgs) == 0 or all(v["type"] != "error" for v in violations if "int → long" in v["message"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])