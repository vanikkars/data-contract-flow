"""Tests for Layer 1: AVRO logical type safety (decimal, timestamp).

These changes all pass AVRO compatibility because the underlying physical type
is unchanged (bytes for decimal, long for timestamp). The corruption is silent,
which makes them the highest-cost gap in the SQL-safety validator.
"""

import pytest

from lib.schema_validator import SchemaSafetyValidator


@pytest.fixture
def validator():
    return SchemaSafetyValidator(strict_mode=True, engine="iceberg")


def decimal(precision: int, scale: int) -> dict:
    return {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": precision,
        "scale": scale,
    }


def timestamp(unit: str) -> dict:
    return {"type": "long", "logicalType": f"timestamp-{unit}"}


def schema_with(field_type) -> dict:
    return {
        "type": "record",
        "name": "Transaction",
        "fields": [{"name": "price", "type": field_type}],
    }


def errors_from(validator, old_type, new_type):
    _, violations = validator.validate_schema_change(
        schema_with(old_type), schema_with(new_type)
    )
    return [v for v in violations if v["type"] == "error"]


class TestDecimalPrecisionAndScale:
    """The DECIMAL(18,4) -> DECIMAL(10,2) financial-corruption case."""

    def test_scale_change_is_error(self, validator):
        """Changing scale silently rescales every stored value."""
        errors = errors_from(validator, decimal(18, 4), decimal(10, 2))

        assert len(errors) == 1
        assert "scale change" in errors[0]["message"]
        assert errors[0]["field"] == "price"

    def test_precision_narrowing_is_error(self, validator):
        """Narrowing precision truncates values that no longer fit."""
        errors = errors_from(validator, decimal(18, 4), decimal(10, 4))

        assert len(errors) == 1
        assert "precision narrowing" in errors[0]["message"]

    def test_precision_widening_is_safe(self, validator):
        """Widening precision at equal scale loses nothing."""
        assert errors_from(validator, decimal(10, 2), decimal(18, 2)) == []

    def test_identical_decimal_is_safe(self, validator):
        assert errors_from(validator, decimal(18, 4), decimal(18, 4)) == []

    def test_nullable_decimal_narrowing_is_caught(self, validator):
        """Union-wrapped decimals must be unwrapped before comparison."""
        errors = errors_from(
            validator, ["null", decimal(18, 4)], ["null", decimal(10, 2)]
        )

        assert len(errors) == 1
        assert "scale change" in errors[0]["message"]

    def test_missing_precision_is_flagged(self, validator):
        """An unverifiable decimal must not be waved through."""
        malformed = {"type": "bytes", "logicalType": "decimal", "scale": 2}
        errors = errors_from(validator, decimal(18, 2), malformed)

        assert len(errors) == 1
        assert "missing precision" in errors[0]["message"]


class TestTimestampUnits:
    """timestamp-millis -> timestamp-micros is AVRO-compatible but 1000x off."""

    def test_millis_to_micros_is_error(self, validator):
        errors = errors_from(validator, timestamp("millis"), timestamp("micros"))

        assert len(errors) == 1
        assert "1000x" in errors[0]["message"]
        assert "inflated" in errors[0]["message"]

    def test_micros_to_millis_is_error(self, validator):
        errors = errors_from(validator, timestamp("micros"), timestamp("millis"))

        assert len(errors) == 1
        assert "truncated" in errors[0]["message"]

    def test_time_millis_to_micros_is_error(self, validator):
        errors = errors_from(
            validator,
            {"type": "int", "logicalType": "time-millis"},
            {"type": "long", "logicalType": "time-micros"},
        )

        assert len(errors) == 1
        assert "1000x" in errors[0]["message"]

    def test_identical_timestamp_is_safe(self, validator):
        assert errors_from(validator, timestamp("millis"), timestamp("millis")) == []


class TestLogicalTypeAddedOrRemoved:
    """Dropping a logical type reinterprets the same bytes."""

    def test_dropping_logical_type_is_error(self, validator):
        errors = errors_from(validator, timestamp("millis"), "long")

        assert len(errors) == 1
        assert "reinterpreted" in errors[0]["message"]

    def test_adding_logical_type_is_error(self, validator):
        errors = errors_from(validator, "long", timestamp("millis"))

        assert len(errors) == 1
        assert "reinterpreted" in errors[0]["message"]

    def test_unrelated_logical_swap_is_error(self, validator):
        errors = errors_from(
            validator,
            {"type": "int", "logicalType": "date"},
            {"type": "string", "logicalType": "uuid"},
        )

        assert len(errors) == 1
        assert "date" in errors[0]["message"]
        assert "uuid" in errors[0]["message"]


class TestPlainTypesStillWork:
    """Logical-type handling must not regress plain base-type checks."""

    def test_int_to_long_still_safe_on_iceberg(self, validator):
        assert errors_from(validator, "int", "long") == []

    def test_long_to_int_still_unsafe(self, validator):
        errors = errors_from(validator, "long", "int")

        assert len(errors) == 1
        assert "data loss" in errors[0]["message"]

    def test_unchanged_plain_type_is_safe(self, validator):
        assert errors_from(validator, "string", "string") == []


class TestTypeFormatting:
    """Violation messages must render logical types readably."""

    def test_decimal_renders_with_precision_and_scale(self, validator):
        assert validator._format_type(decimal(18, 4)) == "decimal(18,4)"

    def test_nullable_decimal_renders_readably(self, validator):
        formatted = validator._format_type(["null", decimal(18, 4)])

        assert formatted == "null | decimal(18,4)"

    def test_timestamp_renders_as_logical_name(self, validator):
        assert validator._format_type(timestamp("millis")) == "timestamp-millis"