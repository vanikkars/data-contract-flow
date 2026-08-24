"""Tests for Layer 0: registry compatibility mode and Iceberg table properties.

Layer 0 is the coarse gate — it blocks the loud breaks (field removal, required
field addition) at the registry before the SQL-safety validator runs.
"""

import logging

from lib.aws_glue import (
    SAFE_COMPATIBILITY_MODES,
    WEAK_COMPATIBILITY_MODES,
    DEFAULT_COMPATIBILITY,
    AwsGlueAdapter,
)
from lib.models import ICEBERG_SAFETY_PROPERTIES, IcebergTable, Column, DataType


class TestCompatibilityDefaults:
    """The default mode must be strong enough for a source-aligned raw layer."""

    def test_default_is_full_all(self):
        """FULL_ALL forbids both field removal and required-field addition."""
        assert DEFAULT_COMPATIBILITY == "FULL_ALL"

    def test_default_is_in_safe_set(self):
        assert DEFAULT_COMPATIBILITY in SAFE_COMPATIBILITY_MODES

    def test_backward_is_classified_weak(self):
        """BACKWARD is the registry default and permits field deletion."""
        assert "BACKWARD" in WEAK_COMPATIBILITY_MODES
        assert "deletion" in WEAK_COMPATIBILITY_MODES["BACKWARD"]

    def test_forward_all_is_classified_weak(self):
        """The previous default permitted adding required fields."""
        assert "FORWARD_ALL" in WEAK_COMPATIBILITY_MODES

    def test_safe_and_weak_sets_are_disjoint(self):
        assert not (SAFE_COMPATIBILITY_MODES & set(WEAK_COMPATIBILITY_MODES))


class TestWeakCompatibilityWarning:
    """A weak mode is permitted but never silent."""

    def _adapter(self):
        # __init__ builds boto3 clients; bypass it since we only exercise
        # the pure-logic helper.
        return AwsGlueAdapter.__new__(AwsGlueAdapter)

    def test_warns_on_backward(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._adapter()._warn_on_weak_compatibility("user-v1", "BACKWARD")

        assert "BACKWARD" in caplog.text
        assert "FULL_ALL" in caplog.text

    def test_warns_on_unrecognised_mode(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._adapter()._warn_on_weak_compatibility("user-v1", "BANANA")

        assert "not a recognised strong mode" in caplog.text

    def test_silent_on_full_all(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._adapter()._warn_on_weak_compatibility("user-v1", "FULL_ALL")

        assert caplog.text == ""

    def test_mode_is_case_insensitive(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._adapter()._warn_on_weak_compatibility("user-v1", "full_all")

        assert caplog.text == ""

    def test_handles_none_mode(self, caplog):
        """A missing mode must warn, not crash."""
        with caplog.at_level(logging.WARNING):
            self._adapter()._warn_on_weak_compatibility("user-v1", None)

        assert "user-v1" in caplog.text


class TestIcebergSafetyProperties:
    """Metadata retention is what makes snapshot-level schema review possible."""

    def test_format_version_2(self):
        assert ICEBERG_SAFETY_PROPERTIES["format-version"] == "2"

    def test_metadata_retained_after_commit(self):
        """Deleting metadata destroys the evidence for evolution review."""
        assert (
            ICEBERG_SAFETY_PROPERTIES["write.metadata.delete-after-commit.enabled"]
            == "false"
        )

    def test_properties_applied_to_created_tables(self):
        table = IcebergTable(
            table_name="user_events",
            contract_id="user-v1",
            version="1.0.0",
            columns=[Column(name="user_id", data_type=DataType.STRING)],
        )

        params = table.get_table_parameters()

        for key, value in ICEBERG_SAFETY_PROPERTIES.items():
            assert params[key] == value

    def test_safety_properties_do_not_clobber_contract_params(self):
        table = IcebergTable(
            table_name="user_events",
            contract_id="user-v1",
            version="1.0.0",
            columns=[Column(name="user_id", data_type=DataType.STRING)],
        )

        params = table.get_table_parameters()

        assert params["contract_id"] == "user-v1"
        assert params["table_type"] == "ICEBERG"