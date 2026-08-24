# SQL-Safety Implementation Guide

## Overview

This document outlines how to implement SQL-safe schema evolution in your Glue schema registry. The code provided adds validation layers to prevent schema changes that could break downstream SQL queries on Athena, Redshift, or Spark SQL.

## Files Added/Modified

### New Files
- **`airflow/lib/schema_validator.py`** — Core SQL-safety validation logic
- **`airflow/tests/test_schema_validator.py`** — Comprehensive test suite
- **`SQL_SAFETY_IMPLEMENTATION.md`** (this file) — Implementation guide

### Modified Files
- **`airflow/lib/aws_glue.py`** — Updated to integrate SQL-safety checks into schema registration

## Key Components

### 1. SchemaSafetyValidator

Validates schema changes for SQL-safety before Avro validation:

```python
from lib.schema_validator import SchemaSafetyValidator

validator = SchemaSafetyValidator(strict_mode=True)

old_schema = {...}
new_schema = {...}

is_safe, violations = validator.validate_schema_change(
    old_schema, 
    new_schema, 
    compatibility_mode="FORWARD_ALL"
)

if not is_safe:
    for v in violations:
        print(f"[{v['type'].upper()}] {v['message']}")
```

### 2. DownstreamImpactAnalyzer

Identifies which tables are affected by schema changes:

```python
from lib.schema_validator import DownstreamImpactAnalyzer

analyzer = DownstreamImpactAnalyzer(glue_client)
impact = analyzer.analyze_impact("my_schema", database_name="iceberg_tables")

print(f"Affected tables: {impact['affected_tables']}")
print(f"Migration complexity: {impact['migration_complexity']}")
```

### 3. SchemaMigrationPlanner

Plans safe evolution strategies for risky changes:

```python
from lib.schema_validator import SchemaMigrationPlanner

# Plan field removal
plan = SchemaMigrationPlanner.plan_field_removal(
    field_name="deprecated_field",
    grace_period_days=30
)

# Plan type change
plan = SchemaMigrationPlanner.plan_type_change(
    field_name="count",
    old_type="int",
    new_type="long",
    has_data=True
)
```

## Integration Steps

### Step 1: Enable SQL-Safety in AwsGlueAdapter

By default, SQL-safety checks are **enabled**. When registering a schema:

```python
adapter = AwsGlueAdapter(
    region="us-east-1",
    registry_name="schema-registry",
    enforce_sql_safety=True  # Default: True
)

schema_arn = adapter.register_schema(
    contract=data_contract,
    data_format="AVRO",
    compatibility="FORWARD_ALL",
    enforce_sql_safety=True  # Can override per registration
)
```

**What happens:**
1. Old schema version is fetched
2. SQL-safety validation runs (new)
3. If violations detected → registration fails with detailed error
4. If safe → continues to Avro compatibility check
5. Glue validates compatibility
6. Schema is registered

### Step 2: Handle SQL-Safety Violations

When SQL-safety validation fails, you have options:

**Option A: Fix the Schema**
```python
# Instead of removing a column, deprecate it
# v1.1 Schema (safe)
{
    "name": "old_field",
    "type": "string",
    "doc": "DEPRECATED: Use new_field instead. Will be removed in v2.0"
}

# Later, in v2.0 (after 30-day grace period), remove it
```

**Option B: Create Migration Plan**
```python
from lib.schema_validator import SchemaMigrationPlanner

# For unsafe changes, create a migration plan
plan = SchemaMigrationPlanner.plan_field_removal(
    field_name="user_id",
    grace_period_days=60  # Extended grace period
)

# Share this plan with downstream teams
print(json.dumps(plan, indent=2))
```

**Option C: Disable Temporarily (Development Only)**
```python
# For dev/testing, can bypass SQL-safety checks
schema_arn = adapter.register_schema(
    contract=data_contract,
    enforce_sql_safety=False  # ONLY for testing!
)
```

### Step 3: Update Your DAG (Optional but Recommended)

Add a compatibility check task before schema/table updates:

```python
# In contract_provisioning_dag.py

def task_validate_schema_table_sync(**context):
    """Ensure schema and table will stay in sync after updates."""
    adapter = AwsGlueAdapter()
    
    validation_results = context["task_instance"].xcom_pull(
        task_ids="validate_contract"
    )
    
    for result in validation_results:
        contract_id = result["contract_id"]
        
        # Get current versions
        current_schema = adapter.get_schema_versions(contract_id)
        current_table = adapter.get_table(contract_id)
        
        # Validate they match
        if current_schema and current_table:
            schema_fields = current_schema["schema"]["fields"]
            table_columns = current_table.columns
            
            for i, (field, col) in enumerate(zip(schema_fields, table_columns)):
                if field["name"] != col.name:
                    raise ValueError(
                        f"Schema-table mismatch at position {i}: "
                        f"schema has '{field['name']}', table has '{col.name}'"
                    )

# Add to DAG
with dag:
    fetch_task = PythonOperator(...)
    validate_tasks = PythonOperator.partial(...).expand(...)
    
    sync_check = PythonOperator(
        task_id="check_schema_table_sync",
        python_callable=task_validate_schema_table_sync,
    )
    
    # New dependency: sync check before provisioning
    fetch_task >> validate_tasks >> sync_check >> schema_provisioning >> ...
```

## Testing

Run the test suite to verify SQL-safety checks:

```bash
cd /Users/vanik_kars/Documents/learning/python/fast_api/data-contract-flow
python -m pytest airflow/tests/test_schema_validator.py -v
```

**Expected results:**
```
test_add_optional_field PASSED
test_remove_field PASSED (detected as unsafe)
test_narrow_type_change PASSED (detected as unsafe)
test_new_required_field_without_default PASSED (detected as unsafe)
test_reorder_detection PASSED (warning)
test_reserved_word_warning PASSED (warning)
test_user_event_v1_to_v2 PASSED (multiple changes coordinated)
...
```

## Real-World Examples

### Example 1: Safe Schema Evolution (Recommended Pattern)

```python
# v1.0 Schema
v1 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "timestamp", "type": "string"},
    ]
}

# v1.1 Schema - ADD optional field
v1_1 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "device_type", "type": ["null", "string"], "default": None},
    ]
}

# v1.2 Schema - DEPRECATE old field
v1_2 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "timestamp", "type": "string", "doc": "DEPRECATED in v2.0"},
        {"name": "device_type", "type": ["null", "string"], "default": None},
        {"name": "timestamp_utc", "type": "string", "doc": "ISO8601 UTC timestamp"},
    ]
}

# v2.0 Schema - REMOVE old field (after 30+ day grace period)
v2_0 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "device_type", "type": ["null", "string"], "default": None},
        {"name": "timestamp_utc", "type": "string"},
    ]
}
```

**Validation results:**
- ✅ v1.0 → v1.1: Safe (only addition of optional field)
- ✅ v1.1 → v1.2: Safe (only addition of optional field + deprecation marker)
- ✅ v1.2 → v2.0: Safe (only removal of deprecated field)

### Example 2: Unsafe Change Detected

```python
# Trying to do this breaks SQL queries
unsafe_change = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        # {"name": "user_id", "type": "string"},  # REMOVED!
        {"name": "timestamp", "type": "string"},
    ]
}

# Validation error:
# ERROR: Field 'user_id' removed — SQL queries referencing this 
#        column will fail. Use DEPRECATED marker instead, or plan 
#        data migration before physical removal.
```

**Solution:** Use the migration plan:

```python
plan = SchemaMigrationPlanner.plan_field_removal(
    field_name="user_id",
    grace_period_days=30
)

# Share with teams:
# Phase 1 (Day 0): Mark DEPRECATED in schema v1.1
# Phase 2 (Day 1): Notify downstream consumers
# Phase 3 (Day 31): Remove field in schema v2.0
```

## Monitoring & Observability

### Log Examples

**Successful registration with warnings:**
```
INFO: Schema definition CHANGED for user_events
WARNING: ⚠️  SQL-Safety warnings for user_events:
  [WARNING] Column order changed. Positional SQL (SELECT * FROM...) 
            or SELECT by position will be affected.
INFO: 📊 This change affects 3 table(s): user_events, user_analytics, session_tracking
INFO: Registered new version 5 for schema user_events
```

**Registration failure due to SQL-safety:**
```
ERROR: ❌ SQL-Safety violations for user_events:
  [ERROR] Field 'user_id' removed — SQL queries referencing this 
          column will fail. Use DEPRECATED marker instead, or plan 
          data migration before physical removal.
ERROR: ❌ Compatibility violation: Schema change violates SQL-safety 
        requirements. Changes could break downstream SQL queries
```

### Metrics to Track

Consider tracking these metrics in CloudWatch/Grafana:

1. **Schema registration attempts** — success vs. SQL-safety violations
2. **Violation types** — broken down by: field removal, type changes, required fields, etc.
3. **DEPRECATED fields** — count of fields marked for removal, time in deprecated state
4. **Migration duration** — how long from deprecation to removal
5. **Downstream impact** — number of affected tables per schema change

## Best Practices

### ✅ DO

1. **Always use semantic versioning** for schemas (1.0.0, 1.1.0, 2.0.0)
2. **Add field deprecation 30+ days** before removal
3. **Use descriptive field names** that describe intent, not position
4. **Mark computed/derived columns** in documentation
5. **Run SQL-safety checks** before registering any schema change
6. **Coordinate with downstream teams** for breaking changes
7. **Keep audit logs** of all schema changes
8. **Test schema migrations** in dev environment first

### ❌ DON'T

1. **Don't remove fields directly** — deprecate first, then remove
2. **Don't rename fields** without migration period (use aliases)
3. **Don't change type without backfill plan** — risks data loss
4. **Don't reorder columns** without checking impact on position-based code
5. **Don't bypass SQL-safety checks** in production (only dev/test)
6. **Don't use SQL reserved words** for column names (or quote them)
7. **Don't skip notifying consumers** of breaking changes

## Troubleshooting

### Issue: "Schema change violates SQL-safety"

**Check:**
1. Are you removing a field? → Deprecate instead
2. Are you renaming a field? → Add new field, deprecate old
3. Are you changing type? → Check if it's a narrowing change (int→short, double→float)
4. Are you adding required field without default? → Add a default value

### Issue: "Schema and table are out of sync"

**Check:**
1. Did schema registration succeed but table update failed? → Retry table update
2. Are column names different? → Update table schema to match
3. Are columns in different order? → Reorder table columns to match schema

### Issue: Downstream queries failing after schema change

**Check:**
1. Did SQL-safety validation pass? → Run validation again to debug
2. Are queries using positional SELECT? → Use explicit column names
3. Are queries checking for removed fields? → Add the field back, mark as DEPRECATED
4. Is Glue table metadata stale? → Refresh with `get_table()` and `update_table()`

## Related Documentation

- [Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/webapi/API_CreateSchema.html)
- [Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/schema-evolution/)
- [AVRO Spec](https://avro.apache.org/docs/current/spec.html)
- [Athena Type Mappings](https://docs.aws.amazon.com/athena/latest/ug/data-types.html)

## Future Enhancements

- [ ] Integrate with GitHub API to notify PRs of breaking changes
- [ ] Add Slack notifications for schema changes affecting multiple tables
- [ ] Implement automatic rollback on downstream query failures
- [ ] Create schema change templates for common patterns
- [ ] Add data migration scheduling (Spark jobs for backfills)
- [ ] Build schema diff visualization for PR reviews