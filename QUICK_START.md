# Quick Start: SQL-Safe Schema Evolution

## TL;DR

**Problem:** Glue compatibility modes validate Avro rules, not SQL queries. Changes that pass Avro validation can still break Athena/Redshift queries.

**Solution:** New `SchemaSafetyValidator` catches SQL-breaking changes before registration.

## 5-Minute Integration

### 1. Import the Validator

```python
from lib.schema_validator import SchemaSafetyValidator

validator = SchemaSafetyValidator(strict_mode=True)
```

### 2. Validate Before Registering

```python
old_schema = json.loads(current_schema_definition)
new_schema = json.loads(proposed_schema_definition)

is_safe, violations = validator.validate_schema_change(
    old_schema, new_schema, compatibility_mode="FORWARD_ALL"
)

if not is_safe:
    errors = [v for v in violations if v["type"] == "error"]
    for e in errors:
        print(f"❌ {e['message']}")
    raise ValueError("Schema change is not SQL-safe")
```

### 3. Already Integrated in AwsGlueAdapter

If you're using `AwsGlueAdapter`, SQL-safety is **already enabled**:

```python
adapter = AwsGlueAdapter(enforce_sql_safety=True)  # Default

schema_arn = adapter.register_schema(contract, compatibility="FORWARD_ALL")
# ✅ SQL-safety check runs automatically before Avro validation
```

## What It Catches

### ✅ Safe Changes (Always Allowed)

```python
# Adding an optional field
"new_field": {
    "type": ["null", "string"],
    "default": None,
}
# ✅ SAFE: Old queries work, new queries can use field

# Marking a field for removal
"old_field": {
    "type": "string",
    "doc": "DEPRECATED in v2.0 (2024-12-31)"
}
# ✅ SAFE: Notifies consumers, no breaking changes yet
```

### ❌ Unsafe Changes (Always Rejected)

```python
# Removing a field
# OLD: {"name": "user_id", "type": "string"}
# NEW: (removed)
# ❌ UNSAFE: Queries like SELECT user_id fail

# Renaming a field
# OLD: "timestamp" → NEW: "timestamp_utc"
# ❌ UNSAFE: Queries referencing "timestamp" fail

# Adding required field without default
"required_field": {
    "type": "string",
    # No default!
}
# ❌ UNSAFE: Existing INSERT queries fail

# Narrowing a numeric type
# OLD: type=long → NEW: type=int
# ❌ UNSAFE: Large values lose data (>2 billion)
```

### ⚠️ Risky Changes (Allowed with Warnings)

```python
# Adding required field WITH default
"status": {
    "type": "string",
    "default": "ACTIVE"
}
# ⚠️  WARNING: Needs backfill of old rows

# Column reordering
# OLD: [user_id, name, email]
# NEW: [email, name, user_id]
# ⚠️  WARNING: Position-based SELECT breaks
```

## Common Workflows

### Adding a New Field (Safe)

```python
# Schema v1.0
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"}
    ]
}

# Schema v1.1 - add optional field
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "created_at", "type": ["null", "string"], "default": None}
    ]
}

# ✅ Safe: validates with no errors
# Deploy immediately
```

### Removing a Field (Safe with 30-day Plan)

```python
# Schema v1.1 - mark field as deprecated
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "old_field", "type": "string", "doc": "DEPRECATED in v2.0"}
    ]
}
# ✅ Safe: just marking for removal

# After 30 days, schema v2.0
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"}
    ]
}
# ✅ Safe: consumers have migrated
```

### Renaming a Field (Safe with Alias Pattern)

```python
# Schema v1.1 - add new name, keep old
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "user_name", "type": "string"},      # New name
        {"name": "name", "type": "string", "doc": "DEPRECATED: use user_name"}
    ]
}
# ✅ Safe: both names work

# After migration period, v2.0
{
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "user_name", "type": "string"}
    ]
}
# ✅ Safe: consumers updated
```

## Error Messages & Fixes

### Error: "Field removed — SQL queries will fail"

```
❌ ERROR: Field 'user_id' removed — SQL queries referencing this 
         column will fail.
```

**Fix:** Don't remove it directly. Instead:

1. In v1.1: Mark as deprecated
2. In v2.0 (after 30 days): Remove it

### Error: "New required field without defaults"

```
❌ ERROR: New required field 'status' without default — existing 
         SQL INSERT queries will fail.
```

**Fix:** Add a default value:

```python
{"name": "status", "type": "string", "default": "ACTIVE"}
```

### Error: "Type change requires migration"

```
❌ ERROR: Type change long → int is UNSAFE (data loss on large numbers)
```

**Fix:** Only widen types (int→long, float→double). For narrowing, contact data team.

### Warning: "Column order changed"

```
⚠️  WARNING: Column order changed — positional SQL queries will be affected
```

**Action:** Update consumers to use explicit column names:

```python
# ❌ BAD (relies on position)
SELECT * FROM table  # Order matters!

# ✅ GOOD (explicit)
SELECT user_id, name, email FROM table  # Order independent
```

## Testing Your Schema

### Validate a Schema File

```bash
cd /Users/vanik_kars/Documents/learning/python/fast_api/data-contract-flow

# Validate current schema doesn't have issues
python -c "
from lib.schema_validator import validate_schema_contract
import json

with open('your_schema.json') as f:
    schema = json.load(f)

is_valid, issues = validate_schema_contract(schema)
if not is_valid:
    for issue in issues:
        print(f'❌ {issue}')
else:
    print('✅ Schema is valid')
"
```

### Run the Full Test Suite

```bash
python -m pytest airflow/tests/test_schema_validator.py -v
```

### Test a Specific Change

```python
from lib.schema_validator import SchemaSafetyValidator
import json

validator = SchemaSafetyValidator(strict_mode=True)

with open('old_schema.json') as f:
    old = json.load(f)

with open('new_schema.json') as f:
    new = json.load(f)

is_safe, violations = validator.validate_schema_change(old, new)

print(f"Safe: {is_safe}")
for v in violations:
    print(f"[{v['type'].upper()}] {v['message']}")
```

## Architecture

```
Schema Registration Flow:
┌─────────────────┐
│ DataContract    │
└────────┬────────┘
         │ contract_to_avro()
         ▼
    ┌─────────────────────┐
    │ AVRO Schema JSON    │
    └────────┬────────────┘
             │
             ▼
    ┌─────────────────────┐     ◄─ NEW
    │ SQL-Safety Check    │     
    │ - Removed fields    │     
    │ - Type changes      │     
    │ - Required fields   │     
    │ - Reordering        │     
    └────────┬────────────┘     
             │                   
         FAIL │  PASS            
         ──┬──|──────────────────┐
           │                     ▼
           │              ┌──────────────┐
           │              │ Avro Compat  │
           │              │ Check (Glue) │
           │              └──────┬───────┘
           │                     │
           │          FAIL       │ PASS
           │              ┌──────┴──────┐
           │              ▼             ▼
           │          ❌ ERROR      ✅ REGISTERED
           │              │
           └──────────────┘
```

## Next Steps

1. **Read** `SQL_SAFETY_IMPLEMENTATION.md` for detailed integration
2. **Review** `SCHEMA_EVOLUTION_CHECKLIST.md` before making changes
3. **Browse** `airflow/lib/schema_validator.py` for all validation rules
4. **Run** `pytest airflow/tests/test_schema_validator.py` to see it in action

## Common Questions

### Q: Will this break my existing schemas?

**A:** No. Existing schemas registered in Glue are not affected. SQL-safety checks only apply to NEW schema registrations going forward.

### Q: Can I bypass SQL-safety checks?

**A:** Yes, for testing only:

```python
adapter.register_schema(contract, enforce_sql_safety=False)
```

But don't do this in production!

### Q: What if my change is "safe" in my use case?

**A:** Contact the data engineering team. If you have a legitimate need for an exception, we can:

1. Document the exception
2. Update consumers proactively
3. Plan the migration timeline together

### Q: How do I deprecate a field properly?

**A:** Follow these steps:

```python
# v1.1: Mark as deprecated
{"name": "old_field", "type": "string", "doc": "DEPRECATED: use new_field instead. Will be removed in v2.0 (2024-12-31)"}

# Wait 30+ days, notify consumers

# v2.0: Remove the field
# (field removed from schema)
```

### Q: Do I need to update my Iceberg table?

**A:** Usually yes. When you register a new schema version, also run:

```python
await adapter.update_table(table)
```

This keeps the physical table in sync with the logical schema.

## Support

- **Documentation:** See `SQL_SAFETY_IMPLEMENTATION.md`
- **Examples:** Check `airflow/tests/test_schema_validator.py`
- **Issues:** GitHub issue with schema validation errors
- **Questions:** Ask in #data-engineering Slack