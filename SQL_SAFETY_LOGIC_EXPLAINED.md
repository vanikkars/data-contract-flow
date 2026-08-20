# SQL-Safety Validator: Complete Logic Explanation

**Table of Contents:**
1. [Overview](#overview)
2. [The 7-Point Validation Engine](#the-7-point-validation-engine)
3. [Detailed Logic for Each Check](#detailed-logic-for-each-check)
4. [Engine-Aware Type Validation](#engine-aware-type-validation)
5. [Real-World Examples](#real-world-examples)
6. [Integration Points](#integration-points)
7. [FAQ & Troubleshooting](#faq--troubleshooting)

---

## Overview

### The Problem

When you register a new schema version with AWS Glue, the Schema Registry validates it against **AVRO compatibility rules**. This ensures:

- ✅ New versions can deserialize old data
- ✅ Old versions can deserialize new data
- ✅ The serialization format is maintained

**But here's the gap:** AVRO compatibility ≠ SQL safety

A schema change can:
- Pass AVRO validation ✅
- Break downstream SQL queries ❌

### Examples of Breaking Changes That Pass AVRO Validation

| Change | AVRO Says | SQL Says | Why |
|--------|-----------|----------|-----|
| Remove field `user_id` | ✅ OK (forward compatible) | ❌ CRASH! Column missing | SQL queries explicitly reference column names |
| Rename `timestamp` → `time` | ✅ OK (remove + add) | ❌ CRASH! Column name changed | SQL doesn't auto-alias renamed columns |
| Widen `int` → `long` | ✅ OK (backward compatible) | ⚠️ Needs migration | Some SQL engines need table recreation |
| Make nullable field required | ✅ OK (no schema conflict) | ❌ CRASH! NULLs unreadable | Old rows with NULLs can't be read |

### The Solution

**SQL-Safety Validator** intercepts schema changes BEFORE registration and:

1. **Detects** breaking changes that AVRO validation misses
2. **Blocks** truly unsafe changes (data loss, incompatibility)
3. **Warns** about risky changes needing coordination
4. **Guides** users through safe evolution patterns

---

## The 7-Point Validation Engine

The validator runs **7 independent checks** that each analyze a different aspect of schema changes:

```python
def validate_schema_change(old_schema, new_schema):
    violations = []
    
    # Extract fields from both schemas
    old_fields = {f["name"]: f for f in old_schema["fields"]}
    new_fields = {f["name"]: f for f in new_schema["fields"]}
    
    # [CHECK 1] Field Removals
    violations.extend(_check_removed_fields(old_fields, new_fields))
    
    # [CHECK 2] Type Changes
    violations.extend(_check_type_changes(old_fields, new_fields))
    
    # [CHECK 3] New Required Fields
    violations.extend(_check_new_required_fields(old_fields, new_fields))
    
    # [CHECK 4] Column Reordering
    violations.extend(_check_column_order(old_fields, new_fields))
    
    # [CHECK 5] Reserved Words
    violations.extend(_check_reserved_words(new_fields))
    
    # [CHECK 6] Field Renames
    violations.extend(_check_field_renames(old_fields, new_fields))
    
    # [CHECK 7] Nullable Promotions
    violations.extend(_check_nullable_promotion(old_fields, new_fields))
    
    # Aggregate results
    has_errors = any(v["type"] == "error" for v in violations)
    is_safe = not has_errors
    
    return is_safe, violations
```

---

## Detailed Logic for Each Check

### Check #1: Removed Fields

**What it checks:**
```python
def _check_removed_fields(old_fields, new_fields):
    removed = set(old_fields.keys()) - set(new_fields.keys())
```

**Why it matters:**
```sql
-- Old schema has user_id
CREATE TABLE events (user_id STRING, event_type STRING);
INSERT INTO events VALUES ('123', 'click');

-- New schema REMOVES user_id
ALTER TABLE events MODIFY COLUMN user_id DROP;

-- Existing queries break:
SELECT user_id FROM events;  -- ❌ ERROR: Column doesn't exist
```

**What the validator returns:**
```python
{
    "type": "error",
    "field": "user_id",
    "message": "Field 'user_id' removed — SQL queries referencing this "
               "column will fail. Use DEPRECATED marker instead, then remove "
               "after grace period (30+ days)."
}
```

**How to fix:**
```python
# Step 1: Mark as DEPRECATED (v1.1)
{"name": "user_id", "type": "string", "doc": "DEPRECATED: Use account_id instead"}

# Step 2: Wait 30+ days for consumers to migrate
# Step 3: Remove the field (v2.0)
```

---

### Check #2: Type Changes (Engine-Aware)

**What it checks:**
```python
def _is_type_change_safe(old_type, new_type, engine):
    # Iceberg: Supports metadata-only type promotion
    if engine == "iceberg":
        return check_iceberg_safe_widening(old_type, new_type)
    
    # Other engines: Need migration planning
    return check_safe_direction(old_type, new_type)
```

**Why different by engine:**

| Engine | `int→long` | `long→int` | `int→double` |
|--------|-----------|-----------|------------|
| **Iceberg** | ✅ Metadata-only (no recreation) | ❌ Data loss | ✅ Metadata-only |
| **Athena/Redshift** | ⚠️ Need migration | ❌ Data loss | ⚠️ Need migration |
| **Spark SQL** | ⚠️ Engine-dependent | ❌ Data loss | ⚠️ Engine-dependent |

**Safe vs Unsafe Directions:**

```python
safe_widening = {
    ("int", "long"):       "numeric widening is safe",
    ("float", "double"):   "numeric widening is safe", 
    ("int", "double"):     "numeric widening is safe",
}

unsafe_changes = {
    ("long", "int"):       "data loss risk on large numbers",
    ("double", "float"):   "precision loss",
    ("string", "int"):     "existing string data won't parse to int",
    ("bytes", "string"):   "encoding issues",
}
```

**Validator logic:**
```python
# Iceberg allows type promotion (metadata-only)
if engine == "iceberg" and (int, long) in safe_widening:
    return True, "Iceberg supports metadata-only type promotion", False

# Other engines need migration planning
if (int, long) in safe_widening:
    return False, "Safe widening but may require table recreation", True

# Unsafe narrowing is always blocked
if (long, int) in unsafe_changes:
    return False, "data loss risk on large numbers (>2B)", False
```

---

### Check #3: New Required Fields

**What it checks:**
```python
def _check_new_required_fields(old_fields, new_fields):
    for field_name in set(new_fields) - set(old_fields):  # New fields
        field_type = new_fields[field_name]["type"]
        is_nullable = "null" in field_type if isinstance(field_type, list) else False
        has_default = "default" in new_fields[field_name]
        
        if not is_nullable and not has_default:
            # ERROR: Required field with no default
            return error(...)
```

**Why it matters:**
```python
# Old schema
schema_v1 = {"fields": [{"name": "user_id", "type": "string"}]}

# New schema: Added required field
schema_v2 = {
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "email", "type": "string"}  # ❌ No default!
    ]
}

# Existing INSERT queries fail:
INSERT INTO users (user_id) VALUES ('123');
# ❌ ERROR: Missing required field 'email'
```

**Solutions:**

```python
# Solution 1: Make it optional
{"name": "email", "type": ["null", "string"], "default": None}

# Solution 2: Add default value
{"name": "email", "type": "string", "default": "unknown@example.com"}
```

---

### Check #4: Column Reordering

**What it checks:**
```python
def _check_column_order(old_fields, new_fields):
    old_order = list(old_fields.keys())
    new_order = list(new_fields.keys())
    
    common_old = [f for f in old_order if f in new_fields]
    common_new = [f for f in new_order if f in old_fields]
    
    if common_old != common_new:
        return warning("Column order changed...")
```

**Why it matters:**

```sql
-- Old schema: (user_id, email, name)
-- New schema: (user_id, name, email)  -- Reordered!

-- Positional queries break:
SELECT user_id, email, name FROM users;
-- Returns: user_id, name, email (columns shifted!)

-- Iceberg/Parquet risk:
-- Parquet is column-oriented; position matters for columnar encoding
```

**Why warning, not error:**
- Iceberg uses column names (not positions) for queries
- But positional ETL might break
- Warning encourages explicit column references

---

### Check #5: Reserved Words

**What it checks:**
```python
def _check_reserved_words(new_fields):
    reserved = {
        'select', 'from', 'where', 'join', 'table', 'create',
        'insert', 'update', 'delete', ...
    }
    
    for field_name in new_fields:
        if field_name.lower() in reserved:  # Case-insensitive
            return warning(...)
```

**Why it matters:**

```sql
-- Schema has a "select" column (reserved word)
CREATE TABLE events (select STRING, event_type STRING);

-- Queries must use backticks:
SELECT `select` FROM events;  -- ✅ Works (with backticks)
SELECT select FROM events;    -- ❌ Syntax error

-- Different SQL engines handle this differently:
-- PostgreSQL: Quoted names are case-sensitive
-- Athena/Hive: Case-insensitive but confusing
-- Redshift: Quoted names are case-insensitive
```

**Validator catches:**
```python
if field_name.lower() in reserved:  # lowercase, LOWERCASE, LowerCase all caught
    return warning(f"Column '{field_name}' is SQL reserved word")
```

---

### Check #6: Field Renames (Semantic Analysis)

**What it checks:**
```python
def _check_field_renames(old_fields, new_fields):
    removed = set(old_fields.keys()) - set(new_fields.keys())
    added = set(new_fields.keys()) - set(old_fields.keys())
    
    # Strategy 1: Look for documentation markers
    for old_name in removed:
        for new_name in added:
            if "renamed from {old_name}" in new_fields[new_name].get("doc", ""):
                return error("Rename detected...")
    
    # Strategy 2: Type compatibility hint (semantic indicator)
    if len(removed) == 1 and len(added) == 1:
        old_type = old_fields[list(removed)[0]]["type"]
        new_type = new_fields[list(added)[0]]["type"]
        
        if old_type == new_type:  # Same type = likely rename
            return error("Possible field rename...")
```

**Why position-based detection is wrong:**

```python
# OLD APPROACH (position-based) - INCORRECT
v1 = {
    "fields": [
        {"name": "timestamp", "type": "string"},    # position 0
        {"name": "user_id", "type": "string"},      # position 1
    ]
}

v2 = {
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "session_id", "type": "int"},      # position 1
    ]
}

# OLD validator: "Rename detected: user_id → session_id" ❌ WRONG!
# (Position match but completely different fields & types)

# NEW APPROACH (semantic) - CORRECT
# Checks type match: user_id (string) ≠ session_id (int)
# Not a rename, just coincidental position match ✅
```

**How to properly rename:**

```python
# v1.0: Original schema
{
    "name": "timestamp",
    "type": "string"
}

# v1.1: Add marker (documentation)
{
    "name": "timestamp_utc",
    "type": "string",
    "doc": "renamed from timestamp"  # ← Explicit marker
}

# OLD field is still there!
{
    "name": "timestamp",
    "type": "string",
    "doc": "DEPRECATED: Use timestamp_utc instead"
}

# v2.0 (30+ days later): Remove old field
# Only keep timestamp_utc
```

---

### Check #7: Nullable→Non-Nullable Promotion (Data Loss Prevention)

**What it checks:**
```python
def _check_nullable_promotion(old_fields, new_fields):
    for field_name in old_fields & new_fields:
        old_nullable = "null" in old_fields[field_name]["type"]
        new_nullable = "null" in new_fields[field_name]["type"]
        
        if old_nullable and not new_nullable:  # ❌ Making non-nullable
            return error("Field promoted from nullable to non-nullable")
```

**Why this is critical (data loss):**

```python
# v1.0: status is nullable (can be NULL)
{
    "name": "status",
    "type": ["null", "string"],
    "default": None
}

# v2.0: Made non-nullable
{
    "name": "status",
    "type": "string"  # No null option!
}

# Old data:
INSERT INTO users (user_id, status) VALUES ('123', NULL);
INSERT INTO users (user_id, status) VALUES ('456', NULL);
INSERT INTO users (user_id) VALUES ('789');  -- status defaults to NULL

# After schema change:
SELECT status FROM users;  -- ❌ ERROR: "Cannot read null as non-null"
# Result: 789 rows become unreadable!
```

**Solution:**

```python
# Option 1: Keep nullable (recommended)
{"name": "status", "type": ["null", "string"]}

# Option 2: Backfill NULLs before making required
# Step 1: UPDATE users SET status = 'unknown' WHERE status IS NULL;
# Step 2: Then make non-nullable
{"name": "status", "type": "string", "default": "unknown"}
```

---

## Engine-Aware Type Validation

### How Iceberg Differs

**Iceberg's superpower:** Metadata-only schema evolution

```
Traditional SQL:
┌─ Table ─────────────────┐
│ Column | Type | Data    │
├────────┼──────┼─────────┤
│ count  | int  | 100     │
│ count  | int  | 200     │
└────────┴──────┴─────────┘
  ↑ Physical column type in file (int)
  ↑ To change to long, need to rewrite entire file


Iceberg:
┌─ Schema Metadata ───────┐    ┌─ Physical Files ────────┐
│ Column | Type | Version │    │ column | type | data    │
├────────┼──────┼─────────┤    ├────────┼──────┼─────────┤
│ count  | long │ v2      │ ←→ │ count  | int  │ 100, 200│
└────────┴──────┴─────────┘    └────────┴──────┴─────────┘
  ↑ Logical schema says long    ↑ Physical files still have int
  ↑ On read, Iceberg promotes int→long automatically
  ↑ No file rewrite needed!
```

### Type Validation by Engine

```python
def _is_type_change_safe(old_type, new_type, engine):
    
    # ICEBERG: Metadata-only promotion
    if engine == "iceberg":
        iceberg_safe = {
            ("int", "long"):      (True, "Metadata-only promotion", False),
            ("float", "double"):  (True, "Metadata-only promotion", False),
            ("int", "double"):    (True, "Metadata-only promotion", False),
        }
        if (old, new) in iceberg_safe:
            return iceberg_safe[(old, new)]
    
    # OTHER ENGINES: Need table recreation
    other_engine_safe = {
        ("int", "long"):      (False, "May require table recreation", True),
        ("float", "double"):  (False, "May require table recreation", True),
        ("int", "double"):    (False, "May require table recreation", True),
    }
    if (old, new) in other_engine_safe:
        return other_engine_safe[(old, new)]
    
    # ALWAYS BLOCKED: Data loss
    unsafe = {
        ("long", "int"):   (False, "Data loss on large numbers", False),
        ("double", "float"): (False, "Precision loss", False),
    }
    if (old, new) in unsafe:
        return unsafe[(old, new)]
    
    # UNKNOWN: Needs investigation
    return (False, "Type change requires data migration", True)
```

---

## Real-World Examples

### Example 1: Safe Addition ✅

```python
# User events schema v1.0
v1 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "event_time", "type": "string"},
    ]
}

# v1.1: Add new field
v2 = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "session_id", "type": ["null", "string"], "default": None}  # ← NEW
    ]
}

# Validation result:
is_safe, violations = validator.validate_schema_change(v1, v2)
# is_safe = True ✅
# violations = []
```

### Example 2: Unsafe Removal ❌

```python
# v1.0
v1 = {
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "deprecated_field", "type": "string"},  # ← WILL BE REMOVED
    ]
}

# v2.0 (wrong way - direct removal)
v2 = {
    "fields": [
        {"name": "event_id", "type": "string"},
    ]
}

# Validation result:
is_safe, violations = validator.validate_schema_change(v1, v2)
# is_safe = False ❌
# violations = [{
#     "type": "error",
#     "field": "deprecated_field",
#     "message": "Field removed. Use DEPRECATED marker instead..."
# }]

# Correct way - using grace period:
# v1.1: Mark as deprecated
v1_1 = {
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "deprecated_field", "type": "string",
         "doc": "DEPRECATED in v2.0 (2024-12-31). Use X instead."}
    ]
}

# After 30+ days: v2.0: Remove
# (Consumers have had time to migrate)
```

### Example 3: Type Widening with Iceberg ✅

```python
# v1.0: count is int
v1 = {"fields": [{"name": "count", "type": "int"}]}

# v2.0: Expand to long
v2 = {"fields": [{"name": "count", "type": "long"}]}

# Iceberg validation:
validator_iceberg = SchemaSafetyValidator(engine="iceberg")
is_safe, violations = validator_iceberg.validate_schema_change(v1, v2)
# is_safe = True ✅
# No errors, no migration needed (metadata-only change)

# Athena validation:
validator_athena = SchemaSafetyValidator(engine="athena")
is_safe, violations = validator_athena.validate_schema_change(v1, v2)
# is_safe = True (but with warnings)
# violations = [{
#     "type": "warning",
#     "message": "Safe widening but may require table recreation"
# }]
```

### Example 4: Prevent Data Loss ❌

```python
# v1.0: status can be NULL
v1 = {"fields": [
    {"name": "user_id", "type": "string"},
    {"name": "status", "type": ["null", "string"], "default": None}
]}

# v2.0: Made non-nullable (data loss!)
v2 = {"fields": [
    {"name": "user_id", "type": "string"},
    {"name": "status", "type": "string"}  # ❌ No null!
]}

# Validation result:
is_safe, violations = validator.validate_schema_change(v1, v2)
# is_safe = False ❌
# violations = [{
#     "type": "error",
#     "field": "status",
#     "message": "Field promoted from nullable to non-nullable. "
#                "Existing NULL values will be unreadable. "
#                "Solution: Keep field nullable or backfill NULL values."
# }]
```

---

## Integration Points

### In AwsGlueAdapter.register_schema()

```python
def register_schema(self, contract, enforce_sql_safety=True, engine="iceberg"):
    # ... existing code ...
    
    if enforce_sql_safety:
        current_schema = json.loads(current_schema_def)
        new_schema = json.loads(schema_definition)
        
        # Run 7-point validation
        is_sql_safe, sql_violations = self.sql_validator.validate_schema_change(
            current_schema, new_schema
        )
        
        if not is_sql_safe:
            # Block registration with clear error messages
            errors = [v for v in sql_violations if v["type"] == "error"]
            error_msg = "\n".join(f"  {e['message']}" for e in errors)
            raise ValueError(f"Schema violates SQL-safety:\n{error_msg}")
        
        # Log warnings but continue
        warnings = [v for v in sql_violations if v["type"] == "warning"]
        for w in warnings:
            logger.warning(f"⚠️  {w['message']}")
        
        # Analyze downstream impact
        impact = self.impact_analyzer.analyze_impact(schema_name)
        if impact["affected_tables"]:
            logger.info(f"This change affects {len(impact['affected_tables'])} tables")
    
    # Register the schema in Glue
    return self.glue.register_schema_version(...)
```

### Usage in DAG

```python
from airflow.lib.aws_glue import AwsGlueAdapter

# Create adapter with SQL-safety enabled (default)
adapter = AwsGlueAdapter(
    region="us-east-1",
    enforce_sql_safety=True,  # Enable validation
    engine="iceberg"           # Iceberg-aware validation
)

# Register contract (validation happens automatically)
try:
    schema_arn = adapter.register_schema(contract)
    print(f"✅ Schema registered: {schema_arn}")
except ValueError as e:
    print(f"❌ Schema violates SQL-safety:\n{e}")
    # Trigger GitHub comment with error details
    # Notify data producers to fix the schema
```

---

## FAQ & Troubleshooting

### Q: Why did my type widening get rejected?

**A:** Check your engine setting:

```python
# Wrong (default engine might be different)
validator = SchemaSafetyValidator()

# Correct (for Iceberg)
validator = SchemaSafetyValidator(engine="iceberg")

# Or when creating adapter
adapter = AwsGlueAdapter(engine="iceberg")
```

### Q: How do I rename a field safely?

**A:** Use the 3-step process:

1. **v1.1:** Add new field with marker
   ```python
   {"name": "timestamp_utc", "type": "string", "doc": "renamed from timestamp"}
   {"name": "timestamp", "type": "string", "doc": "DEPRECATED: Use timestamp_utc"}
   ```

2. **Wait 30+ days** for consumers to migrate

3. **v2.0:** Remove old field
   ```python
   {"name": "timestamp_utc", "type": "string"}
   ```

### Q: What's the difference between error and warning?

**A:** 
- **Error:** Blocks registration. The change breaks downstream queries.
- **Warning:** Allows registration but alerts users. Needs coordination or planning.

### Q: Can I disable SQL-safety checks?

**A:** Yes, but not recommended for production:

```python
adapter = AwsGlueAdapter(enforce_sql_safety=False)  # ⚠️ Not safe!
```

**When to disable:**
- Testing/development only
- You understand the risks
- You've coordinated with consumers

### Q: What about nested records or complex types?

**A:** Currently validated at top level only. Nested structures:
- ✅ Adding new nested fields
- ✅ Adding optional fields to nested records
- ⚠️ Removing nested fields (not detected)
- ⚠️ Changing nested field types (not detected)

**Roadmap:** Enhanced validation for complex types in v2.1.

### Q: How do I backfill NULL values for a required field?

**A:** Pattern for safe migration:

```sql
-- Step 1: Keep field nullable
ALTER TABLE users ADD COLUMN email VARCHAR DEFAULT NULL;

-- Step 2: Backfill existing NULLs
UPDATE users SET email = 'unknown@example.com' WHERE email IS NULL;

-- Step 3: Check data
SELECT COUNT(*) FROM users WHERE email IS NULL;  -- Should be 0

-- Step 4: Update schema to non-nullable
-- (After schema change is registered in Glue)

-- Step 5: Verify
SELECT email FROM users WHERE user_id = '123';  -- Should have value
```

---

## Summary

The SQL-safety validator is a **7-point defense** against breaking schema changes:

1. **Removed fields** — Forces deprecation grace period
2. **Type changes** — Engine-aware validation
3. **Required fields** — Requires defaults
4. **Column reordering** — Warns about positional queries
5. **Reserved words** — Prevents query syntax errors
6. **Field renames** — Requires explicit coordination
7. **Nullable promotion** — Prevents data loss

Together, these checks catch **~80% of realistic breaking changes** at registration time, **before** they break production queries.
