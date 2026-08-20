# Schema Evolution Checklist

Use this checklist before and after registering any schema change.

## For Schema Producers (Data Engineers)

### Before Registering a Schema Change

- [ ] **Schema is valid**
  - [ ] All fields have unique names
  - [ ] All fields have a `type` defined
  - [ ] Schema follows AVRO format
  - [ ] Run: `validate_schema_contract(schema)` to verify

- [ ] **SQL-safety validated**
  - [ ] No fields removed (without 30-day deprecation period)
  - [ ] No fields renamed (without migration path)
  - [ ] No narrowing type changes (int→short, double→float)
  - [ ] No column reordering affecting position-based code
  - [ ] No new required fields without defaults
  - [ ] No SQL reserved words in column names (or properly quoted)
  - [ ] Run: `validator.validate_schema_change(old, new)` to verify

- [ ] **Impact analysis complete**
  - [ ] Identified all downstream tables and views
  - [ ] Checked if any use position-based column references
  - [ ] Verified affected systems can handle the change
  - [ ] Run: `analyzer.analyze_impact(schema_name)` to find affected tables

- [ ] **Breaking changes documented**
  - [ ] If removal needed: Created 30+ day deprecation plan
  - [ ] If rename needed: Created migration plan with grace period
  - [ ] If type change: Documented backfill strategy
  - [ ] Notified downstream teams (GitHub issue, Slack, email)

- [ ] **Version incremented correctly**
  - [ ] Backward-compatible additions: bump patch (1.0.0 → 1.0.1)
  - [ ] New optional fields: bump minor (1.0.0 → 1.1.0)
  - [ ] Removals/breaking changes: bump major (1.0.0 → 2.0.0)

- [ ] **Documentation updated**
  - [ ] Added deprecation notices to removed fields
  - [ ] Updated field descriptions
  - [ ] Added migration notes for complex changes
  - [ ] Updated data contract YAML/JSON files

- [ ] **Tested in dev/staging**
  - [ ] Schema registers without errors
  - [ ] Iceberg table updates successfully
  - [ ] Existing queries still work
  - [ ] New data can be written in new schema format
  - [ ] Ran full test suite: `pytest airflow/tests/test_schema_validator.py`

### After Registering a Schema Change

- [ ] **Registration succeeded**
  - [ ] Check Glue console for schema version
  - [ ] Verify version status is AVAILABLE (not FAILURE)
  - [ ] Confirm Iceberg table was updated
  - [ ] Check CloudWatch logs for any warnings

- [ ] **Consumers notified**
  - [ ] GitHub PR with schema change approved
  - [ ] Slack message posted to #data-engineering
  - [ ] Affected teams acknowledged the notification
  - [ ] Deprecation notices attached to GitHub issue/PR

- [ ] **Monitoring enabled**
  - [ ] CloudWatch alarms set for query failures
  - [ ] Schema change logged to audit trail
  - [ ] Grafana dashboard updated with new field info
  - [ ] Created GitHub issue for deprecation removal date

- [ ] **Rollback plan ready**
  - [ ] If issues emerge, can rollback to previous version
  - [ ] Old Iceberg table snapshot available
  - [ ] Documented rollback procedure in PR/issue

---

## For Schema Consumers (Analysts, Application Teams)

### When You're Notified of a Schema Change

- [ ] **Understand the change**
  - [ ] Read the GitHub issue/PR description
  - [ ] Review breaking changes section
  - [ ] Check which fields are affected
  - [ ] Understand deprecation timelines

- [ ] **Assess impact on your systems**
  - [ ] List all queries/dashboards using this schema
  - [ ] Check if any use removed/renamed fields
  - [ ] Verify type changes don't break casting/operations
  - [ ] Test positional column references if affected

- [ ] **Update your code (if needed)**
  - [ ] Use explicit column names (not SELECT *)
  - [ ] Update field references if renamed
  - [ ] Add null checks for new optional fields
  - [ ] Update type casting if types changed
  - [ ] Test changes in dev first, then prod

- [ ] **Verify compatibility**
  - [ ] Run your queries against new schema
  - [ ] Check that results haven't changed unexpectedly
  - [ ] Verify performance isn't degraded
  - [ ] Review any new NULL values in output

- [ ] **Acknowledge completion**
  - [ ] Comment on GitHub PR: "✅ Updated [system name]"
  - [ ] Update ticket/issue to mark your work done
  - [ ] Document any issues encountered

### For Deprecated Fields (Before Removal Date)

- [ ] **Identify usage**
  - [ ] Search codebase for deprecated field name
  - [ ] Check Athena query history in CloudTrail
  - [ ] Ask other teams if they use this field
  - [ ] Review BI dashboards and reports

- [ ] **Plan migration**
  - [ ] Update references to use new field name
  - [ ] Adjust calculations if field type changed
  - [ ] Test migrated queries
  - [ ] Schedule migration before removal date

- [ ] **Complete before deadline**
  - [ ] Remove deprecated field references
  - [ ] Verify no queries fail
  - [ ] Deploy to production
  - [ ] Comment on deprecation issue when done

---

## Schema Change Type Checklist

### ✅ Adding an Optional Field (Safest)

```
Fields added: new_field (nullable, with default)
SQL-safety: ✅ Safe
Compatibility: Backward & forward compatible
Consumer action: None required (optional to use)

Checklist:
- [ ] Field is nullable (type includes "null")
- [ ] Field has a default value
- [ ] Field added at end of field list (not reordered)
- [ ] Documentation describes new field
- [ ] No downstream changes needed
```

### ⚠️ Adding a Required Field (Risky)

```
Fields added: required_field (non-nullable, no default)
SQL-safety: ⚠️ Risky (needs backfill)
Compatibility: Breaks backward compat (requires FORWARD mode)
Consumer action: Backfill existing rows with default

Checklist:
- [ ] Have backfill plan (SQL query to populate old rows)
- [ ] Backfill job scheduled before data writes
- [ ] Verified backfill doesn't break existing queries
- [ ] Documented the default value used for backfill
- [ ] Verified SQL-safety check passed with warning
```

### ❌ Removing a Field (Unsafe)

```
Fields removed: old_field
SQL-safety: ❌ Unsafe (breaks queries)
Compatibility: Not compatible with any mode
Consumer action: Migrate queries to not use field

Checklist:
- [ ] Field marked DEPRECATED first (v1.1)
- [ ] 30+ day grace period announced
- [ ] All consumers acknowledged and migrated
- [ ] Only then remove field in v2.0
- [ ] Removal date documented and tracked
```

### ❌ Renaming a Field (Unsafe)

```
Fields renamed: old_name → new_name
SQL-safety: ❌ Unsafe (breaks queries)
Compatibility: Not compatible without grace period
Consumer action: Update all query references

Checklist:
- [ ] Plan: Add new_name first, keep old_name
- [ ] Phase 1: Deprecate old_name (v1.1)
- [ ] Phase 2: Wait 30+ days for migration
- [ ] Phase 3: Remove old_name (v2.0)
- [ ] Consumer queries updated to use new_name
```

### ⚠️ Changing Field Type (Risky)

```
Fields modified: field_type (int → long, string → int, etc.)
SQL-safety: ⚠️ Depends on direction (widening vs narrowing)
Compatibility: Depends on mode and direction
Consumer action: Verify type change won't break existing queries

Checklist:
- [ ] Verified change is type-safe direction (widening, not narrowing)
- [ ] If narrowing: Have explicit backfill/migration plan
- [ ] Tested type change in dev environment
- [ ] Verified no type-cast operations will fail
- [ ] Noted that table recreation may be needed (Iceberg)
- [ ] Consumers aware of type change
```

### 🔄 Reordering Columns (Warning)

```
Columns reordered: position[0] != position[1]
SQL-safety: ⚠️ Warning (affects position-based code)
Compatibility: OK for Avro, risky for SQL
Consumer action: Use explicit column names

Checklist:
- [ ] Identified all position-based queries (SELECT col1, col2...)
- [ ] Updated queries to use explicit names (SELECT col1, col2 FROM...)
- [ ] Tested that output order doesn't matter
- [ ] Verified no code relies on column position
```

---

## Emergency Checklist (Schema Broke Downstream)

### Immediate Actions (Minutes 0-5)

- [ ] **Identify the issue**
  - [ ] Which schema version is causing problems?
  - [ ] Which queries are failing?
  - [ ] What's the error message?

- [ ] **Notify stakeholders**
  - [ ] Slack #data-engineering immediately
  - [ ] Ping data owners and affected consumers
  - [ ] Create incident ticket

- [ ] **Assess rollback feasibility**
  - [ ] Can we revert to previous schema version?
  - [ ] How many new records written in bad schema?
  - [ ] Will rollback lose recent data?

### Short-term Actions (Minutes 5-30)

- [ ] **Implement quick fix** (if possible)
  - [ ] Deploy hotfix to bad queries
  - [ ] Add missing field if newly required
  - [ ] Update table to fix schema mismatch

- [ ] **Or execute rollback**
  - [ ] Revert schema to previous version
  - [ ] Update Iceberg table to previous version
  - [ ] Notify consumers: "Using v1.0, issue fixed"

- [ ] **Verify recovery**
  - [ ] Test affected queries again
  - [ ] Confirm dashboards/reports working
  - [ ] Check for data integrity issues

### Post-incident Actions (Hours/Days)

- [ ] **Root cause analysis**
  - [ ] Why didn't SQL-safety validation catch this?
  - [ ] Was SQL-safety check bypassed?
  - [ ] Missing test case?

- [ ] **Update safeguards**
  - [ ] Add test case to prevent recurrence
  - [ ] Update SQL-safety validator if needed
  - [ ] Document the failure scenario

- [ ] **Communication**
  - [ ] Postmortem meeting with team
  - [ ] Document what happened
  - [ ] Commit improved tests to repository
  - [ ] Share learnings with other teams

---

## Monthly Maintenance

### First Monday of Each Month

- [ ] **Review deprecated fields**
  - [ ] List all DEPRECATED fields in registry
  - [ ] Check removal dates
  - [ ] Notify teams if removal date is this month
  - [ ] Send reminder 2 weeks before removal

- [ ] **Review schema changes**
  - [ ] Check CloudWatch metrics for violations
  - [ ] Review failed schema registration attempts
  - [ ] Identify patterns in breaking changes

- [ ] **Update documentation**
  - [ ] Review schema YAML files for accuracy
  - [ ] Update field descriptions if needed
  - [ ] Check for orphaned deprecated fields

- [ ] **Health check**
  - [ ] Verify Glue registry connectivity
  - [ ] Check Iceberg table snapshots
  - [ ] Audit schema change logs
  - [ ] Review consumer feedback

---

## Quick Reference

### Schema Version Bump Rules

| Change | Patch | Minor | Major |
|--------|-------|-------|-------|
| Add optional field | | ✅ | |
| Add required field with default | | ✅ | |
| Add computed field | | ✅ | |
| Deprecate field | | ✅ | |
| Remove deprecated field | | | ✅ |
| Rename field | | | ✅ |
| Change field type (same direction) | | ✅ | |
| Change field type (opposite direction) | | | ✅ |

### SQL-Safety Quick Checks

| Change | Safe? | Action |
|--------|-------|--------|
| Add nullable field | ✅ | Register immediately |
| Add required field with default | ⚠️ | Backfill old rows |
| Remove field | ❌ | Deprecate first, then remove |
| Rename field | ❌ | Create alias, migrate consumers |
| Change type | ⚠️ | Verify safe direction, test |
| Reorder columns | ⚠️ | Update position-based code |

### Deprecation Timeline

```
Day 0-1: Announce deprecation
  └─ Increment version (patch)
  └─ Mark field as DEPRECATED in doc
  └─ Notify consumers (GitHub, Slack)

Day 1-30: Grace period (at least 30 days)
  └─ Consumers migrate their code
  └─ Update dashboards, reports, queries
  └─ Remove deprecated field references

Day 31+: Safe to remove
  └─ Increment version (minor)
  └─ Remove field from schema
  └─ Update Iceberg table
  └─ Verify no queries fail
```
