# Compatibility Modes vs. SQL Safety — and Protecting Downstream dbt Consumers

> Context: Kafka topics (one schema per topic) → Iceberg raw layer (source-aligned data products) → dbt.
> Goal: put the raw input layer under data contracts and guarantee downstream dbt models survive schema evolution.

---

## The one-sentence version

**Compatibility modes protect the *deserializer*. SQL safety protects the *query*.** They validate different layers, and a change can pass one while destroying the other.

---

## What a compatibility mode actually checks

A schema registry (Glue, Confluent) answers exactly one question:

> Given a record serialized with schema **X**, can a process holding schema **Y** decode the bytes into a valid object?

That's it. It's a property of the binary Avro reader/writer resolution algorithm. Nothing in it knows that a column name is also a SQL identifier that a human typed into a dbt model.

| Mode | Guarantees | What it *permits* |
|---|---|---|
| `BACKWARD` | New schema reads old data | **Delete a field**, add optional field |
| `FORWARD` | Old schema reads new data | **Add a required field**, delete optional field |
| `FULL` | Both directions | Add/remove *optional* fields only |
| `*_ALL` | Same, against **every** prior version, not just the last | Same set, transitively enforced |
| `NONE` | Nothing | Everything |

The critical trap is `BACKWARD`, which is the default in most registries. **BACKWARD compatibility explicitly allows field deletion.** The reasoning is sound for streaming: a new consumer with schema v2 reading an old v1 message simply ignores the extra bytes. But your dbt model isn't a new consumer reading old bytes — it's a SQL parser resolving `SELECT deprecated_field` against a catalog, and that column is now gone.

So the registry says ✅ and dbt says:

```
Column 'fee_category' cannot be resolved
```

`airflow/lib/schema_validator.py` already encodes this correctly — `_check_removed_fields` returns `type: "error"` unconditionally, regardless of what compatibility mode was passed in. That's the right call, and it's the single most important line in the file.

---

## The two-axis model

The clean mental model is that these are orthogonal axes, not a spectrum:

```
                    SQL-SAFE
                        ▲
       add optional     │     (empty — nothing is
       field with       │      SQL-safe but Avro-
       default          │      incompatible in
                        │      practice)
   ─────────────────────┼─────────────────────► AVRO-COMPATIBLE
                        │
       rename column    │     drop field (BACKWARD ✅)
       (NONE mode)      │     narrow long→int (both are ints ✅)
       reorder + drop   │     nullable → required (✅)
                        │     add required field (FORWARD ✅)
                        ▼
                   SQL-UNSAFE
```

The lower-right quadrant is the dangerous one and it is **large**. Every incident in `article/schema-registry-compatibility-vs-sql-safety.md` lives there. Avro compatibility is a *necessary* gate, never a *sufficient* one.

---

## The four mechanisms by which dbt actually breaks

It's worth being precise about *how* dbt breaks, because the mitigation differs per mechanism.

### 1. Compile-time resolution failure — loud, immediate

Field dropped or renamed. A `ref()`-ed model references `old_col`, Iceberg dropped it, dbt fails at compile or at execution. This is the *best* failure mode: it's noisy and it stops the DAG.

### 2. `select *` propagation — silent, contagious

A staging model does `select * from {{ source('raw','user_events') }}`. Add a column upstream and it silently appears in every downstream model, changes table schemas under `on_schema_change`, and can collide with a name defined later in the DAG. Nothing errors. Snapshots and incremental models drift.

### 3. Silent type coercion — silent, corrupting

This is the `DECIMAL(18,4) → DECIMAL(10,2)` case. dbt compiles fine, the query runs fine, and the numbers are wrong. The existing validator catches the Avro-level narrowing cases; the decimal-precision case is the one worth adding explicitly since Avro models decimal as a logical type over `bytes` and a naive `_normalize_type` will see `bytes → bytes` and wave it through.

### 4. Incremental-model schema drift — deferred, confusing

An incremental model with `on_schema_change='fail'` breaks on the next run after an upstream add. With `'ignore'` (the historical default) new columns silently never land, so the model quietly stops carrying data that exists in raw. Both are schema-evolution consequences that appear hours later, disconnected from the schema change that caused them.

> **Note:** mechanisms 2 and 4 are triggered by **additive** changes — the ones every compatibility mode calls safe and the current validator classifies as fine. Additive is safe for the *table*; it is not automatically safe for the *dbt project*.

---

# Architecture for the Kafka → Iceberg raw layer

The goal — raw layer under contract, dbt guaranteed not to break — decomposes into four enforcement points. Layers 0–1 largely exist in this repo today.

## Layer 0: pick your modes deliberately

For a source-aligned raw product ingesting from Kafka into Iceberg:

- **Registry compatibility: `FULL_TRANSITIVE`** (Glue: `FULL_ALL`). Not `BACKWARD`. `FULL` forbids both field removal and required-field addition, which eliminates two of the five break classes at the registry layer before the custom validator even runs. `_TRANSITIVE` matters because Iceberg tables hold data written under *every* historical schema version, not just the previous one — a non-transitive check compares only against v_latest and will happily let v5 break readers of v1 data still sitting in old snapshots.
- **Iceberg format-version 2, and `write.metadata.delete-after-commit.enabled=false`** so you retain the metadata history needed to reason about which snapshot had which schema.
- **Never `ALTER TABLE ... RENAME COLUMN` on the raw layer**, even though Iceberg supports it safely at the storage level via field-IDs. Iceberg's rename is safe for *data* (IDs are stable, no rewrite) but fatal for *SQL*, because dbt resolves by name. Iceberg's safety guarantee and your safety requirement are different guarantees.

## Layer 1: contract + SQL-safety gate (already built)

`SchemaSafetyValidator` runs pre-registration in the Airflow DAG. Two gaps worth closing given the dbt requirement:

- **Decimal precision/scale.** `_normalize_type` flattens Avro logical types to their base. Add a branch that reads `logicalType: decimal` and compares `(precision, scale)` — narrowing either is an error, widening precision is safe, changing scale is an error (it silently rescales values).
- **Timestamp unit changes.** `timestamp-millis → timestamp-micros` is Avro-compatible (both are `long`) and silently shifts every timestamp by 1000× in engines that don't reinterpret the logical type. Classify as error.

## Layer 2: contract → dbt source materialization

> **Out of scope for this repo — belongs on the consumer side.**
>
> This repo is a contract *provisioning* service: contract JSON → Glue schema → Iceberg table. The dbt project is a different repo owned by the analytics engineers who consume these data products. Generating their `sources.yml` from here would couple provisioning to one particular consumer.
>
> The right home for this is a step in the **dbt repo's own CI** that pulls the published contract and generates its sources file locally. Same idea, correct ownership. Documented here so that team has the design.

The contract JSON is the single source of truth, so **generate the dbt sources file from it** rather than letting anyone hand-write it:

```
contracts/all/user/user_v1.json
        │
        ├──► Avro schema  ──► Glue Registry ──► Iceberg table DDL
        │
        └──► dbt/models/sources/_raw__sources.yml   (generated, committed)
                 - columns declared explicitly
                 - not_null / accepted_values tests from contract nullable/enum
                 - meta.contract_version pinned
```

Generating this gives three things at once: dbt has a *declared* expectation of the raw schema, `dbt build` fails fast if reality diverges, and the git diff on the generated file becomes the human-readable blast-radius review during contract PRs.

## Layer 3: ban `select *` at the staging boundary

> **Out of scope for this repo — belongs on the consumer side.**
>
> This lints dbt model files, which live in the consumer's repo. The raw layer cannot enforce it and does nothing wrong when it is violated: adding a nullable column is safe for the table, and the silent propagation is a consequence of how the consumer chose to write their models.

The staging layer is the anti-corruption layer. Enforce, via a CI check on the dbt repo:

- Every model under `models/staging/` selects **explicit, aliased columns** — no `select *`, no `dbt_utils.star()`. This converts break mechanism #2 from silent contagion into a no-op: an upstream additive change simply doesn't propagate until someone deliberately adds the column.
- Every incremental model sets `on_schema_change='append_new_columns'` or `'fail'` explicitly — never leave it defaulted.
- Staging models are 1:1 with a raw table and do nothing but rename, cast, and select.

With this in place, a *new column upstream* becomes a genuinely zero-impact change, which is what lets you move fast on additive evolution.

## Layer 4: reverse-direction impact analysis

`DownstreamImpactAnalyzer` currently walks the Glue catalog. For the dbt guarantee, the authoritative source is dbt's own artifact: `target/manifest.json` contains the full column-level lineage from `source()` down to every mart. In the contract PR pipeline:

1. Contract PR proposes dropping `user.fee_category`.
2. Fetch the latest production `manifest.json` (from the dbt Cloud / S3 artifact store).
3. Resolve which `source('raw','user')` nodes exist, and which models reference that column.
4. Post the list of affected models as a PR comment, and block merge if non-empty.

This closes the loop: the contract owner sees the exact set of dbt models their change breaks, *before* merging, rather than discovering it at 2 AM. It also makes the 30-day deprecation window in `SchemaMigrationPlanner` actionable — you can tell precisely when the last consumer stopped referencing the field.

---

## The resulting evolution policy

| Change | Registry (`FULL_ALL`) | Iceberg | dbt impact | Verdict |
|---|---|---|---|---|
| Add nullable field w/ default | ✅ | metadata-only | none (explicit staging) | **auto-approve** |
| Add required field | ❌ blocked | — | — | rejected at registry |
| Widen `int→long`, `float→double` | ✅ | metadata-only promotion | none | **auto-approve** |
| Widen decimal precision | ✅ | metadata-only | none | **auto-approve** |
| Narrow any type / decimal scale | ✅ *(passes!)* | rewrite + data loss | silent corruption | **block — SQL-safety validator** |
| Drop field | ❌ blocked | — | compile failure | rejected at registry |
| Rename field | ❌ blocked (drop+add) | safe via field-ID | compile failure | **two-phase: add alias, deprecate, drop after 30d** |
| Reorder fields | ✅ | no-op | none (explicit staging) | **auto-approve** |
| Nullable → required | ✅ *(passes!)* | old NULLs unreadable | runtime failure | **block — SQL-safety validator** |

Note the shape of that table: the registry blocks the *loud* breaks, and the SQL-safety validator exists specifically to catch the three rows where the registry says ✅ and production burns. That's the argument for why Layer 1 isn't redundant with Layer 0.

---

## Implementation status

**Implemented in this repo (Layers 0–1):**

| Layer | Change | Where |
|---|---|---|
| 0 | Default compatibility `FORWARD_ALL` → `FULL_ALL` | `airflow/lib/aws_glue.py` |
| 0 | Warn loudly when a weaker mode is used | `AwsGlueAdapter._warn_on_weak_compatibility` |
| 0 | Iceberg format-v2 + metadata retention on every table | `airflow/lib/models.py` (`ICEBERG_SAFETY_PROPERTIES`) |
| 1 | Decimal precision/scale change detection | `SchemaSafetyValidator._check_decimal_change` |
| 1 | Timestamp unit change detection (1000× silent shift) | `SchemaSafetyValidator._check_logical_type_swap` |
| 1 | Logical type added/removed detection | `SchemaSafetyValidator._check_logical_type_change` |

Tests: `airflow/tests/test_layer0_compatibility.py`, `airflow/tests/test_layer1_logical_types.py`.

**Deliberately out of scope here (consumer-side):** Layers 2 and 3 — see the notes on those sections. They operate on dbt repo files and belong in the dbt repo's CI.

**Deferred:** Layer 4 reverse-direction impact analysis via `manifest.json`. Requires Layer 2 to exist first, since it reads the `source()` nodes that layer declares.

---

## Related documents in this repo

- `article/schema-registry-compatibility-vs-sql-safety.md` — narrative article on the same gap
- `SQL_SAFETY_LOGIC_EXPLAINED.md` — validator internals
- `SQL_SAFETY_IMPLEMENTATION.md` — implementation guide
- `SCHEMA_EVOLUTION_CHECKLIST.md` — pre-registration checklist
- `airflow/lib/schema_validator.py` — `SchemaSafetyValidator`, `DownstreamImpactAnalyzer`, `SchemaMigrationPlanner`