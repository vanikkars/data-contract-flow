# Building a Data Contract Management System with Apache Airflow and AWS Glue

Data contracts have become essential for maintaining trust between data producers and consumers. Yet most organizations still struggle to incorporate them into existing systems—balancing multiple data sources, complex validation requirements, and the overhead of custom orchestration logic.

This article walks through a pragmatic approach: building a contract-driven data pipeline using Apache Airflow DAGs, AWS Glue Schema Registry, and Iceberg tables. Rather than writing custom orchestration code from scratch, we'll compose production-grade solutions and let mature tools handle the complexity we'd otherwise reinvent.

## The Problem: Data Contracts Without Orchestration

Data contracts are agreements that establish trust: a producer promises data will arrive in a specific shape, with defined columns, types, and quality standards. A consumer depends on that contract being enforced.

But implementing contracts in a real system is harder than the concept suggests:

- **Validation at scale**: When a contract changes, you need to validate it against existing schemas, check for breaking changes, and verify compatibility—tasks that deserve automation, not manual review.
- **Multiple sources**: Organizations typically have contracts spread across domains—users, products, transactions, events. Orchestrating validation and provisioning across all of them requires centralization.
- **Enforcement without friction**: Teams need confidence that contracts are enforced *before* data lands in production tables. Adding manual gates defeats the purpose; automating them requires orchestration.
- **Auditability**: Who changed what, when, and whether it passed validation? Without a structured audit trail, you lose visibility into the data pipeline.

Microservices-based approaches exist, but they introduce deployment overhead, service-to-service communication, and operational complexity. A better path: leverage Airflow's native ability to orchestrate validation, registration, and provisioning in a single DAG.

## Architecture: A DAG-Driven Approach with Per-Contract Isolation

The core design uses Airflow's dynamic task mapping to orchestrate the complete contract lifecycle with true per-contract isolation:

```
contract_provisioning (DAG)
├── fetch_contracts (discover contract files)
├── validate_contract[1..N] (per-contract, parallel)
├── schema_provisioning.register_schema[1..N] (per-contract, sequential after validation)
├── table_creation.create_table[1..N] (per-contract, after schema registration)
├── aggregate_results (collect outcomes from all contracts)
└── report_to_github (comment on PR with status)
```

Each stage processes contracts independently:

1. **Fetch Contracts**: Scan `contracts/current/` directory for JSON contract definitions. Returns list for dynamic expansion.
2. **Validate Contracts** [dynamic]: One task per contract. Verify JSON structure, required fields (contract_id, name, columns), and data types.
3. **Register Schemas** [dynamic, sequential]: One per-contract schema registration in AWS Glue Schema Registry. Only runs after validation succeeds for that contract.
4. **Create Iceberg Tables** [dynamic, sequential]: One per-contract table creation in AWS Glue Catalog. Only runs after schema registration succeeds for that contract.
5. **Aggregate Results**: Collect validation and provisioning results from all contract task instances into a structured report.
6. **Report to GitHub**: Comment on pull requests with provisioning status and any errors.

**Why this architecture?**

- **True per-contract isolation**: Each contract gets its own task instances throughout the pipeline. Failure in one contract doesn't block others.
- **Centralized orchestration**: Airflow is the single source of truth for contract processing.
- **Sequential per-contract provisioning**: Schema registration must succeed before table creation for each contract—guarantees consistency.
- **Parallel across contracts**: With 100 contracts, you get 100 validate tasks, 100 register tasks, and 100 create tasks running concurrently (within Airflow executor limits).
- **Error handling built-in**: Failed tasks trigger retries, alerts, and rollback paths automatically.
- **Native visibility**: Airflow UI shows each contract's individual status through the entire pipeline.
- **No service boundaries**: No inter-service communication, no distributed tracing headaches, no deployment choreography.

## Implementation: The Contract Provisioning DAG with Per-Contract Isolation

The production DAG uses Airflow's dynamic task mapping for true per-contract isolation. Here's a simplified version capturing the pattern:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'contract_provisioning',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2026, 1, 1),
)

def task_fetch_contracts(**context):
    """Fetch contract file paths for dynamic expansion"""
    repo_path = Variable.get("repo_path", "/app")
    contracts_dir = Variable.get("contracts_dir", "contracts/current")
    
    contract_files = []
    for contract_file in (Path(repo_path) / contracts_dir).rglob("*.json"):
        contract_files.append(str(contract_file))
        logger.info(f"📋 Found contract: {contract_file}")
    
    if not contract_files:
        logger.warning("⚠️  No contract files found")
        return []
    
    logger.info(f"Found {len(contract_files)} contract files")
    return contract_files


def task_validate_contract(contract_path: str, **context):
    """Validate a single contract file.
    
    This task runs once per contract (via dynamic expansion).
    Failure in one contract doesn't block others.
    """
    from tasks.contract_tasks import ContractTasks
    
    try:
        result = ContractTasks.validate_contract(contract_path)
        result["file_path"] = contract_path
        result["status"] = "valid"
        logger.info(f"✅ Validated {result.get('contract_id')}")
        return result
    except ValueError as e:
        logger.error(f"❌ Validation failed for {contract_path}: {str(e)}")
        raise


def task_register_schema(validation_result: dict, **context):
    """Register validated contract as AVRO schema in AWS Glue.
    
    This runs once per contract after validation succeeds.
    Failure in one contract doesn't affect others.
    """
    from tasks.contract_tasks import ContractTasks
    
    contract_path = validation_result["file_path"]
    contract_id = validation_result.get("contract_id", "unknown")
    
    try:
        schema_result = ContractTasks.register_schema(contract_path)
        logger.info(f"✅ Registered schema for {contract_id}")
        return schema_result
    except Exception as e:
        logger.error(f"❌ Schema registration failed for {contract_id}: {str(e)}")
        raise


def task_create_table(validation_result: dict, **context):
    """Create Iceberg table from validated contract schema.
    
    This runs only after schema registration succeeds for that contract.
    Sequential per-contract: schema → table (guarantees schema exists).
    """
    from tasks.contract_tasks import ContractTasks
    
    contract_path = validation_result["file_path"]
    contract_id = validation_result.get("contract_id", "unknown")
    
    try:
        table_result = ContractTasks.create_iceberg_table(contract_path)
        logger.info(f"✅ Created table for {contract_id}")
        return table_result
    except Exception as e:
        logger.error(f"❌ Table creation failed for {contract_id}: {str(e)}")
        raise


def task_collect_results(validation_results: list, schema_results: list, 
                         table_results: list, **context):
    """Aggregate results from all dynamic task instances.
    
    Handles partial failures gracefully. If one contract failed,
    others still have their results captured.
    """
    from tasks.contract_tasks import ContractTasks
    from tasks.github_tasks import GitHubTasks
    
    ti = context["task_instance"]
    
    logger.info("📊 Collecting results from all contracts")
    
    # Collect via library
    aggregated = ContractTasks.collect_results(validation_results, schema_results, table_results)
    
    # Format for GitHub
    github_payload = GitHubTasks.get_pr_comment_body(
        aggregated,
        github_pr_number=Variable.get("github_pr_number", None)
    )
    
    ti.xcom_push(key="aggregated_results", value=aggregated)
    ti.xcom_push(key="github_payload", value=github_payload)
    
    logger.info(f"✅ Results collected: {aggregated.get('schemas_registered')} schemas, "
                f"{aggregated.get('tables_created')} tables")
    return {"aggregated": aggregated}


# ============================================================================
# DAG Structure with Dynamic Task Mapping
# ============================================================================

# Step 1: Fetch contract files
fetch_task = PythonOperator(
    task_id='fetch_contracts',
    python_callable=task_fetch_contracts,
)

# Step 2: Dynamic validation - one task per contract
validate_tasks = PythonOperator.partial(
    task_id='validate_contract',
    python_callable=task_validate_contract,
).expand(
    op_args=fetch_task.output.map(lambda x: [x])
)

# Step 3: Schema registration in TaskGroup
# One task per contract, runs after validation
with TaskGroup('schema_provisioning') as schema_group:
    register_tasks = PythonOperator.partial(
        task_id='register_schema',
        python_callable=task_register_schema,
    ).expand(
        op_args=validate_tasks.output.map(lambda x: [x])
    )

# Step 4: Table creation in TaskGroup  
# One task per contract, runs after schema registration succeeds
with TaskGroup('table_creation') as table_group:
    create_table_tasks = PythonOperator.partial(
        task_id='create_table',
        python_callable=task_create_table,
    ).expand(
        op_args=validate_tasks.output.map(lambda x: [x])
    )

# Step 5: Aggregate results
collect_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=task_collect_results,
    op_args=[
        validate_tasks.output,
        register_tasks.output,
        create_table_tasks.output,
    ],
)

# Step 6: Report to GitHub
github_task = PythonOperator(
    task_id='report_to_github',
    python_callable=lambda: None,  # Implementation details omitted
)

# DAG Dependencies
fetch_task >> validate_tasks >> schema_group >> table_group >> collect_task >> github_task
```

**Key architectural patterns:**

- **Dynamic task expansion**: `.expand()` with `fetch_task.output.map()` creates one task instance per contract. The Airflow UI shows each separately.
- **Sequential per-contract provisioning**: Schema registration runs after validation for each contract, and table creation depends on schema registration succeeding. This guarantees the schema exists before creating the table.
- **Parallel across contracts**: With 100 contracts, Airflow creates 100 validate tasks, 100 register tasks, and 100 create tasks that run concurrently (limited by executor parallelism).
- **TaskGroups for organization**: Schema and table provisioning tasks are grouped for cleaner UI and logical separation.
- **Result aggregation via xcom_pull**: The collect task pulls results from all dynamic instances (not individual tasks) into lists, then processes them as a batch.
- **Failure isolation**: If contract-A's table creation fails, contracts B through Z still complete successfully. The aggregation step captures partial results.

## Key Design Decisions

### 1. **Dynamic Task Mapping for Per-Contract Isolation**

Airflow's `.expand()` method enables one critical pattern: creating one task instance per contract without manual loops.

```python
# One validation task per contract
validate_tasks = PythonOperator.partial(
    task_id='validate_contract',
    python_callable=task_validate_contract,
).expand(
    op_args=fetch_task.output.map(lambda x: [x])  # Map each contract to a task
)
```

**Why this matters:**

- **Failure isolation**: If contract-A fails validation, contracts B–Z still run. One contract's failure doesn't cascade.
- **Native visibility**: Airflow UI shows each contract separately—you can click into any contract's task instance to see logs, XCom values, and retry history.
- **Parallel execution**: All 100 validation tasks (or 100 register tasks) run concurrently, limited only by executor parallelism. No manual threading or process pools needed.
- **Result aggregation**: XCom lets child tasks (like `collect_task`) pull results from *all* dynamic instances in one call: `xcom_pull(task_ids="validate_contract")` returns a list.

### 2. **Sequential Per-Contract Provisioning (Schema → Table)**

Rather than running schema registration and table creation in parallel for each contract, the refactored DAG sequences them:

```
validate_task[contract_1] 
  → register_task[contract_1] 
    → create_table_task[contract_1]
```

But across contracts, they're parallel:

```
validate_task[1..N] (all N run concurrently)
  → register_task[1..N] (all N run concurrently, after validation)
    → create_table_task[1..N] (all N run concurrently, after registration)
```

**Benefits:**

- **Guarantees consistency**: Schema always exists before table creation attempts it.
- **Simplified error handling**: Clear causality—table creation fails only if schema registration failed, not due to a race condition.
- **Reduced AWS API contention**: Sequential per-contract means fewer concurrent AWS Glue API calls per contract (though still highly parallel across contracts).

### 3. **Composing Production-Grade Solutions**

Rather than building schema validation and table creation from scratch, leverage:

- **AWS Glue Schema Registry** for schema versioning, compatibility checking, and schema evolution.
- **AWS Glue Catalog** for table metadata and Iceberg integration.
- **Apache Airflow** for orchestration, retry logic, and audit trails.

Each tool is mature and battle-tested. The integration layer—our DAG tasks—is thin and focused on orchestration, not reimplementing features these services already provide.

### 4. **Hexagonal Architecture for Decoupling**

The contract processing logic lives in library modules, independent of Airflow:

```
airflow/
├── dags/
│   └── contract_provisioning_dag.py  # DAG definition (orchestration only)
├── tasks/
│   ├── contract_tasks.py             # Contract validation logic (business logic)
│   └── github_tasks.py               # GitHub integration
└── lib/
    ├── models.py                     # Data models (Contract, ValidationResult)
    ├── validator.py                  # Schema validation
    ├── converters.py                 # Contract → AVRO conversion
    ├── aws_glue.py                   # AWS Glue wrapper
    └── exceptions.py                 # Custom exceptions
```

This separation ensures:

- **Testability**: Validation logic runs in unit tests without Airflow.
- **Reusability**: GitHub Actions workflows can import the same libraries.
- **Portability**: Replace Airflow with Step Functions or Prefect without rewriting core logic.

### 5. **Airflow Variables for Configuration**

Use Airflow variables instead of hardcoding:

```python
Variable.get("contracts_dir", "contracts/current")
Variable.get("repo_path", "/app")
Variable.get("github_token")
Variable.get("github_repo")
Variable.get("aws_glue_registry_name", "default-registry")
```

This decouples DAG code from infrastructure configuration. Data engineers own the DAG; ops teams own the variables.

### 6. **Graceful Partial Failure Handling**

The aggregation step handles contracts that failed at different stages:

```python
aggregated = ContractTasks.collect_results(
    validation_results,      # Some may have error=True
    schema_results,          # Some may be None (validation failed)
    table_results            # Some may be None (schema registration failed)
)
```

The GitHub comment reports per-contract status, showing which contracts succeeded and which failed (and at which stage). Teams get visibility into the exact point of failure without needing to dig through logs.

## Real-World Flow: Adding a New Contract

Here's what happens when a developer adds a new contract:

1. **Developer creates and commits contract file**:
   ```bash
   mkdir -p contracts/current/users/01
   cat > contracts/current/users/01/users.json << 'EOF'
   {
     "contract_id": "users-v1",
     "name": "User Accounts",
     "description": "Core user profile data",
     "columns": [
       {"name": "user_id", "data_type": "string"},
       {"name": "email", "data_type": "string"},
       {"name": "created_at", "data_type": "timestamp"},
       {"name": "status", "data_type": "string"}
     ]
   }
   EOF
   git add contracts/current/users/01/users.json
   git commit -m "add users contract v1"
   git push
   ```

2. **GitHub Actions workflow triggers** (listening for changes to `contracts/current/**`).

3. **Workflow invokes Airflow DAG** with context:
   - `github_pr_number`: The PR that triggered the workflow
   - `changed_files`: List of contract files changed (only these are processed)
   - `repo_path`: Path to repository clone

4. **Airflow DAG executes with per-contract isolation**:
   
   a) **Fetch**: Lists all changed contract files → `[users.json]`
   
   b) **Validate** (dynamic): Creates `validate_contract[0]` task (one per contract)
      - Reads JSON, checks required fields, validates column types
      - Logs: `✅ Validated users-v1`
      - Returns: `{contract_id: "users-v1", columns: [...], status: "valid"}`
   
   c) **Register Schema** (dynamic): Creates `register_schema[0]` task
      - Converts contract to AVRO schema
      - Calls `glue.create_schema(SchemaName="users-v1", ...)`
      - Logs: `✅ Registered schema for users-v1`
      - Returns: `{contract_id: "users-v1", schema_arn: "...", status: "registered"}`
      - **If this fails**, the table creation task is skipped for this contract (but other contracts still process)
   
   d) **Create Table** (dynamic): Creates `create_table[0]` task
      - Converts schema columns to Glue table format
      - Calls `glue.create_table(DatabaseName="contracts", TableType="ICEBERG", ...)`
      - Logs: `✅ Created table for users-v1`
      - Returns: `{contract_id: "users-v1", location: "s3://bucket/iceberg/users-v1/", status: "created"}`
   
   e) **Aggregate Results**: Collects all results into a report:
      ```
      {
        "schemas_registered": 1,
        "tables_created": 1,
        "results": [
          {
            "contract_id": "users-v1",
            "validation": {"status": "valid"},
            "schema": {"status": "registered"},
            "table": {"status": "created"}
          }
        ]
      }
      ```

5. **GitHub PR gets a comment** with per-contract status:
   ```
   ✅ Contract Provisioning Results
   
   **Summary**: 1 schema registered, 1 table created
   
   | Contract | Validation | Schema | Table |
   |----------|-----------|--------|-------|
   | users-v1 | ✓ | ✓ | ✓ |
   ```

6. **Developer approves PR** with confidence—schema and table are ready in production. The next data pipeline can start writing to `contracts.users_v1`.

## Advantages Over Microservices

Why choose DAG-based orchestration over microservices?

| Aspect | Microservices | Airflow DAGs |
|--------|---------------|-------------|
| **Deployment** | Multi-service choreography, version coordination | Single YAML/Python file, no coordination |
| **Observability** | Distributed tracing, multiple dashboards | Unified DAG history in one UI |
| **Scaling** | Horizontal scaling per service | Horizontal scaling per task via Airflow executors |
| **Testing** | Mock inter-service calls, integration tests | Unit test library code independently |
| **Operations** | Manage service uptime, rolling deployments | Manage single Airflow instance |
| **Cost** | Multiple containers, multiple databases | Single Airflow container, shared metadata DB |

For contract provisioning, the DAG approach reduces friction: one logical workflow, one deployment pipeline, one audit trail.

## Getting Started

The full reference implementation is available at [data-contract-flow](https://github.com/vanikkars/data-contract-flow):

```bash
# Clone and setup
git clone https://github.com/vanikkars/data-contract-flow.git
cd data-contract-flow

# Start Airflow
cp .env.example .env
source .env
make airflow-up

# Access UI
open http://localhost:8080
# Login: admin / (check docker logs for password)

# Create a test contract
mkdir -p contracts/current/demo/01
cat > contracts/current/demo/01/demo.json << 'EOF'
{
  "contract_id": "demo-v1",
  "name": "Demo Contract",
  "columns": [
    {"name": "id", "data_type": "string"},
    {"name": "name", "data_type": "string"}
  ]
}
EOF

# Trigger the DAG
make airflow-trigger

# Watch in Airflow UI
```

## Conclusion

Data contracts enforce trust in data pipelines, but enforcement requires orchestration. Rather than building custom systems or deploying microservices, leverage Airflow's dynamic task mapping and compose production-grade AWS services.

**The key insight from this refactoring:** Per-contract isolation through dynamic task expansion eliminates the need for manual batch loops, thread pools, or custom parallelism code. Airflow handles it natively:

- **One task instance per contract**: Failure isolation, clear UI visibility, independent retry logic.
- **Sequential provisioning per contract**: Schema registration before table creation ensures consistency.
- **Parallel across contracts**: All contracts process concurrently within executor limits.
- **Built-in result aggregation**: XCom and dynamic task outputs make collecting per-contract results trivial.

This approach scales to hundreds of contracts and integrates seamlessly with existing infrastructure. The DAG becomes the contract-processing system—no additional services, no additional operational burden, no custom orchestration logic to maintain.

The pattern generalizes to any data governance requirement: schema validation, data quality checks, access control provisioning, metadata enrichment. Start with contracts; extend the DAG with additional dynamic tasks as your governance needs grow.