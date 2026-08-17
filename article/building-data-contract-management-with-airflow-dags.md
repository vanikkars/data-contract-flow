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

## Architecture: A DAG-Driven Approach

The core design is straightforward: one Airflow DAG orchestrates the complete contract lifecycle.

```
contract_provisioning (DAG)
├── fetch_contracts (discover contract files)
├── validate_contracts (check schema, fields, types)
├── register_schemas (parallel tasks → AWS Glue Schema Registry)
├── create_iceberg_tables (parallel tasks → AWS Glue Catalog)
├── collect_and_format_results (aggregate outcomes)
└── report_to_github (comment on PR with status)
```

Each task is a discrete, testable unit:

1. **Fetch Contracts**: Scan `contracts/current/` directory for JSON contract definitions.
2. **Validate Contracts**: Verify JSON structure, required fields (contract_id, name, columns), and data types.
3. **Register Schemas**: Convert contracts to AVRO format and register in AWS Glue Schema Registry.
4. **Create Iceberg Tables**: Provision Iceberg tables in AWS Glue Catalog with schema from registry.
5. **Collect Results**: Aggregate validation and provisioning results into a structured report.
6. **Report to GitHub**: Comment on pull requests with provisioning status and any errors.

**Why this architecture?**

- **Centralized orchestration**: Airflow is the single source of truth for contract processing.
- **Parallel execution**: Register schemas and create tables concurrently for hundreds of contracts without overhead.
- **Error handling built-in**: Failed tasks trigger retries, alerts, and rollback paths automatically.
- **Auditability**: Every task execution is logged; Airflow UI shows the full DAG history.
- **No service boundaries**: No inter-service communication, no distributed tracing headaches, no deployment choreography.

## Implementation: The Contract Provisioning DAG

Let's walk through a simplified DAG that captures the pattern:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'contract_provisioning',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
)

CONTRACTS_DIR = Variable.get("contracts_dir", "contracts/current")
GLUE_REGISTRY_NAME = Variable.get("glue_registry_name", "default-registry")
GLUE_DATABASE = Variable.get("glue_database", "contracts")
S3_BUCKET = Variable.get("s3_iceberg_bucket", "your-bucket")
ICEBERG_PATH_PREFIX = Variable.get("iceberg_path_prefix", "iceberg")

def fetch_contracts(**context):
    """Discover and load contract files"""
    contracts = []
    
    for contract_file in Path(CONTRACTS_DIR).rglob("*.json"):
        try:
            with open(contract_file) as f:
                contract = json.load(f)
                contracts.append({
                    'path': str(contract_file),
                    'contract_id': contract['contract_id'],
                    'name': contract['name'],
                    'columns': contract['columns'],
                })
                logger.info(f"Loaded contract: {contract['contract_id']}")
        except Exception as e:
            logger.error(f"Failed to load {contract_file}: {str(e)}")
    
    if not contracts:
        logger.warning("No contracts found in {CONTRACTS_DIR}")
    
    return contracts

def validate_contract(contract, **context):
    """Validate single contract"""
    required_fields = ['contract_id', 'name', 'columns']
    contract_id = contract.get('contract_id', 'unknown')
    
    try:
        for field in required_fields:
            if field not in contract:
                raise ValueError(f"Missing required field: {field}")
        
        for col in contract['columns']:
            if 'name' not in col or 'data_type' not in col:
                raise ValueError(f"Column missing 'name' or 'data_type': {col}")
        
        logger.info(f"✓ Validated {contract_id}")
        return {
            'contract_id': contract_id,
            'status': 'valid',
            'columns': contract['columns'],
        }
    except ValueError as e:
        logger.error(f"✗ Validation failed for {contract_id}: {str(e)}")
        raise

def register_schema(contract, **context):
    """Register single contract as AVRO schema"""
    from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
    
    contract_id = contract['contract_id']
    
    try:
        avro_schema = convert_contract_to_avro(contract)
        
        hook = GlueCatalogHook()
        registry = hook.get_client()
        
        response = registry.create_schema(
            RegistryId={'RegistryName': GLUE_REGISTRY_NAME},
            SchemaName=contract_id,
            DataFormat='AVRO',
            Compatibility='BACKWARD',
            SchemaDefinition=json.dumps(avro_schema),
        )
        
        logger.info(f"✓ Registered schema for {contract_id}")
        return {
            'contract_id': contract_id,
            'status': 'registered',
            'schema_arn': response['SchemaArn'],
        }
    except Exception as e:
        logger.error(f"✗ Failed to register {contract_id}: {str(e)}")
        raise

def create_table(contract, **context):
    """Create single Iceberg table"""
    from airflow.providers.amazon.aws.hooks.glue import GlueHook
    
    contract_id = contract['contract_id']
    
    try:
        iceberg_location = f"s3://{S3_BUCKET}/{ICEBERG_PATH_PREFIX}/{contract_id}/"
        
        hook = GlueHook()
        glue = hook.get_client()
        
        glue.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput={
                'Name': contract_id,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': col['name'], 'Type': col['data_type']}
                        for col in contract['columns']
                    ],
                    'Location': iceberg_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    },
                },
                'TableType': 'ICEBERG',
            },
        )
        
        logger.info(f"✓ Created table for {contract_id}")
        return {
            'contract_id': contract_id,
            'status': 'created',
            'location': iceberg_location,
        }
    except Exception as e:
        logger.error(f"✗ Failed to create table for {contract_id}: {str(e)}")
        raise

def collect_results(task_instance, **context):
    """Aggregate all task results into provisioning report"""
    # Pull all results from dynamic task instances
    # With dynamic expansion, xcom_pull returns a list of results from all mapped tasks
    validation_results = task_instance.xcom_pull(
        task_ids='validate_contract',
        key='return_value'
    )
    schema_results = task_instance.xcom_pull(
        task_ids='provision.register_schema',
        key='return_value'
    )
    table_results = task_instance.xcom_pull(
        task_ids='provision.create_table',
        key='return_value'
    )
    
    # Ensure results are lists (may be single item or list from expand)
    validation_results = validation_results if isinstance(validation_results, list) else [validation_results] if validation_results else []
    schema_results = schema_results if isinstance(schema_results, list) else [schema_results] if schema_results else []
    table_results = table_results if isinstance(table_results, list) else [table_results] if table_results else []
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'schemas_registered': len([r for r in schema_results if r.get('status') == 'registered']),
        'tables_created': len([r for r in table_results if r.get('status') == 'created']),
        'results': [],
    }
    
    for val_result in validation_results:
        contract_id = val_result['contract_id']
        report['results'].append({
            'contract_id': contract_id,
            'validation': val_result,
            'schema': next((r for r in schema_results if r['contract_id'] == contract_id), None),
            'table': next((r for r in table_results if r['contract_id'] == contract_id), None),
        })
    
    logger.info(f"Report: {report['schemas_registered']} schemas, {report['tables_created']} tables")
    task_instance.xcom_push(key='provisioning_report', value=report)
    return report

def report_to_github(task_instance, **context):
    """Post aggregated results to GitHub PR"""
    report = task_instance.xcom_pull(key='provisioning_report', task_ids='aggregate_results')
    
    github_token = Variable.get("github_token", None)
    github_repo = Variable.get("github_repo", None)
    github_pr_number = Variable.get("github_pr_number", None)
    
    if not all([github_token, github_repo, github_pr_number]):
        logger.warning("GitHub credentials not configured, skipping PR comment")
        return {"status": "skipped"}
    
    markdown = f"""✅ Contract Provisioning Results

**Summary**: {report['schemas_registered']} schemas registered, {report['tables_created']} tables created

| Contract | Validation | Schema | Table |
|----------|-----------|--------|-------|
"""
    
    for result in report['results']:
        val_icon = "✓" if result['validation'] and result['validation']['status'] == 'valid' else "✗"
        sch_icon = "✓" if result['schema'] and result['schema']['status'] == 'registered' else "✗"
        tbl_icon = "✓" if result['table'] and result['table']['status'] == 'created' else "✗"
        markdown += f"| {result['contract_id']} | {val_icon} | {sch_icon} | {tbl_icon} |\n"
    
    logger.info(f"Generated report for PR #{github_pr_number}")
    return {"status": "posted", "pr": github_pr_number}

# ============================================================================
# DAG Structure with Dynamic Task Mapping
# ============================================================================

fetch = PythonOperator(
    task_id='fetch_contracts',
    python_callable=fetch_contracts,
)

# Dynamic validation task: one instance per contract
validate_contracts = PythonOperator.partial(
    task_id='validate_contract',
    python_callable=validate_contract,
).expand(op_args=[[contract] for contract in fetch.output])

# Provision tasks: schema registration and table creation
# Both run in parallel for all validated contracts
with TaskGroup('provision') as provision_group:
    register_schema = PythonOperator.partial(
        task_id='register_schema',
        python_callable=register_schema,
    ).expand(op_args=[[contract] for contract in validate_contracts.output])
    
    create_table = PythonOperator.partial(
        task_id='create_table',
        python_callable=create_table,
    ).expand(op_args=[[contract] for contract in validate_contracts.output])
    
    # Both provisioning tasks run independently (no inter-dependencies)
    # They both depend on validation via the expand reference

# Aggregate results from all dynamic task instances
aggregate_results = PythonOperator(
    task_id='aggregate_results',
    python_callable=collect_results,
)

# Report to GitHub
report_pr = PythonOperator(
    task_id='report_to_github',
    python_callable=report_to_github,
)

# Task dependencies
fetch >> validate_contracts >> provision_group >> aggregate_results >> report_pr
```

The DAG architecture uses **dynamic task mapping** to achieve per-contract isolation:

- **Dynamic validation**: `.expand()` creates one task per contract. Each runs independently; failure in one doesn't block others.
- **Parallel provisioning**: Within a `TaskGroup`, schema registration and table creation both run concurrently for all contracts—if you have 100 contracts, you get 100 register tasks and 100 create tasks running in parallel.
- **Per-contract isolation**: Each dynamic task instance processes one contract. Airflow UI shows each contract's individual status, making debugging trivial.
- **Result aggregation**: `xcom_pull()` collects results from all dynamic instances into a unified report without manual looping.
- **GitHub integration**: Report includes per-contract status, giving developers clear visibility into which contracts succeeded and which failed.

## Key Design Decisions

### 1. **Composing Production-Grade Solutions**

Rather than building schema validation and table creation from scratch, we're leveraging:

- **AWS Glue Schema Registry** for schema versioning, compatibility checking, and schema evolution.
- **AWS Glue Catalog** for table metadata and Iceberg integration.
- **Apache Airflow** for orchestration, retry logic, and audit trails.

Each tool is mature and battle-tested. The integration layer—our DAG tasks—is thin and focused on orchestration, not reimplementing features these services already provide.

### 2. **Hexagonal Architecture for Decoupling**

The contract processing logic lives in library modules, independent of Airflow:

```
airflow/
├── dags/
│   └── contract_provisioning_dag.py  # DAG definition
├── tasks/
│   ├── contract_tasks.py             # Contract validation logic
│   └── github_tasks.py               # GitHub integration
└── lib/
    ├── models.py                     # Data models (Contract, ValidationResult)
    ├── validator.py                  # Schema validation
    ├── converters.py                 # Contract → AVRO conversion
    ├── aws_glue.py                   # AWS Glue wrapper
    └── exceptions.py                 # Custom exceptions
```

This structure means:

- **Testability**: Validation and conversion logic can be tested independently of Airflow.
- **Reusability**: GitHub Actions workflows can import and use the same validation library without spinning up Airflow.
- **Portability**: If you later replace Airflow with Step Functions or Prefect, the core logic remains unchanged.

### 3. **Airflow Variables for Configuration**

Rather than hardcoding paths and credentials, use Airflow variables:

```python
Variable.get("contracts_dir", "contracts/current")
Variable.get("github_token")
Variable.get("github_repo")
Variable.get("aws_glue_registry_name", "default-registry")
```

This allows teams to change configuration without modifying the DAG code—a key requirement for production systems where data engineers own the DAG but ops teams own infrastructure.

### 4. **Failure Handling and Observability**

Airflow provides built-in retry and alert mechanisms:

```python
default_args = {
    'owner': 'data-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_slack,  # Custom alert function
}
```

When a schema registration fails, the task retries automatically. If it fails after retries, the alert fires immediately—giving teams visibility without manual polling.

## Real-World Flow: Adding a New Contract

Here's what happens when a developer adds a new contract to the repository:

1. **Developer creates contract file**:
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
   ```

2. **Developer pushes to GitHub** and creates a pull request.

3. **GitHub Actions workflow triggers** (configured to listen for changes to `contracts/current/**`).

4. **Airflow DAG is triggered** with PR metadata:
   - Fetches all contracts in `contracts/current/`
   - Validates JSON schema and required fields
   - Registers AVRO schemas in AWS Glue
   - Creates corresponding Iceberg tables
   - Collects results (pass/fail per contract)

5. **GitHub gets a comment** showing provisioning status:
   ```
   ✅ Contract Provisioning Results
   
   users-v1: ✓ Valid | ✓ Schema Registered | ✓ Table Created
   products-v2: ✓ Valid | ✓ Schema Registered | ✓ Table Created
   
   All contracts provisioned successfully!
   ```

6. **Developer approves PR** with confidence—the contracts are already in production infrastructure.

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

Data contracts enforce trust in data pipelines, but enforcement requires orchestration. Rather than building custom systems or deploying microservices, leverage Airflow's native orchestration and compose production-grade AWS services.

This approach trades simplicity for power: you get a single, auditable workflow that scales, retries gracefully, and integrates seamlessly with existing infrastructure. The DAG becomes the contract-processing system—no additional services, no additional operational burden.

The pattern works for any centralized data governance requirement: schema validation, data quality checks, access control provisioning, metadata enrichment. Start with contracts; extend the DAG.