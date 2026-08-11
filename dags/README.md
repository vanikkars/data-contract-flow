# Airflow DAGs for Contract Provisioning

## Overview

The `contract_provisioning_dag` orchestrates the complete workflow for validating, registering, and provisioning data contracts as schemas and Iceberg tables.

## DAG: `contract_provisioning`

**Purpose:** End-to-end contract provisioning pipeline  
**Schedule:** Manual trigger or webhook (from GitHub Actions)  
**Owner:** data-engineering  
**Tags:** contracts, provisioning, glue, iceberg

## Workflow

```
┌─────────────────────┐
│ Fetch Contracts     │ Read contract files from contracts/current
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│ Validate Contracts  │ Validate JSON schema, required fields, data types
└──────────┬──────────┘
           ↓
      ┌────┴────┐
      ↓         ↓
┌──────────┐  ┌──────────────────┐
│ Register │  │ Create Iceberg   │
│ Schemas  │  │ Tables           │
└────┬─────┘  └────────┬─────────┘
     └─────────┬──────┘
               ↓
      ┌──────────────────┐
      │ Collect & Format │ Aggregate results
      │ Results          │
      └────────┬─────────┘
               ↓
      ┌──────────────────┐
      │ Report to GitHub │ Comment on PR with results
      └──────────────────┘
```

## Tasks

### 1. `fetch_contracts` (PythonOperator)
**Purpose:** Discover contract files in the repository

**Output (XCom):**
```python
{
    "contracts": ["/app/contracts/current/users/01/users.json", ...],
    "count": 3
}
```

**Environment Variables:**
- `repo_path` - Base repository path (default: `/app`)
- `contracts_dir` - Relative path to contracts (default: `contracts/current`)

### 2. `validate_contracts` (PythonOperator)
**Purpose:** Validate each contract JSON and schema

**Input:** Contract file paths from `fetch_contracts`  
**Output (XCom):**
```python
[
    {
        "file_path": "/app/contracts/current/users/01/users.json",
        "contract_id": "users-v1",
        "name": "Users",
        "status": "valid",
        "contract_dict": {...}
    },
    ...
]
```

**Validation Checks:**
- Required fields: `contract_id`, `name`, `description`, `columns`
- Column fields: `name`, `data_type`
- Valid data types: string, integer, number, boolean, date, timestamp, object, array
- No duplicate column names

**Errors:** Reported with specific field errors (e.g., "Column 'email' missing 'data_type'")

### 3. `register_schemas` (PythonOperator)
**Purpose:** Register validated contracts as schemas in AWS Glue Schema Registry

**Input:** Valid contracts from `validate_contracts`  
**Output (XCom):**
```python
[
    {
        "file_path": "/app/contracts/current/users/01/users.json",
        "contract_id": "users-v1",
        "schema_arn": "arn:aws:glue:us-east-1:123456789012:schema/users-v1/1",
        "status": "registered"
    },
    ...
]
```

**Features:**
- Converts contract to AVRO schema
- Handles schema versioning (creates new version if definition changed)
- Validates compatibility (FORWARD_ALL mode)
- Polls for version validation completion
- Updates schema tags with metadata

**AWS Glue Integration:**
- Uses `AWS_DEFAULT_REGION` environment variable
- Reads registry name from `TF_VAR_registry_name` (default: `schema-registry`)

### 4. `create_iceberg_tables` (PythonOperator)
**Purpose:** Create or update Iceberg tables in AWS Glue Catalog

**Input:** Valid contracts from `validate_contracts`  
**Output (XCom):**
```python
[
    {
        "file_path": "/app/contracts/current/users/01/users.json",
        "contract_id": "users-v1",
        "table_name": "users_v1",
        "status": "created",  # or "updated", "unchanged"
        "columns_count": 5
    },
    ...
]
```

**Features:**
- Creates table with generated S3 location (auto from AWS account ID)
- Detects schema changes (added/removed/modified columns)
- Updates existing tables with new schema
- Creates database if it doesn't exist
- Stores contract metadata in table parameters

**Database:** Defaults to `iceberg_tables`

### 5. `collect_and_format_results` (PythonOperator)
**Purpose:** Aggregate all results and format for GitHub

**Input:** Results from all previous tasks  
**Output (XCom):**
```python
{
    "total_contracts": 3,
    "validation_passed": 3,
    "schema_registered": 3,
    "tables_created": 2,
    "tables_updated": 1,
    "results_by_contract": {...},
    "all_results": {
        "validation": [...],
        "schema": [...],
        "table": [...]
    },
    "github_payload": {
        "body": "## Provisioning Results...",
        "pr_number": 42
    }
}
```

### 6. `report_to_github` (PythonOperator)
**Purpose:** Comment on GitHub PR with results

**Placeholder:** Currently logs output; in production:
- Use PyGithub library to create PR comment
- Or call GitHub API directly
- Or send results back to GitHub Actions webhook

**Comment Format:**
- ✅ Validation results (passed/failed)
- 📋 Schema registration status
- 🗄️ Iceberg table creation/update status
- ✅/⚠️ Ready for merge or action items

## Running the DAG

### Local Development (Docker)

```bash
# Start Airflow with docker-compose
docker-compose -f docker-compose.airflow.yml up

# Access UI
open http://localhost:8080

# Trigger DAG manually
curl -X POST http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf": {"github_pr_number": 42}}'
```

### Production Deployment

1. Deploy Airflow to your environment (AWS ECS, K8s, EC2)
2. Configure AWS credentials via IAM role or environment variables
3. Set Airflow Variables for GitHub integration
4. Trigger via GitHub Actions webhook

## Configuration

### Environment Variables

**AWS:**
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_DEFAULT_REGION` - AWS region (default: us-east-1)
- `TF_VAR_registry_name` - Glue Schema Registry name (default: schema-registry)

**Application:**
- `AIRFLOW_VAR_repo_path` - Repository root path (default: /app)
- `AIRFLOW_VAR_contracts_dir` - Contracts directory (default: contracts/current)
- `AIRFLOW_VAR_github_token` - GitHub API token (for PR comments)
- `AIRFLOW_VAR_github_repo` - GitHub repository (format: owner/repo)
- `AIRFLOW_VAR_github_pr_number` - GitHub PR number (passed by GitHub Actions)

### Airflow Variables (UI)

Set these in Airflow UI → Admin → Variables:

```
Key: repo_path
Value: /app

Key: contracts_dir
Value: contracts/current

Key: github_token
Value: <your-github-token>

Key: github_repo
Value: vanikkars/schema-registry

Key: github_pr_number
Value: <set dynamically by GitHub Actions>
```

## Task Dependencies

```
fetch_contracts
    ↓
validate_contracts
    ↓
(register_schemas, create_iceberg_tables) in parallel
    ↓
collect_and_format_results
    ↓
report_to_github
```

- Register schemas and create tables run in parallel (no dependency)
- All must complete before results are collected
- GitHub reporting is final step

## Error Handling

### Validation Errors
- Specific field errors (missing fields, invalid types)
- Contracts with errors are skipped for schema registration and table creation
- DAG continues with valid contracts

### Schema Registration Errors
- Compatibility violations (attempted to remove/modify fields)
- Detailed error message with diff analysis
- Table creation still proceeds for other contracts

### Table Creation Errors
- Database creation failures
- S3 location generation errors
- Glue Catalog API errors
- DAG logs all errors, continues with remaining contracts

### Retry Logic
- Default: 1 retry on failure
- Retry delay: 5 minutes
- Execution timeout: 30 minutes per task

## Monitoring

### Airflow UI
- DAG runs: http://localhost:8080/dags/contract_provisioning/runs
- Task logs: Click task → Log
- XCom values: Click task → XCom

### Logs
- Location: `logs/dags/contract_provisioning/`
- Format: Airflow standard (timestamp, task, level, message)

### GitHub Integration
- PR comments with detailed results
- Success/failure indicators
- Approval command for merge

## GitHub Integration

### Triggered By
GitHub Actions workflow when contracts change:

```yaml
# .github/workflows/contract-validation-workflow.yml
on:
  pull_request:
    paths:
      - 'contracts/current/**'

jobs:
  trigger-airflow:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Airflow DAG
        run: |
          curl -X POST $AIRFLOW_URL/api/v1/dags/contract_provisioning/dagRuns \
            -H "Authorization: Bearer $AIRFLOW_TOKEN" \
            -d "{\"conf\": {\"github_pr_number\": ${{ github.event.pull_request.number }}}}"
```

### Output
Airflow comments on PR with:
1. Validation results (all contracts)
2. Schema registration status (ARNs)
3. Table creation status (table names, changes)
4. Approval request or action items

## Future Enhancements

1. **GitHub Integration:** Use PyGithub to create actual PR comments
2. **Async Table Creation:** Use AsyncPythonOperator for concurrent table operations
3. **Notifications:** Slack/email on failures
4. **Approval Workflow:** Automatic merge after human approval
5. **Rollback:** Ability to revert schema/table changes
6. **Testing:** Unit tests for all tasks
7. **Monitoring:** Prometheus metrics, custom dashboards
8. **Audit Log:** Track who changed what and when

## Troubleshooting

### DAG not appearing in UI
```bash
# Check DAG syntax
airflow dags list

# View parse errors
airflow dags validate dags/contract_provisioning_dag.py
```

### Tasks failing
```bash
# View task logs
docker logs airflow-webserver

# Check XCom values
airflow tasks output contract_provisioning fetch_contracts --execution-date <date>
```

### AWS credential issues
```bash
# Verify credentials in container
docker exec airflow-webserver aws sts get-caller-identity

# Check environment variables
docker exec airflow-webserver env | grep AWS
```

### Database connection errors
```bash
# Check postgres health
docker logs airflow-postgres

# Verify connection
docker exec airflow-webserver airflow db check
```
