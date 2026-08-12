"""Airflow DAG for contract validation, schema registration, and Iceberg table provisioning.

This DAG replaces two separate microservices (Registry API and Iceberg Service) by orchestrating
all operations using Airflow tasks. It:

1. Fetches contract files from the repository
2. Validates each contract
3. Registers schemas with AWS Glue Schema Registry
4. Creates/updates Iceberg tables in AWS Glue Catalog
5. Reports results to GitHub PR

Triggered by:
- GitHub Actions webhook (when contracts/current/** changes)
- Manual trigger for re-provisioning
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Add parent directory to path so we can import tasks
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.contract_tasks import ContractTasks
from tasks.github_tasks import GitHubTasks

logger = logging.getLogger(__name__)

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    "contract_provisioning",
    default_args=default_args,
    description="Validate, register, and provision contracts as schemas and Iceberg tables",
    schedule_interval=None,  # Manual or webhook triggered
    catchup=False,
    tags=["contracts", "provisioning", "glue", "iceberg"],
    is_paused_upon_creation=False,
    user_defined_macros={
        "now": datetime.now(),
    },
)

# ============================================================================
# Task Functions
# ============================================================================

def task_fetch_contracts(**context):
    """Fetch contract files from repository."""
    logger.info("🔍 Starting contract provisioning DAG")

    repo_path = Variable.get("repo_path", "/app")
    contracts_dir = Variable.get("contracts_dir", "contracts/current")

    contract_files = ContractTasks.fetch_contract_files(repo_path, contracts_dir)

    if not contract_files:
        logger.warning("⚠️  No contract files found")
        return {"contracts": [], "count": 0}

    logger.info(f"📋 Found {len(contract_files)} contract files")

    # Push to XCom for downstream tasks
    context["task_instance"].xcom_push(key="contract_files", value=contract_files)

    return {
        "contracts": contract_files,
        "count": len(contract_files),
    }


def task_validate_contracts(**context):
    """Validate each contract file."""
    ti = context["task_instance"]

    # Get contract files from upstream task
    contract_files = ti.xcom_pull(
        task_ids="fetch_contracts",
        key="contract_files"
    )

    if not contract_files:
        logger.info("No contracts to validate")
        return []

    logger.info(f"✓ Validating {len(contract_files)} contracts")

    validation_results = []
    for contract_path in contract_files:
        try:
            result = ContractTasks.validate_contract(contract_path)
            result["status"] = "valid"
            validation_results.append(result)
        except ValueError as e:
            logger.error(f"❌ Validation failed for {contract_path}: {str(e)}")
            validation_results.append({
                "file_path": contract_path,
                "status": "invalid",
                "error": str(e),
            })

    # Push results to XCom
    ti.xcom_push(key="validation_results", value=validation_results)

    passed = sum(1 for r in validation_results if r["status"] == "valid")
    logger.info(f"✅ Validation complete: {passed}/{len(validation_results)} passed")

    return validation_results


def task_register_schemas(**context):
    """Register validated contracts as schemas."""
    ti = context["task_instance"]

    # Get validation results
    validation_results = ti.xcom_pull(
        task_ids="validate_contracts",
        key="validation_results"
    )

    if not validation_results:
        logger.info("No valid contracts to register")
        return []

    # Only process valid contracts
    valid_contracts = [r for r in validation_results if r["status"] == "valid"]

    logger.info(f"📝 Registering {len(valid_contracts)} schemas")

    schema_results = []
    for result in valid_contracts:
        contract_path = result["file_path"]
        try:
            schema_result = ContractTasks.register_schema(contract_path)
            schema_results.append(schema_result)
        except Exception as e:
            logger.error(f"❌ Schema registration failed for {contract_path}: {str(e)}")
            schema_results.append({
                "file_path": contract_path,
                "contract_id": result.get("contract_id", "unknown"),
                "status": "failed",
                "error": str(e),
            })

    # Push results to XCom
    ti.xcom_push(key="schema_results", value=schema_results)

    registered = sum(1 for r in schema_results if r.get("status") == "registered")
    logger.info(f"✅ Schema registration complete: {registered}/{len(schema_results)} registered")

    return schema_results


def task_create_iceberg_tables(**context):
    """Create Iceberg tables from contracts."""
    ti = context["task_instance"]

    # Get validation results (need all contracts, not just schema-registered)
    validation_results = ti.xcom_pull(
        task_ids="validate_contracts",
        key="validation_results"
    )

    if not validation_results:
        logger.info("No valid contracts to create tables for")
        return []

    valid_contracts = [r for r in validation_results if r["status"] == "valid"]

    logger.info(f"🗄️  Creating {len(valid_contracts)} Iceberg tables")

    table_results = []
    for result in valid_contracts:
        contract_path = result["file_path"]
        try:
            # Note: This task runs synchronously, but the function is async-ready
            # In production, use AsyncPythonOperator or separate async handling
            table_result = ContractTasks.create_iceberg_table.__wrapped__(contract_path)
            table_results.append(table_result)
        except Exception as e:
            logger.error(f"❌ Table creation failed for {contract_path}: {str(e)}")
            table_results.append({
                "file_path": contract_path,
                "contract_id": result.get("contract_id", "unknown"),
                "status": "failed",
                "error": str(e),
            })

    # Push results to XCom
    ti.xcom_push(key="table_results", value=table_results)

    created = sum(1 for r in table_results if r.get("status") == "created")
    updated = sum(1 for r in table_results if r.get("status") == "updated")
    logger.info(
        f"✅ Table creation complete: {created} created, {updated} updated "
        f"({len(table_results)} total)"
    )

    return table_results


def task_collect_and_format_results(**context):
    """Collect results from all tasks and format for GitHub."""
    ti = context["task_instance"]

    # Get all results from previous tasks
    validation_results = ti.xcom_pull(task_ids="validate_contracts", key="validation_results") or []
    schema_results = ti.xcom_pull(task_ids="register_schemas", key="schema_results") or []
    table_results = ti.xcom_pull(task_ids="create_iceberg_tables", key="table_results") or []

    logger.info("📊 Collecting results from all tasks")

    # Collect results
    aggregated = ContractTasks.collect_results(validation_results, schema_results, table_results)

    # Push aggregated results to XCom
    ti.xcom_push(key="aggregated_results", value=aggregated)

    # Format for GitHub
    github_payload = GitHubTasks.get_pr_comment_body(
        aggregated,
        github_pr_number=Variable.get("github_pr_number", None)
    )

    ti.xcom_push(key="github_payload", value=github_payload)

    # Log for visibility
    GitHubTasks.log_results_for_github(aggregated)

    logger.info("✅ Results collected and formatted")

    return {
        "aggregated": aggregated,
        "github_payload": github_payload,
    }


def task_report_to_github(**context):
    """Report results to GitHub PR (if applicable).

    This is a placeholder task. In production, use:
    - PyGithub library
    - GitHub Actions with gh CLI
    - Webhooks back to GitHub Actions
    """
    ti = context["task_instance"]

    github_payload = ti.xcom_pull(
        task_ids="collect_and_format_results",
        key="github_payload"
    )

    pr_number = github_payload.get("pr_number")
    pr_comment = github_payload.get("body")

    if not pr_number:
        logger.info("ℹ️  No GitHub PR number provided, skipping GitHub comment")
        return {"status": "skipped"}

    logger.info(f"📤 Would comment on PR #{pr_number}")
    logger.info(f"Comment preview:\n{pr_comment[:200]}...")

    # In production, uncomment and use actual GitHub API:
    # from github import Github
    # g = Github(Variable.get("github_token"))
    # repo = g.get_repo(Variable.get("github_repo"))
    # pr = repo.get_pull(pr_number)
    # pr.create_issue_comment(pr_comment)
    # logger.info(f"✅ Commented on PR #{pr_number}")

    return {
        "status": "success",
        "pr_number": pr_number,
        "comment_length": len(pr_comment),
    }


# ============================================================================
# DAG Tasks
# ============================================================================

with dag:
    fetch_task = PythonOperator(
        task_id="fetch_contracts",
        python_callable=task_fetch_contracts,
        doc="Fetch contract files from contracts/current directory",
    )

    validate_task = PythonOperator(
        task_id="validate_contracts",
        python_callable=task_validate_contracts,
        doc="Validate each contract against schema",
    )

    register_task = PythonOperator(
        task_id="register_schemas",
        python_callable=task_register_schemas,
        doc="Register contracts as schemas in AWS Glue Schema Registry",
    )

    create_tables_task = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=task_create_iceberg_tables,
        doc="Create/update Iceberg tables in AWS Glue Catalog",
    )

    collect_task = PythonOperator(
        task_id="collect_and_format_results",
        python_callable=task_collect_and_format_results,
        doc="Collect results from all tasks and format for GitHub",
    )

    github_task = PythonOperator(
        task_id="report_to_github",
        python_callable=task_report_to_github,
        doc="Report results to GitHub PR as comment",
    )

    # DAG dependency
    # fetch → validate → (register + create_tables) → collect → github
    fetch_task >> validate_task >> [register_task, create_tables_task] >> collect_task >> github_task
