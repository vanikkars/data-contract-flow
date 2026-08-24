"""Airflow DAG for contract validation, schema registration, and Iceberg table provisioning.

This DAG uses dynamic task mapping with task groups to achieve true per-contract isolation:
- Each contract gets its own task instance through the entire pipeline
- Failures in one contract don't block others
- Airflow UI shows clear per-contract visibility

Pipeline:
1. Fetch contract files from repository
2. For each contract (dynamic tasks):
   - Validate the contract
   - Register schema in AWS Glue
   - Create/update Iceberg table
3. Aggregate results from all contracts
4. Report to GitHub PR

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
    description="Validate, register, and provision contracts as schemas and Iceberg tables (per-contract isolation)",
    schedule_interval=None,  # Manual or webhook triggered
    catchup=False,
    tags=["contracts", "provisioning", "glue", "iceberg"],
    is_paused_upon_creation=False,
)

# ============================================================================
# Task Functions
# ============================================================================

def get_conf(context, key: str, default=None):
    """Read a run parameter from dag_run.conf, falling back to an Airflow Variable.

    The GitHub Actions workflow passes `changed_files`, `process_all` and
    `github_pr_number` in the DAG run's `conf` payload. Reading these only from
    Variables silently ignored everything the trigger sent: `changed_files`
    resolved to None on every webhook-triggered run, so no contract was ever
    processed and every downstream task was skipped.

    conf wins because it is per-run; Variables remain the deployment-wide
    default for manual runs.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}

    if key in conf and conf[key] not in (None, ""):
        return conf[key]

    return Variable.get(key, default)


def task_fetch_contracts(**context):
    """Fetch contract files from repository.

    Returns list of contract file paths for dynamic task expansion.
    """
    logger.info("🔍 Starting contract provisioning DAG")

    repo_path = get_conf(context, "repo_path", "/app")
    contracts_dir = get_conf(context, "contracts_dir", "contracts/current")
    changed_files = get_conf(context, "changed_files", None)
    process_all = get_conf(context, "process_all", False)

    logger.info(
        f"Run parameters: contracts_dir={contracts_dir}, "
        f"process_all={process_all}, changed_files={changed_files!r}"
    )

    # Parse changed_files if it's a space-separated string
    if changed_files and isinstance(changed_files, str):
        changed_files = changed_files.split()

    # Parse process_all if it's a string
    if isinstance(process_all, str):
        process_all = process_all.lower() in ('true', '1', 'yes')

    contract_files = ContractTasks.fetch_contract_files(repo_path, contracts_dir, changed_files, process_all)

    if not contract_files:
        # Returning [] here would expand into zero downstream tasks, so every
        # validation task is SKIPPED and the run reports success without having
        # checked anything. A provisioning run that finds nothing to provision
        # is a misconfiguration, so fail loudly instead.
        raise ValueError(
            "No contract files to process — refusing to report success.\n"
            f"  repo_path:     {repo_path}\n"
            f"  contracts_dir: {contracts_dir}\n"
            f"  changed_files: {changed_files!r}\n"
            f"  process_all:   {process_all}\n"
            "\n"
            "Nothing would be validated, so downstream tasks would be skipped "
            "and the PR comment would show a misleading pass.\n"
            "Fix the trigger (is `changed_files` populated?) or set "
            "`process_all=true` to provision every contract."
        )

    logger.info(f"📋 Found {len(contract_files)} contract files")
    return contract_files


def task_validate_contract(contract_path: str, **context):
    """Validate a single contract file.

    This task runs once per contract (via dynamic expansion).

    Args:
        contract_path: Path to the contract file

    Returns:
        Dict with validation result

    Raises:
        ValueError: If validation fails (task will fail for this contract only)
    """
    try:
        result = ContractTasks.validate_contract(contract_path)
        result["file_path"] = contract_path
        result["status"] = "valid"
        logger.info(f"✅ Validated {result.get('contract_id', contract_path)}")
        return result
    except ValueError as e:
        logger.error(f"❌ Validation failed for {contract_path}: {str(e)}")
        raise


def task_register_schema(validation_result: Dict[str, Any], **context):
    """Register a validated contract as AVRO schema in AWS Glue.

    This task runs once per contract after validation succeeds.

    Args:
        validation_result: Result from validate_contract task

    Returns:
        Dict with registration result

    Raises:
        Exception: If registration fails (task will fail for this contract only)
    """
    contract_path = validation_result["file_path"]
    contract_id = validation_result.get("contract_id", "unknown")

    try:
        schema_result = ContractTasks.register_schema(contract_path)
        logger.info(f"✅ Registered schema for {contract_id}")
        return schema_result
    except Exception as e:
        logger.error(f"❌ Schema registration failed for {contract_id}: {str(e)}")
        raise


def task_create_table(validation_result: Dict[str, Any], **context):
    """Create/update Iceberg table from validated contract schema.

    This task runs once per contract after validation succeeds.
    Runs independently of schema registration (both start after validation).

    Args:
        validation_result: Result from validate_contract task

    Returns:
        Dict with table creation result

    Raises:
        Exception: If table creation fails (task will fail for this contract only)
    """
    contract_path = validation_result["file_path"]
    contract_id = validation_result.get("contract_id", "unknown")

    try:
        table_result = ContractTasks.create_iceberg_table(contract_path)

        # An un-awaited coroutine is truthy and returns instantly, so calling an
        # async function without awaiting it logged success for work that never
        # ran. The path is synchronous now; this guard keeps a silent
        # regression from reaching XCom as an apparent pass.
        if not isinstance(table_result, dict):
            raise TypeError(
                f"create_iceberg_table returned {type(table_result).__name__}, "
                f"expected dict. The table was NOT provisioned."
            )

        logger.info(
            f"✅ Table {table_result.get('status', 'processed')} for {contract_id}: "
            f"{table_result.get('table_name')}"
        )
        return table_result
    except Exception as e:
        logger.error(f"❌ Table creation failed for {contract_id}: {str(e)}")
        raise


def task_collect_results(validation_results: List[Dict],
                         schema_results: List[Dict],
                         table_results: List[Dict],
                         **context):
    """Collect and aggregate results from all dynamic task instances.

    This task runs once after all per-contract tasks complete.
    Handles partial failures gracefully.

    Args:
        validation_results: List of validation results from all contracts
        schema_results: List of schema registration results from all contracts
        table_results: List of table creation results from all contracts

    Returns:
        Dict with aggregated results
    """
    ti = context["task_instance"]

    logger.info("📊 Collecting results from all contract processing tasks")

    # Ensure we have lists
    validation_results = validation_results if isinstance(validation_results, list) else [validation_results] if validation_results else []
    schema_results = schema_results if isinstance(schema_results, list) else [schema_results] if schema_results else []
    table_results = table_results if isinstance(table_results, list) else [table_results] if table_results else []

    # Collect results via ContractTasks library
    aggregated = ContractTasks.collect_results(validation_results, schema_results, table_results)

    # Push to XCom for reporting task
    ti.xcom_push(key="aggregated_results", value=aggregated)

    # Format for GitHub
    github_payload = GitHubTasks.get_pr_comment_body(
        aggregated,
        github_pr_number=get_conf(context, "github_pr_number", None)
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
    """Report aggregated results to GitHub PR.

    This task runs once after all contracts are processed and results aggregated.

    Note: This is a placeholder. In production, implement using:
    - PyGithub library
    - GitHub Actions with gh CLI
    - Webhooks back to GitHub Actions
    """
    ti = context["task_instance"]

    github_payload = ti.xcom_pull(
        task_ids="aggregate_results",
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
# DAG Structure with Dynamic Task Mapping
# ============================================================================

with dag:
    # Step 1: Fetch all contract files
    fetch_task = PythonOperator(
        task_id="fetch_contracts",
        python_callable=task_fetch_contracts,
        doc="Fetch contract file paths from contracts/current directory",
    )

    # Step 2: Dynamic task expansion - one validate task per contract
    # Each contract is processed independently; failure in one doesn't block others
    validate_tasks = PythonOperator.partial(
        task_id="validate_contract",
        python_callable=task_validate_contract,
    ).expand(
        op_args=fetch_task.output.map(lambda x: [x])
    )

    # Step 3: Schema provisioning task group
    # Register schemas in AWS Glue per contract
    with TaskGroup("schema_provisioning", tooltip="Schema registration in AWS Glue per contract") as schema_group:
        register_tasks = PythonOperator.partial(
            task_id="register_schema",
            python_callable=task_register_schema,
        ).expand(
            op_args=validate_tasks.output.map(lambda x: [x])
        )

    # Step 4: Table creation task group
    # Create/update Iceberg tables per contract
    with TaskGroup("table_creation", tooltip="Iceberg table creation/update per contract") as table_group:
        create_table_tasks = PythonOperator.partial(
            task_id="create_table",
            python_callable=task_create_table,
        ).expand(
            op_args=validate_tasks.output.map(lambda x: [x])
        )

    # Step 5: Collect and aggregate results from all dynamic task instances
    collect_task = PythonOperator(
        task_id="aggregate_results",
        python_callable=task_collect_results,
        op_args=[
            validate_tasks.output,
            register_tasks.output,
            create_table_tasks.output,
        ],
        doc="Aggregate results from all per-contract processing tasks",
    )

    # Step 6: Report to GitHub
    github_task = PythonOperator(
        task_id="report_to_github",
        python_callable=task_report_to_github,
        doc="Report aggregated results to GitHub PR as comment",
    )

    fetch_task >> validate_tasks >> schema_group >> table_group >> collect_task >> github_task
