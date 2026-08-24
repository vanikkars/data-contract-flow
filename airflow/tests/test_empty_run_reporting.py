"""Regression tests: a run that processed zero contracts must never report success.

Root cause of the incident: the GitHub Actions workflow passed `changed_files`
in the DAG run's `conf`, but the DAG read it from an Airflow Variable. The value
resolved to None on every webhook-triggered run, so `fetch_contract_files`
processed nothing, all downstream tasks were skipped, and the PR comment
rendered "All 0 contracts validated successfully!" — green, for a change that
removed a required field.

Two independent defects, both covered here:
  1. Zero contracts rendered as success in every formatter (`x == total` where
     both sides are 0).
  2. `task_fetch_contracts` returned [] instead of failing.
"""

import pytest

from tasks.contract_tasks import ContractTasks
from tasks.github_tasks import GitHubTasks, NO_CONTRACTS_WARNING


@pytest.fixture
def empty_results():
    """Aggregated results for a run that processed nothing."""
    return ContractTasks.collect_results([], [], [])


@pytest.fixture
def passing_results():
    """Aggregated results for one contract that passed every stage."""
    return ContractTasks.collect_results(
        [{"contract_id": "users-v1", "name": "Users", "status": "valid"}],
        [{"contract_id": "users-v1", "schema_arn": "arn:aws:glue:x", "status": "registered"}],
        [{"contract_id": "users-v1", "table_name": "users", "status": "created",
          "columns_count": 3}],
    )


class TestEmptyRunIsNotSuccess:
    """The incident: 0 == 0 rendered as 'all passed'."""

    def test_validation_comment_does_not_claim_success(self, empty_results):
        comment = GitHubTasks.format_validation_comment(empty_results)

        assert "All 0 contracts validated successfully" not in comment
        assert "✅" not in comment

    def test_validation_comment_warns_explicitly(self, empty_results):
        comment = GitHubTasks.format_validation_comment(empty_results)

        assert "nothing was validated" in comment.lower()
        assert "NOT a passing result" in comment

    def test_schema_comment_does_not_claim_success(self, empty_results):
        comment = GitHubTasks.format_schema_comment(empty_results)

        assert "All 0 schemas registered successfully" not in comment
        assert "SQL-safety validation did not run" in comment

    def test_table_comment_does_not_claim_success(self, empty_results):
        comment = GitHubTasks.format_table_comment(empty_results)

        assert "All 0 tables processed successfully" not in comment

    def test_summary_withholds_approve_merge(self, empty_results):
        """The merge instruction must never appear for an unvalidated run."""
        comment = GitHubTasks.format_summary_comment(empty_results, 42)

        assert "/approve-merge" not in comment
        assert "Ready for Merge" not in comment

    def test_summary_states_pipeline_did_not_run(self, empty_results):
        comment = GitHubTasks.format_summary_comment(empty_results, 42)

        assert "Pipeline Did Not Run" in comment
        assert "not an approval" in comment

    def test_summary_action_required_is_not_empty(self, empty_results):
        """`0 < 0` was false, so the old Action Required block had no content."""
        comment = GitHubTasks.format_summary_comment(empty_results, 42)
        _, _, tail = comment.partition("---")

        assert len(tail.strip()) > 100

    def test_warning_text_discourages_merge(self):
        assert "Do not merge" in NO_CONTRACTS_WARNING


class TestNormalRunStillReportsSuccess:
    """The fix must not suppress genuine passes."""

    def test_passing_run_claims_success(self, passing_results):
        comment = GitHubTasks.format_validation_comment(passing_results)

        assert "All 1 contracts validated successfully" in comment

    def test_passing_run_offers_approve_merge(self, passing_results):
        comment = GitHubTasks.format_summary_comment(passing_results, 42)

        assert "/approve-merge" in comment
        assert "Ready for Merge" in comment

    def test_passing_run_has_no_empty_warning(self, passing_results):
        comment = GitHubTasks.format_summary_comment(passing_results, 42)

        assert "Pipeline Did Not Run" not in comment
        assert "nothing was validated" not in comment.lower()


class TestFetchContractFilesEmptyCases:
    """fetch_contract_files returns [] — the DAG task must treat that as fatal."""

    def test_no_changed_files_returns_empty(self, tmp_path):
        (tmp_path / "contracts" / "current").mkdir(parents=True)

        files = ContractTasks.fetch_contract_files(
            repo_path=str(tmp_path),
            contracts_dir="contracts/current",
            changed_files=None,
            process_all=False,
        )

        assert files == []

    def test_changed_files_are_resolved(self, tmp_path):
        current = tmp_path / "contracts" / "current" / "user"
        current.mkdir(parents=True)
        (current / "user_v1.json").write_text("{}")

        files = ContractTasks.fetch_contract_files(
            repo_path=str(tmp_path),
            contracts_dir="contracts/current",
            changed_files=["contracts/current/user/user_v1.json"],
            process_all=False,
        )

        assert len(files) == 1
        assert files[0].endswith("user_v1.json")

    def test_process_all_finds_every_contract(self, tmp_path):
        current = tmp_path / "contracts" / "current" / "user"
        current.mkdir(parents=True)
        (current / "a.json").write_text("{}")
        (current / "b.json").write_text("{}")

        files = ContractTasks.fetch_contract_files(
            repo_path=str(tmp_path),
            contracts_dir="contracts/current",
            changed_files=None,
            process_all=True,
        )

        assert len(files) == 2


class TestRunParameterPrecedence:
    """dag_run.conf must win over Variables — the actual root cause.

    Mirrors get_conf() from the DAG module, which cannot be imported here
    because parsing it requires an Airflow runtime.
    """

    @staticmethod
    def get_conf(context, key, default=None, variables=None):
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        if key in conf and conf[key] not in (None, ""):
            return conf[key]
        return (variables or {}).get(key, default)

    def _context(self, conf):
        from types import SimpleNamespace

        return {"dag_run": SimpleNamespace(conf=conf)}

    def test_conf_value_wins(self):
        ctx = self._context({"changed_files": "contracts/current/user/user_v1.json"})

        result = self.get_conf(ctx, "changed_files", variables={"changed_files": "stale"})

        assert result == "contracts/current/user/user_v1.json"

    def test_falls_back_to_variable(self):
        ctx = self._context({})

        result = self.get_conf(ctx, "changed_files", variables={"changed_files": "from_var"})

        assert result == "from_var"

    def test_empty_string_conf_falls_back(self):
        """The workflow sends '' when no files changed."""
        ctx = self._context({"changed_files": ""})

        assert self.get_conf(ctx, "changed_files", "DEFAULT") == "DEFAULT"

    def test_missing_dag_run_uses_default(self):
        assert self.get_conf({}, "changed_files", "DEFAULT") == "DEFAULT"

    def test_pr_number_read_from_conf(self):
        ctx = self._context({"github_pr_number": 42})

        assert self.get_conf(ctx, "github_pr_number") == 42