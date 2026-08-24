"""Airflow tasks for GitHub integration (PR comments, status checks)."""

import json
import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Rendered whenever a run processed zero contracts.
#
# "Nothing was checked" and "everything passed" are both `0 == 0`, so without an
# explicit guard every formatter reports success for a run that never validated
# anything. That turns the provisioning gate into a rubber stamp precisely when
# it failed to do its job.
NO_CONTRACTS_WARNING = (
    "⚠️ **No contracts were processed — nothing was validated.**\n\n"
    "This is NOT a passing result. The pipeline did not check any contract, so "
    "no statement can be made about whether the proposed changes are safe.\n\n"
    "Common causes:\n"
    "- The changed-files detection produced an empty list (check `git merge-base` "
    "resolution in the trigger workflow)\n"
    "- The DAG was triggered without `changed_files` and without `process_all`\n"
    "- The contracts directory path is misconfigured\n\n"
    "**Do not merge on the strength of this comment.**\n\n"
)


class GitHubTasks:
    """Tasks for GitHub integration."""

    @staticmethod
    def _no_contracts_processed(results: Dict[str, Any]) -> bool:
        """True when the run processed no contracts at all."""
        return results.get("total_contracts", 0) == 0

    @staticmethod
    def format_validation_comment(results: Dict[str, Any]) -> str:
        """Format validation results as GitHub markdown comment.

        Args:
            results: Aggregated results from all tasks

        Returns:
            Markdown string for GitHub comment
        """
        logger.info("📝 Formatting validation comment for GitHub")

        total = results.get("total_contracts", 0)
        validation_passed = results.get("validation_passed", 0)

        if GitHubTasks._no_contracts_processed(results):
            return "## ⚠️ Schema Validation Results\n\n" + NO_CONTRACTS_WARNING

        comment = "## ✅ Schema Validation Results\n\n"

        if validation_passed == total:
            comment += f"**All {total} contracts validated successfully!**\n\n"
        else:
            comment += f"**{validation_passed}/{total} contracts passed validation**\n\n"

        # Validation table
        all_results = results.get("all_results", {})
        validation_results = all_results.get("validation", [])

        if validation_results:
            comment += "| Contract | Status | Details |\n"
            comment += "|----------|--------|----------|\n"

            for result in validation_results:
                contract_id = result.get("contract_id", "N/A")
                name = result.get("name", "")
                status_icon = "✅" if result.get("status") == "valid" else "❌"

                comment += f"| {contract_id} ({name}) | {status_icon} | Validated |\n"

            comment += "\n"

        return comment

    @staticmethod
    def format_schema_comment(results: Dict[str, Any]) -> str:
        """Format schema registration results as GitHub markdown.

        Args:
            results: Aggregated results from all tasks

        Returns:
            Markdown string for GitHub comment
        """
        logger.info("📝 Formatting schema registration comment for GitHub")

        schema_registered = results.get("schema_registered", 0)
        total = results.get("total_contracts", 0)

        if GitHubTasks._no_contracts_processed(results):
            return (
                "## ⚠️ Schema Registration Results\n\n"
                "No schemas were registered — no contracts were processed. "
                "SQL-safety validation did not run.\n\n"
            )

        comment = "## 📋 Schema Registration Results\n\n"

        if schema_registered == total:
            comment += f"**All {total} schemas registered successfully!**\n\n"
        else:
            comment += f"**{schema_registered}/{total} schemas registered**\n\n"

        # Schema table
        all_results = results.get("all_results", {})
        schema_results = all_results.get("schema", [])

        if schema_results:
            comment += "| Contract | Schema ARN | Status |\n"
            comment += "|----------|-----------|--------|\n"

            for result in schema_results:
                contract_id = result.get("contract_id", "N/A")
                arn = result.get("schema_arn", "N/A")[:60] + "..." if len(result.get("schema_arn", "")) > 60 else result.get("schema_arn", "N/A")
                status_icon = "✅" if result.get("status") == "registered" else "❌"

                comment += f"| {contract_id} | `{arn}` | {status_icon} |\n"

            comment += "\n"

        return comment

    @staticmethod
    def format_table_comment(results: Dict[str, Any]) -> str:
        """Format Iceberg table creation results as GitHub markdown.

        Args:
            results: Aggregated results from all tasks

        Returns:
            Markdown string for GitHub comment
        """
        logger.info("📝 Formatting table creation comment for GitHub")

        tables_created = results.get("tables_created", 0)
        tables_updated = results.get("tables_updated", 0)
        total = results.get("total_contracts", 0)

        if GitHubTasks._no_contracts_processed(results):
            return (
                "## ⚠️ Iceberg Table Results\n\n"
                "No tables were created or updated — no contracts were processed.\n\n"
            )

        comment = "## 🗄️ Iceberg Table Results\n\n"

        if tables_created + tables_updated == total:
            comment += f"**All {total} tables processed successfully!**\n\n"
        else:
            comment += f"**{tables_created + tables_updated}/{total} tables processed**\n"
            comment += f"({tables_created} created, {tables_updated} updated)\n\n"

        # Table status table
        all_results = results.get("all_results", {})
        table_results = all_results.get("table", [])

        if table_results:
            comment += "| Contract | Table Name | Status | Columns |\n"
            comment += "|----------|-----------|--------|----------|\n"

            for result in table_results:
                contract_id = result.get("contract_id", "N/A")
                table_name = result.get("table_name", "N/A")
                status_value = result.get("status", "unknown")

                status_icons = {
                    "created": "✅ CREATED",
                    "updated": "✏️ UPDATED",
                    "unchanged": "✔️ UNCHANGED",
                }
                status_display = status_icons.get(status_value, f"❓ {status_value.upper()}")

                columns_count = result.get("columns_count", "?")

                comment += f"| {contract_id} | `{table_name}` | {status_display} | {columns_count} |\n"

            comment += "\n"

        return comment

    @staticmethod
    def format_summary_comment(results: Dict[str, Any], github_pr_number: Optional[int] = None) -> str:
        """Format complete summary comment for GitHub PR.

        Args:
            results: Aggregated results from all tasks
            github_pr_number: GitHub PR number (optional, for reference)

        Returns:
            Complete markdown string for GitHub PR comment
        """
        logger.info(f"📝 Formatting summary comment for GitHub PR #{github_pr_number}")

        comment = "## 📊 Contract Provisioning Summary\n\n"

        if github_pr_number:
            comment += f"_Triggered by: PR #{github_pr_number}_\n\n"

        # Get individual comments
        validation_comment = GitHubTasks.format_validation_comment(results)
        schema_comment = GitHubTasks.format_schema_comment(results)
        table_comment = GitHubTasks.format_table_comment(results)

        comment += validation_comment
        comment += schema_comment
        comment += table_comment

        # Add approval request if everything passed
        total = results.get("total_contracts", 0)
        validation_passed = results.get("validation_passed", 0)
        schema_registered = results.get("schema_registered", 0)
        tables_created_updated = results.get("tables_created", 0) + results.get("tables_updated", 0)

        if (validation_passed == total and schema_registered == total and
                tables_created_updated == total and total > 0):
            comment += "---\n\n"
            comment += "## ✅ Ready for Merge\n\n"
            comment += "All validations passed! Reply with:\n\n"
            comment += "```\n/approve-merge\n```\n\n"
            comment += "This will automatically merge the PR with all changes.\n"
        elif total == 0:
            comment += "---\n\n"
            comment += "## 🛑 Pipeline Did Not Run\n\n"
            comment += (
                "Zero contracts were processed, so **no validation took place**. "
                "This is a pipeline failure, not an approval.\n\n"
                "Investigate why no contracts reached the DAG before merging "
                "anything in this PR.\n"
            )
        else:
            comment += "---\n\n"
            comment += "## ⚠️ Action Required\n\n"
            if validation_passed < total:
                comment += "- Fix validation errors in contracts\n"
            if schema_registered < total:
                comment += "- Schema registration failed\n"
            if tables_created_updated < total:
                comment += "- Iceberg table creation failed\n"
            comment += "\nCheck the details above and update contracts as needed.\n"

        return comment

    @staticmethod
    def get_pr_comment_body(results: Dict[str, Any], github_pr_number: Optional[int] = None) -> Dict[str, Any]:
        """Get GitHub API payload for PR comment.

        Args:
            results: Aggregated results from all tasks
            github_pr_number: GitHub PR number

        Returns:
            Dictionary ready for GitHub API call
        """
        logger.info(f"🔗 Preparing GitHub API payload for PR #{github_pr_number}")

        body = GitHubTasks.format_summary_comment(results, github_pr_number)

        return {
            "body": body,
            "pr_number": github_pr_number,
        }

    @staticmethod
    def log_results_for_github(results: Dict[str, Any]) -> None:
        """Log results in format suitable for GitHub Actions output.

        Args:
            results: Aggregated results from all tasks
        """
        logger.info("📤 GitHub Actions Output:")
        logger.info(f"  Total Contracts: {results.get('total_contracts')}")
        logger.info(f"  Validated: {results.get('validation_passed')}")
        logger.info(f"  Schemas Registered: {results.get('schema_registered')}")
        logger.info(f"  Tables Created: {results.get('tables_created')}")
        logger.info(f"  Tables Updated: {results.get('tables_updated')}")

        # Export for Airflow XCom
        return {
            "validation_passed": results.get("validation_passed"),
            "schema_registered": results.get("schema_registered"),
            "tables_created": results.get("tables_created"),
            "tables_updated": results.get("tables_updated"),
            "github_pr_comment": GitHubTasks.format_summary_comment(results),
        }
