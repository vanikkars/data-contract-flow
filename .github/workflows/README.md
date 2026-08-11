# GitHub Actions Workflows

This directory contains GitHub Actions workflows for contract provisioning automation.

## Workflows

### `trigger-airflow-dag.yml` (Main - Active)
**Purpose:** Trigger Airflow DAG for contract validation and provisioning

**Trigger:** Pull request when `contracts/current/**` changes

**Flow:**
1. Detect changed contract files
2. Trigger Airflow DAG (`contract_provisioning`)
3. Wait for DAG completion (with 10-minute timeout)
4. Fetch results from DAG execution
5. Post results as PR comment
6. Set GitHub check status (success/failure)

**Requirements:**
- Airflow deployed and accessible
- GitHub secrets configured (see below)

### Deprecated

- `contract-validation-workflow.yml` - Old workflow using direct HTTP calls (kept for reference)
- `approve-merge.yml` - Auto-merge command handler (unchanged)

## GitHub Secrets Configuration

To use the Airflow DAG workflow, configure these secrets in your GitHub repository:

**Settings → Secrets and Variables → Actions**

### Required Secrets

1. **`AIRFLOW_URL`**
   - Value: `https://airflow.example.com` (or localhost for testing)
   - Used to: Trigger DAG and fetch results
   - Format: Complete URL, no trailing slash
   - Example: `https://airflow.mycompany.com`

2. **`AIRFLOW_USERNAME`**
   - Value: Airflow UI login username
   - Used to: Authenticate with Airflow API
   - Example: `admin`

3. **`AIRFLOW_PASSWORD`**
   - Value: Airflow UI login password
   - Used to: Authenticate with Airflow API
   - Note: Use a service account or strong password

### Optional Secrets

- `GITHUB_TOKEN` - Automatically provided by GitHub Actions, no need to configure

## Setting Up Secrets

### Via GitHub Web UI

1. Go to: Settings → Secrets and Variables → Actions
2. Click "New repository secret"
3. Add each secret with its value
4. Click "Add secret"

### Via GitHub CLI

```bash
gh secret set AIRFLOW_URL --body "https://airflow.example.com"
gh secret set AIRFLOW_USERNAME --body "admin"
gh secret set AIRFLOW_PASSWORD --body "your-secure-password"
```

### Via Terraform (IaC)

```hcl
resource "github_actions_secret" "airflow_url" {
  repository      = "your-repo"
  secret_name     = "AIRFLOW_URL"
  plaintext_value = "https://airflow.example.com"
}
```

## Workflow Behavior

### When Contracts Change

The workflow automatically triggers when:
- Files in `contracts/current/**` are modified
- PR is opened, synchronized (new commits), or reopened
- Workflow file itself is changed

### What the Workflow Does

1. **Detect Changes**
   - Compares PR branch against base branch (main/master)
   - Lists all changed *.json files in contracts/current

2. **Trigger Airflow DAG**
   - Makes HTTP POST to Airflow API
   - Passes GitHub context:
     - PR number
     - Repository name
     - GitHub token (for Airflow to comment back)
   - Receives DAG run ID

3. **Poll for Completion**
   - Checks DAG status every 5 seconds
   - Timeout: 10 minutes
   - Possible states:
     - `running` - Still executing
     - `success` - Completed successfully
     - `failed` - DAG failed
     - `upstream_failed` - Task dependency failed
     - `skipped` - No changes to process

4. **Fetch Results**
   - Retrieves DAG execution results from Airflow XCom
   - Gets formatted comment body
   - Falls back to minimal message if fetch fails

5. **Post PR Comment**
   - Posts detailed results comment
   - Shows validation status
   - Shows schema registration status
   - Shows table creation status
   - Includes approval instructions if all passed

6. **Set Check Status**
   - Creates GitHub check with result
   - Shows in PR UI as pass/fail
   - Blocks merge if configured as required check

## Environment Variables

Set in workflow or Airflow environment:

```yaml
AIRFLOW_DAG_ID: contract_provisioning      # DAG ID to trigger
AIRFLOW_TIMEOUT: 600                       # 10 minutes in seconds
```

## Airflow API Endpoints Used

The workflow uses these Airflow API endpoints:

1. **Trigger DAG**
   ```
   POST /api/v1/dags/{dag_id}/dagRuns
   ```
   - Body: JSON with conf (GitHub context)
   - Response: DAG run details with run_id

2. **Get DAG Run Status**
   ```
   GET /api/v1/dags/{dag_id}/dagRuns/{run_id}
   ```
   - Response: Current run state
   - Possible states: running, success, failed, skipped, etc.

3. **Fetch XCom Value**
   ```
   GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries?key={key}
   ```
   - Gets formatted comment from DAG
   - Used to post results on PR

## Testing the Workflow

### Local Testing

1. **Start Airflow locally**
   ```bash
   docker-compose -f docker-compose.airflow.yml up
   ```

2. **Configure secrets locally** (in .env or GitHub Codespaces)
   ```bash
   AIRFLOW_URL=http://localhost:8080
   AIRFLOW_USERNAME=admin
   AIRFLOW_PASSWORD=admin
   ```

3. **Trigger workflow manually**
   ```bash
   # Via GitHub CLI
   gh workflow run trigger-airflow-dag.yml -f ref=your-branch

   # Or create a PR with contract changes
   git checkout -b test/add-contract
   # ... make contract changes ...
   git push origin test/add-contract
   # Create PR on GitHub
   ```

4. **Monitor**
   - GitHub: Check workflow run in Actions tab
   - Airflow: View DAG run at http://localhost:8080
   - PR: Check for comment with results

### CI/CD Testing

1. Deploy Airflow to staging environment
2. Set GitHub secrets to staging Airflow URL
3. Create test PR with contract changes
4. Verify workflow completes and comments on PR

## Troubleshooting

### Workflow Fails on DAG Trigger

**Error:** `Failed to trigger DAG`

**Causes:**
- Airflow URL is incorrect or unreachable
- Username/password are wrong
- DAG doesn't exist in Airflow

**Fix:**
```bash
# Test connectivity
curl -u username:password https://airflow.example.com/api/v1/health

# Verify DAG exists
curl -u username:password https://airflow.example.com/api/v1/dags/contract_provisioning
```

### Workflow Timeout

**Error:** `DAG execution timed out after 600 seconds`

**Causes:**
- DAG is taking longer than 10 minutes
- One or more tasks are hanging
- Network issues between GitHub and Airflow

**Fix:**
- Increase `AIRFLOW_TIMEOUT` in workflow
- Check Airflow logs for hanging tasks
- Verify network connectivity

### No Comment Posted on PR

**Error:** No comment appears on PR after DAG completes

**Causes:**
- XCom fetch failed (network/auth issue)
- DAG didn't set XCom value
- Comment body is empty

**Fix:**
- Check Airflow logs for task errors
- Verify XCom is being set in collect_and_format_results task
- Use fallback comment (shows run ID and link)

### GitHub Secret Not Found

**Error:** `AIRFLOW_URL secret not configured`

**Causes:**
- Secret hasn't been added to repository
- Secret name is incorrect (case-sensitive)
- Secret is org-level, not repo-level

**Fix:**
```bash
# Add missing secret
gh secret set AIRFLOW_URL --body "https://airflow.example.com"

# Verify it exists
gh secret list
```

## Security Considerations

1. **Passwords in Secrets**
   - Use strong passwords for service accounts
   - Consider rotating periodically
   - Don't commit secrets to git

2. **API Authentication**
   - Airflow API uses HTTP Basic Auth
   - Consider using HTTPS in production
   - Restrict Airflow access to GitHub Actions IPs

3. **GitHub Token**
   - Automatically available as `${{ secrets.GITHUB_TOKEN }}`
   - Passed to Airflow for DAG context
   - Has limited scope (repo-specific)

4. **Logs**
   - Workflow logs are visible in GitHub UI
   - Airflow logs are in Airflow only
   - Avoid logging sensitive data

## Migration from Old Workflow

If using the old `contract-validation-workflow.yml`:

1. **Keep old workflow** for backward compatibility (or disable it)
2. **Enable new workflow** in GitHub UI
3. **Test** with a PR on a test branch
4. **Monitor** for any issues
5. **Remove old workflow** once confident in new one

Old workflow will still:
- Validate contracts locally
- Call HTTP endpoints (if configured)
- Provide basic validation checks

New workflow:
- Delegates all logic to Airflow DAG
- Provides better monitoring and logs
- Supports complex provisioning workflows

## Future Enhancements

1. **Approval Workflow**
   - Require manual approval before merge
   - Airflow can trigger approval via GitHub

2. **Rollback Support**
   - Ability to revert schema/table changes
   - DAG can execute rollback tasks

3. **Multiple Environments**
   - Dev/staging/prod Airflow instances
   - Different workflows for each env

4. **Performance Metrics**
   - Track provisioning time per contract
   - Report metrics to monitoring system

5. **Slack Notifications**
   - Send DAG results to Slack channel
   - Alert on failures

## Configuration Examples

### Production Setup

```yaml
env:
  AIRFLOW_DAG_ID: contract_provisioning
  AIRFLOW_TIMEOUT: 1200  # 20 minutes for large batches
```

### Development Setup

```yaml
env:
  AIRFLOW_DAG_ID: contract_provisioning_dev
  AIRFLOW_TIMEOUT: 300  # 5 minutes for quick feedback
```

### Staging with Approval

```yaml
# Add approval step before merge
  - name: Request Approval
    if: steps.wait-dag.outputs.state == 'success'
    uses: actions/github-script@v7
    with:
      script: |
        await github.rest.issues.createComment({
          issue_number: context.issue.number,
          owner: context.repo.owner,
          repo: context.repo.repo,
          body: 'All checks passed! Reply with `/approve-merge` to auto-merge.'
        })
```

## References

- [Airflow REST API Docs](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [GitHub Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
