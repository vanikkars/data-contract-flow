# Airflow + GitHub Actions Setup Guide

Quick setup guide for integrating Airflow DAG with GitHub Actions workflow.

## Prerequisites

1. ✅ Airflow deployed and running
2. ✅ `contract_provisioning` DAG created (from Phase 2)
3. ✅ GitHub repository with permissions to manage secrets
4. ✅ Airflow API accessible from GitHub (if not localhost)

## Step 1: Verify Airflow Setup

### Check Airflow is Running

```bash
# If using docker-compose
docker-compose -f docker-compose.airflow.yml up

# Verify UI is accessible
curl http://localhost:8080/api/v1/health

# Should return:
# {"status":"healthy"}
```

### Verify DAG Exists

```bash
# Check DAG is loaded
curl -u admin:admin http://localhost:8080/api/v1/dags/contract_provisioning

# Should return DAG details with dag_id: contract_provisioning
```

### Create DAG Run Manually (Test)

```bash
# Trigger DAG manually to verify it works
curl -X POST \
  http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "conf": {
      "github_pr_number": 1,
      "github_repo": "test/repo",
      "github_token": "test-token"
    }
  }'

# Should return with dag_run_id
```

## Step 2: Configure GitHub Secrets

### Via GitHub Web UI

1. Go to: **Repository Settings → Secrets and Variables → Actions**
2. Click **New repository secret**
3. Add these secrets:

| Secret Name | Value | Example |
|-------------|-------|---------|
| `AIRFLOW_URL` | Full Airflow URL | `https://airflow.mycompany.com` |
| `AIRFLOW_USERNAME` | Airflow login user | `admin` |
| `AIRFLOW_PASSWORD` | Airflow login password | `your-secure-password` |

**For Local Testing:**
```
AIRFLOW_URL: http://localhost:8080
AIRFLOW_USERNAME: admin
AIRFLOW_PASSWORD: admin
```

### Via GitHub CLI

```bash
# Install GitHub CLI if not already installed
# macOS: brew install gh
# Linux: sudo apt install gh
# Windows: choco install gh

# Login to GitHub
gh auth login

# Add secrets
gh secret set AIRFLOW_URL --body "http://localhost:8080"
gh secret set AIRFLOW_USERNAME --body "admin"
gh secret set AIRFLOW_PASSWORD --body "admin"

# Verify
gh secret list
```

## Step 3: Enable Workflow

The workflow is defined in: `.github/workflows/trigger-airflow-dag.yml`

### Verify Workflow is Visible

1. Go to: **Actions** tab in GitHub
2. You should see: **Trigger Airflow DAG for Contract Provisioning**
3. Status should show as active (not disabled)

### If Workflow Doesn't Appear

```bash
# Git pull latest changes
git pull origin

# Verify workflow file exists
ls -la .github/workflows/trigger-airflow-dag.yml

# Push to remote if not synced
git push origin main
```

## Step 4: Test the Integration

### Test 1: Trigger from GitHub Actions UI

1. Go to: **Actions** tab
2. Click: **Trigger Airflow DAG for Contract Provisioning**
3. Click: **Run workflow** (or use dropdown for specific branch)
4. Wait for workflow to complete

**Expected Results:**
- Workflow status shows success/failure
- Check runs show contract provisioning result
- DAG run appears in Airflow UI

### Test 2: Trigger via Contract Change (Real PR)

1. Create a test branch:
   ```bash
   git checkout -b test/airflow-integration
   ```

2. Add a new contract file or modify existing one:
   ```bash
   # Create or edit file in contracts/current/
   echo '{"contract_id": "test-v1", "name": "Test", "description": "Test contract", "columns": [{"name": "id", "data_type": "string"}]}' > contracts/current/test/01/test.json
   ```

3. Commit and push:
   ```bash
   git add contracts/current/test/01/test.json
   git commit -m "Test: Add test contract for Airflow integration"
   git push origin test/airflow-integration
   ```

4. Create Pull Request on GitHub

5. Monitor:
   - **GitHub:** Check workflow run in Actions tab
   - **Airflow:** View DAG run at http://localhost:8080
   - **PR:** Check for comment with results

### Test 3: Verify PR Comment

After DAG completes, you should see a comment on the PR showing:
- ✅ Validation results
- 📋 Schema registration status
- 🗄️ Iceberg table creation status

## Step 5: Production Deployment

### Deploy Airflow to Production

Choose your deployment method:

#### Option A: AWS ECS

```bash
# Build Airflow image
docker build -t my-airflow:latest -f Dockerfile.airflow .

# Push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker tag my-airflow:latest <account>.dkr.ecr.<region>.amazonaws.com/my-airflow:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/my-airflow:latest

# Deploy to ECS (using task definition)
aws ecs update-service --cluster production --service airflow --force-new-deployment
```

#### Option B: Kubernetes

```bash
# Create namespace
kubectl create namespace airflow

# Deploy using Helm
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values values.yaml

# Get service URL
kubectl get service airflow-webserver -n airflow
```

#### Option C: AWS AppRunner

```bash
# Push image to ECR
aws apprunner create-service \
  --service-name airflow-schema-registry \
  --source-configuration ImageRepository={ImageIdentifier=<ecr-uri>,ImageRepositoryType=ECR}
```

### Configure GitHub Secrets for Production

```bash
# Update secrets with production URL
gh secret set AIRFLOW_URL --body "https://airflow-prod.mycompany.com"
gh secret set AIRFLOW_USERNAME --body "service-account-username"
gh secret set AIRFLOW_PASSWORD --body "service-account-password"
```

### Enable Required Status Checks

1. Go to: **Settings → Branches → Branch Protection Rules**
2. Add protection rule for `main` branch
3. Require: **Contract Provisioning** check to pass
4. Require: Pull request reviews (optional)
5. Require: Status checks to pass before merge

## Step 6: Monitor and Troubleshoot

### Workflow Logs

**GitHub Actions:**
1. Go to: **Actions** tab
2. Click on workflow run
3. Click on job to see logs
4. Look for: Airflow API calls, DAG trigger response, polling status

**Airflow:**
1. Go to: http://airflow-url/dags/contract_provisioning/runs
2. Click on DAG run
3. View task logs for each task
4. Check XCom values for data between tasks

### Common Issues

#### Issue: Workflow Fails on Trigger

**Error in logs:**
```
Failed to trigger DAG
Response: 404 Not Found
```

**Fix:**
1. Verify AIRFLOW_URL is correct
2. Check DAG exists: `curl -u admin:admin http://url/api/v1/dags/contract_provisioning`
3. Verify DAG is in "active" state (not paused)

#### Issue: Workflow Timeout

**Error in logs:**
```
DAG execution timed out after 600 seconds
```

**Fix:**
1. Increase timeout in workflow (change `AIRFLOW_TIMEOUT`)
2. Check Airflow logs for slow tasks
3. Verify AWS credentials in Airflow environment

#### Issue: No Comment Posted

**Error in logs:**
```
Could not extract comment body from XCom
```

**Fix:**
1. Check DAG completed successfully
2. Verify `collect_and_format_results` task ran
3. Check Airflow logs for task failures
4. Fallback message should still be posted

### Monitoring Dashboard

Create a monitoring dashboard in Airflow:

1. Go to: **Admin → XCom**
2. View all XCom values from DAG runs
3. Check results posted to GitHub

In GitHub:
1. Go to: **Actions → Contract Provisioning Runs**
2. Filter by status (success/failure)
3. Review trend over time

## Verification Checklist

- [ ] Airflow deployed and running
- [ ] `contract_provisioning` DAG visible in Airflow UI
- [ ] GitHub secrets configured (AIRFLOW_URL, USERNAME, PASSWORD)
- [ ] Workflow file exists: `.github/workflows/trigger-airflow-dag.yml`
- [ ] Manual DAG trigger works in Airflow
- [ ] Workflow runs successfully with test PR
- [ ] PR comment posted with results
- [ ] Check run status shows pass/fail
- [ ] Production Airflow deployment complete
- [ ] Production GitHub secrets updated
- [ ] Branch protection rules configured
- [ ] Team knows how to troubleshoot

## Next Steps

1. **Enable for all branches** (not just main)
2. **Add email notifications** on failures
3. **Create Slack alerts** for failed provisioning
4. **Set up monitoring** dashboard
5. **Document** for team (link to this guide)
6. **Train team** on workflow and troubleshooting

## Quick Reference

### Workflow Triggers
- PR created with contract changes
- PR synchronized (new commits)
- PR reopened
- Manual trigger from Actions UI

### Workflow Steps
1. Detect changed contracts
2. Trigger Airflow DAG
3. Poll for completion (10 min timeout)
4. Fetch results from DAG
5. Post PR comment
6. Set check status

### Key URLs
- Airflow UI: `AIRFLOW_URL` (configured in secrets)
- DAG Runs: `AIRFLOW_URL/dags/contract_provisioning/runs`
- GitHub Actions: `https://github.com/YOUR-REPO/actions`
- Workflow Logs: Click workflow run in Actions tab

## Support

For issues or questions:

1. **Check workflow logs** in GitHub Actions
2. **Check Airflow logs** in Airflow UI
3. **Check this guide** for troubleshooting steps
4. **Review DAG README** at `dags/README.md`
5. **Open GitHub issue** with details

---

**Setup should take ~15 minutes. You're ready to go!** 🚀
