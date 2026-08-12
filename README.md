# Data Contract Flow

A contract-driven data pipeline orchestration system using Apache Airflow, AWS Glue Schema Registry, and Iceberg tables. Automatically validates, registers, and provisions data contracts through a scalable DAG-based workflow.

## Quick Start

```bash
# 1. Setup environment
cp .env.example .env
source .env

# Add GitHub integration (optional, for PR comments)
export GITHUB_REPO=vanikkars/data-contract-flow
export GITHUB_TOKEN=ghp_your_token_here

# 2. Start Airflow
make airflow-up

# 3. Access Airflow UI
open http://localhost:8080
# Default credentials: admin / admin

# 4. Trigger the DAG
make airflow-trigger
```

## What It Does

The `contract_provisioning` DAG orchestrates a complete contract lifecycle:

1. **Fetch Contracts** — Discover contract files in `contracts/current/`
2. **Validate** — Verify JSON schema, required fields, and data types
3. **Register Schemas** — Create AVRO schemas in AWS Glue Schema Registry
4. **Create Tables** — Provision Iceberg tables in AWS Glue Catalog
5. **Report Results** — Comment on GitHub PRs with provisioning status

## Architecture

```
Airflow 3.3.0
├── contract_provisioning (DAG)
│   ├── fetch_contracts
│   ├── validate_contracts
│   ├── register_schemas (parallel)
│   ├── create_iceberg_tables (parallel)
│   ├── collect_and_format_results
│   └── report_to_github
│
├── PostgreSQL (metadata store)
└── LocalExecutor (task execution)

AWS Services
├── Glue Schema Registry
└── Glue Catalog (Iceberg tables)

GitHub Actions
└── Triggers DAG when contracts/current/** changes
```

## Project Structure

```
airflow/
├── dags/
│   ├── contract_provisioning_dag.py    # Main DAG definition
│   └── README.md                       # DAG documentation
├── tasks/
│   ├── contract_tasks.py               # Contract validation & provisioning logic
│   └── github_tasks.py                 # GitHub integration
├── lib/
│   ├── aws_glue.py                     # AWS Glue SDK wrapper
│   ├── models.py                       # Data models
│   ├── validator.py                    # Schema validation
│   ├── converters.py                   # Contract converters
│   └── exceptions.py                   # Custom exceptions
└── docker/
    └── airflow-entrypoint.sh           # Airflow startup script

contracts/
├── current/                            # Active contracts (triggers DAG)
│   ├── user/01/user.json
│   ├── product/01/product.json
│   └── ...
└── all/                                # Historical contracts

scripts/
├── validate-contracts.py               # Standalone validation tool
└── convert_to_avro.py                  # Contract to AVRO converter

infra/
├── aws/                                # Terraform for AWS resources
└── README.md                           # Infrastructure docs
```

## Commands

### Airflow Management

```bash
make airflow-up           # Start Airflow services (3.3.0)
make airflow-down         # Stop Airflow services
make airflow-logs         # View Airflow logs
make airflow-ui           # Open Airflow UI (http://localhost:8080)
make airflow-trigger      # Trigger contract_provisioning DAG manually
```

### Contract Validation

```bash
make validate-contracts        # Validate contracts/current/
make validate-contracts-all    # Validate contracts/all/
make validate-export           # Validate and export results to JSON
make validate-export-all       # Validate all and export results
make validate-remote           # Validate against remote API
```

### Utilities

```bash
make bore-start       # Start Bore tunnels (GitHub Actions integration)
make bore-stop        # Stop Bore tunnels
make check-env        # Verify .env configuration
make check-aws        # Check AWS credentials
make info             # Show Airflow project info
```

## Environment Variables

### Required (AWS)
```bash
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
```

### Optional (GitHub Integration)

**Important:** Keep GitHub tokens separate for security!

```bash
# For Airflow to comment on PRs (Project Token)
GITHUB_REPO=owner/repo                           # e.g., vanikkars/data-contract-flow
GITHUB_TOKEN=github_pat_your_airflow_token       # Fine-grained token for Airflow

# For Claude Code with GitHub integration (Claude's Token)
ANTHROPIC_GITHUB_TOKEN=github_pat_your_claude_token  # Fine-grained token for Claude Code
```

**Why separate tokens?**
- Security: Each tool only has the permissions it needs
- Auditing: Track which token is used by which tool
- Revocation: Disable Claude's access without breaking Airflow
- Best practices: Follow principle of least privilege

### Terraform
```bash
TF_VAR_registry_name=schema-registry
```

## Creating a GitHub Personal Access Token

To enable Airflow to comment on pull requests, you need a GitHub Fine-Grained Personal Access Token (recommended) or a Classic token.

### Step-by-Step Guide (Fine-Grained Token - Recommended)

Fine-grained tokens are more secure with granular permissions per repository.

1. **Go to GitHub Settings**
   - Click your profile icon (top-right corner)
   - Select **Settings**
   - Scroll down and click **Developer settings** (left sidebar)
   - Click **Personal access tokens** → **Fine-grained tokens**

2. **Create a New Token**
   - Click **Generate new token**
   - Give it a descriptive name: `airflow-contract-provisioning`
   - Set expiration (recommended: 30-90 days)
   - Select repository access: **Only select repositories**
   - Choose: `data-contract-flow` (or your specific repo)

3. **Select Repository Permissions**
   
   Under **Repository permissions**, enable:
   - ✅ **Pull requests** → `Read and write` (for commenting on PRs)
   - ✅ **Contents** → `Read-only` (optional, to read contract files)
   
   That's it! Fine-grained tokens are more restrictive by default.

4. **Copy and Save the Token**
   - GitHub will display the token once: `github_pat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx`
   - **Copy it immediately** (you won't see it again)
   - Store it securely (password manager, CI/CD secrets, 1Password, etc.)

5. **Add to Your Environment**
   ```bash
   # Add to .env file
   export GITHUB_TOKEN=github_pat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   export GITHUB_REPO=vanikkars/data-contract-flow
   ```

6. **Load Environment**
   ```bash
   source .env
   ```

### Alternative: Classic Personal Access Token

If you need a classic token for backward compatibility:

1. **Go to GitHub Settings**
   - Click your profile icon (top-right corner)
   - Select **Settings**
   - Scroll down and click **Developer settings** (left sidebar)
   - Click **Personal access tokens** → **Tokens (classic)**

2. **Create a New Token**
   - Click **Generate new token (classic)**
   - Give it a descriptive name: `airflow-contract-provisioning`
   - Set expiration (e.g., 90 days)

3. **Select Scopes**
   - ✅ `repo` (full control of private repositories)
   - ✅ `workflow` (optional, for GitHub Actions)

4. **Copy token and add to .env**
   ```bash
   export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
   ```

### Managing Multiple GitHub Tokens

This project uses **two separate GitHub tokens** for better security:

| Token | Purpose | Permissions | Used By |
|-------|---------|-------------|---------|
| `GITHUB_TOKEN` | Airflow PR comments | `Pull requests` (R/W), `Contents` (R) | Airflow DAG |
| `ANTHROPIC_GITHUB_TOKEN` | Claude Code integration | `Pull requests` (R/W), `Contents` (R) | Claude Code |

**Benefits of separation:**
- 🔒 **Security**: Each tool only gets needed permissions
- 📊 **Auditing**: Track token usage per tool
- ⚙️ **Control**: Revoke one without affecting the other
- 🛡️ **Best Practice**: Principle of least privilege

### Creating Tokens for Both Tools

**Step 1: Create GITHUB_TOKEN (for Airflow)**
- See section above "Step-by-Step Guide (Fine-Grained Token)"
- Name: `airflow-contract-provisioning`
- Permissions:
  - ✅ Pull requests → `Read and write`
  - ✅ Contents → `Read-only`

**Step 2: Create ANTHROPIC_GITHUB_TOKEN (for Claude)**
- Follow same steps, create another fine-grained token
- Name: `claude-code-github-integration`
- Same permissions (Pull requests R/W, Contents R)
- Store separately in `.env`

**Step 3: Update .env**
```bash
# Airflow's GitHub token
export GITHUB_TOKEN=github_pat_airflow_xxxxx

# Claude Code's GitHub token
export ANTHROPIC_GITHUB_TOKEN=github_pat_claude_xxxxx

# Repository info (shared)
export GITHUB_REPO=vanikkars/data-contract-flow
```

### Troubleshooting Token Issues

**Token not working:**
```bash
# Test Airflow token
curl -H "Authorization: token $GITHUB_TOKEN" \
  https://api.github.com/user

# Test Claude token
curl -H "Authorization: token $ANTHROPIC_GITHUB_TOKEN" \
  https://api.github.com/user
```

**Fine-grained token insufficient permissions:**
- Go to token settings on GitHub
- Add **Pull requests** → `Read and write`
- Add **Contents** → `Read-only` (if reading contracts)

**Token expired:**
- Go back to Personal Access Tokens (Fine-grained)
- Delete the old token
- Create a new one with fresh expiration
- Update `.env` with new token

**Token exposed in git:**
```bash
# Revoke immediately at GitHub Settings
# Go to Personal access tokens
# Click the token → Delete
# Create a new token
# Update .env and run: source .env
```

**Airflow not commenting on PRs:**
- Verify `GITHUB_TOKEN` is set correctly
- Check Airflow logs: `make airflow-logs`
- Ensure token has `Pull requests` → `Read and write`
- Note: PR comment feature is commented out in DAG (line 281-284)

**Claude Code not accessing GitHub:**
- Verify `ANTHROPIC_GITHUB_TOKEN` is set
- Check Claude Code MCP configuration
- Ensure token has required permissions
- Test with curl command above

**Classic token limitations:**
- Grants access to all your repositories
- Broader scope than necessary
- Use fine-grained tokens when possible

## Airflow Setup & Credentials

### Default Admin User

When Airflow 3.3.0 starts for the first time, it creates a default admin user. However, Airflow 3.3.0 uses `SimpleAuthManager` in standalone mode which **generates a random password** for security reasons.

**To find your password:**

1. Check the Airflow container logs:
```bash
docker logs airflow-webserver | grep "Password for user"
# Output: Password for user 'admin': xxxxxxxxxxxx
```

2. Or set a custom password (see section below)

**Access Airflow UI:**
```bash
open http://localhost:8080
# Login with: admin / <random-password-from-logs>
```

### Setting a Custom Admin Password

**Option 1: Via Airflow UI (after logging in)**

1. Get the random password: `docker logs airflow-webserver | grep "Password for user"`
2. Go to http://localhost:8080 and login
3. Click **Admin** (top menu) → **Users**
4. Click the pencil icon next to `admin`
5. Change password and save

**Option 2: Via Command Line (before or after starting)**

```bash
# Reset admin password to a custom value
docker exec airflow-webserver airflow users delete admin --yes 2>/dev/null || true
docker exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password yourpassword123
```

**Option 3: Disable Random Password Generation**

To prevent Airflow from generating random passwords, set this in the entrypoint before starting Airflow:

```bash
# In docker-compose.airflow.yml, add to airflow service environment:
AIRFLOW__API_FASTAPI__SIMPLE_AUTH_MANAGER__GENERATE_PASSWORDS: "false"
```

Then restart Airflow and use the password you set in the entrypoint script.

### Default Connections

Airflow automatically creates:

| Connection | Type | Details |
|-----------|------|---------|
| `aws_default` | AWS | Uses `AWS_*` environment variables from `.env` |

### Security Notes

⚠️ **For Production:**
- Change `AIRFLOW__WEBSERVER__SECRET_KEY` in docker-compose.airflow.yml
- Use strong admin password
- Configure authentication (OAuth, LDAP, etc.)
- Use encrypted connections
- Restrict network access

## Configuration

### Airflow Variables (set in Airflow UI → Admin → Variables)

```
Key: repo_path
Value: /app

Key: contracts_dir
Value: contracts/current

Key: github_token
Value: your-github-token

Key: github_repo
Value: owner/repo

Key: github_pr_number
Value: (set dynamically by GitHub Actions)
```

## How to Use

### Adding a New Contract

1. Create contract file: `contracts/current/<domain>/<version>/<name>.json`
2. Push to GitHub (or add locally)
3. DAG automatically triggers on `contracts/current/**` changes
4. Airflow validates, registers, and creates tables
5. Check results in Airflow UI or GitHub PR comments

### Running Locally

```bash
# Start Airflow
make airflow-up

# Open UI
make airflow-ui

# Create a contract
mkdir -p contracts/current/users/01
cat > contracts/current/users/01/users.json << 'EOF'
{
  "contract_id": "users-v1",
  "name": "Users",
  "description": "User account information",
  "columns": [
    {"name": "id", "data_type": "string"},
    {"name": "email", "data_type": "string"},
    {"name": "created_at", "data_type": "timestamp"}
  ]
}
EOF

# Trigger DAG
make airflow-trigger

# Watch in UI at http://localhost:8080
```

## CI/CD Integration

### GitHub Actions Workflows

This project includes two automated workflows:

#### 1. **Trigger Airflow DAG** (`.github/workflows/trigger-airflow-dag.yml`)
- **Trigger:** PR with changes to `contracts/current/**`
- **Actions:** 
  - Validates contracts with Airflow
  - Registers schemas in AWS Glue
  - Creates/updates Iceberg tables
  - Comments PR with results
- **Requires:** Airflow server + GitHub secrets

#### 2. **Approve & Merge PR** (`.github/workflows/approve-merge.yml`)
- **Trigger:** Comment `/approve-merge` on PR
- **Actions:** Automatically squash-merges PR
- **Who can use:** PR author, maintainers, admins

### Setup GitHub Actions (Complete Guide)

#### Step 1: Start Airflow Locally & Enable Remote Access

```bash
# 1. Start Airflow
make airflow-up

# 2. Open another terminal and start Bore tunnel for GitHub to reach your local Airflow
make bore-start
# Output will show: bore.pub forwarding http://... → localhost:8080

# Keep this running while testing!
```

#### Step 2: Get Airflow Credentials

```bash
# Find Airflow admin password
docker logs airflow-webserver | grep "Password for user"
# Output: Password for user 'admin': xxxxxxxxxxx

# Note down:
# - Username: admin
# - Password: xxxxxxxxxxx (from above)
# - Airflow URL: http://your-bore-url (from Step 1)
```

#### Step 3: Add GitHub Secrets

Go to **GitHub Repository → Settings → Secrets and variables → Actions**

Add these **Repository Secrets:**

| Secret Name | Value | Example |
|------------|-------|---------|
| `AIRFLOW_URL` | Your Bore tunnel URL | `http://abc123.bore.pub` |
| `AIRFLOW_USERNAME` | Airflow admin user | `admin` |
| `AIRFLOW_PASSWORD` | Airflow admin password | From Step 2 |

**Steps to add:**
1. Click **"New repository secret"**
2. Name: `AIRFLOW_URL`
3. Value: Paste your Bore tunnel URL
4. Click **"Add secret"**
5. Repeat for `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`

#### Step 4: Test the Workflow

1. **Create a test PR with contract changes:**
   ```bash
   git checkout -b test-contract-pr
   # Edit contracts/current/user/user_v1.json
   git add contracts/current/
   git commit -m "test: update contract for CI/CD test"
   git push origin test-contract-pr
   ```

2. **Create Pull Request on GitHub**
   - Go to your repository
   - Click **"Compare & pull request"**
   - Click **"Create pull request"**

3. **Watch GitHub Actions**
   - Go to **Actions** tab
   - Click **"Trigger Airflow DAG for Contract Provisioning"**
   - Watch the workflow run (should complete in ~2 min)
   - See results commented on your PR

4. **Approve and Merge (Optional)**
   ```
   Comment on PR: /approve-merge
   ```
   The PR will auto-merge!

#### Step 5: Deploy Airflow (Production)

When ready for production, deploy Airflow to a cloud provider:

**Option A: AWS EC2**
```bash
# Deploy using Terraform (see infra/aws/README.md)
cd infra/aws
terraform apply
```

**Option B: Docker on Server**
```bash
# Deploy docker-compose to your server
scp docker-compose.airflow.yml user@server:/opt/airflow/
ssh user@server
cd /opt/airflow
docker-compose up -d
```

**Then update GitHub secrets:**
- `AIRFLOW_URL` = `https://your-airflow-server.com` (not Bore URL anymore)

### Approval Workflow

After DAG succeeds on PR:

1. ✅ Check Airflow logs
2. ✅ Review GitHub PR comment (shows validation results)
3. ✅ Approve with comment: `/approve-merge`
4. ✅ PR auto-merges, contracts go to production

## Requirements

- **Docker & Docker Compose** (3.8+)
- **Airflow** 3.3.0 (containerized)
- **PostgreSQL** 15 (for Airflow metadata)
- **AWS Account** (Glue Schema Registry & Iceberg tables)
- **Python** 3.11+ (for local scripts)
- **GitHub Repository** (for automation)

## Documentation

- [Airflow DAG Details](airflow/dags/README.md) — Complete DAG task documentation
- [AWS Infrastructure](infra/aws/README.md) — Terraform setup
- [GitHub Actions Workflows](.github/workflows/) — CI/CD integration
  - `trigger-airflow-dag.yml` — Auto-triggers DAG on contract changes
  - `approve-merge.yml` — Auto-merges PRs with `/approve-merge` comment

## Troubleshooting

### Airflow UI not accessible
```bash
# Restart services
make airflow-down
make airflow-up

# Check logs
make airflow-logs
```

### DAG not triggering
```bash
# Check Airflow UI for parse errors
# View DAG folder
docker-compose -f docker-compose.airflow.yml exec airflow-webserver \
  airflow dags list

# Validate DAG syntax
docker-compose -f docker-compose.airflow.yml exec airflow-webserver \
  airflow dags validate airflow/dags/contract_provisioning_dag.py
```

### AWS credential issues
```bash
# Verify credentials in container
docker-compose -f docker-compose.airflow.yml exec airflow-webserver \
  aws sts get-caller-identity

# Check environment variables
docker-compose -f docker-compose.airflow.yml exec airflow-webserver \
  env | grep AWS
```

## Roadmap

- [ ] GitHub PR comment integration (currently placeholder)
- [ ] Slack notifications on failures
- [ ] Async table creation for better performance
- [ ] Rollback support for failed provisioning
- [ ] Unit tests for all DAG tasks
- [ ] Prometheus metrics and custom dashboards
- [ ] Audit logging for compliance

## License

MIT
