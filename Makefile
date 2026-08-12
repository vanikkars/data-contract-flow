.PHONY: help airflow-up airflow-down airflow-logs airflow-ui airflow-trigger validate-contracts validate-contracts-all bore-start bore-stop

help:
	@echo "Airflow DAG - Contract Provisioning"
	@echo "===================================="
	@echo ""
	@echo "Airflow Commands:"
	@echo "  make airflow-up      - Start Airflow services (docker-compose.airflow.yml)"
	@echo "  make airflow-down    - Stop Airflow services"
	@echo "  make airflow-logs    - View Airflow logs"
	@echo "  make airflow-ui      - Open Airflow UI (http://localhost:8080)"
	@echo "  make airflow-trigger - Trigger contract_provisioning DAG manually"
	@echo ""
	@echo "Validation Commands (Contract Automation):"
	@echo "  make validate-contracts     - Validate current contracts (contracts/current/)"
	@echo "  make validate-contracts-all - Validate all contracts (contracts/all/)"
	@echo "  make validate-export        - Validate current and export results"
	@echo "  make validate-export-all    - Validate all and export results"
	@echo "  make validate-remote        - Validate current contracts against remote API"
	@echo ""
	@echo "Tunnel Commands (GitHub Actions Integration):"
	@echo "  make bore-start    - Start Bore Tunnels (instant, no setup)"
	@echo "  make bore-stop     - Stop Bore Tunnels"
	@echo ""

# Airflow Commands (Contract Provisioning Orchestration)
airflow-up:
	docker-compose -f docker-compose.airflow.yml up
	@echo "✅ Airflow services started"
	@echo "📊 Airflow UI: http://localhost:8080"
	@echo "   Default credentials: airflow / airflow"
	@echo "   DAG: contract_provisioning"

airflow-up-build:
	docker-compose -f docker-compose.airflow.yml up --build
	@echo "✅ Airflow services started"
	@echo "📊 Airflow UI: http://localhost:8080"
	@echo "   Default credentials: airflow / airflow"
	@echo "   DAG: contract_provisioning"

airflow-down:
	docker-compose -f docker-compose.airflow.yml down
	@echo "✅ Airflow services stopped"

airflow-clean:
	@echo "🧹 Cleaning up Airflow (removing volumes and data)..."
	docker-compose -f docker-compose.airflow.yml down -v
	@echo "✅ Airflow cleaned up. Run 'make airflow-up' to start fresh"

airflow-reset:
	@echo "🔄 Resetting Airflow (full clean restart)..."
	docker-compose -f docker-compose.airflow.yml down -v
	docker-compose -f docker-compose.airflow.yml up -d
	@echo "⏳ Waiting for Airflow to initialize..."
	@sleep 15
	@echo "✅ Airflow reset complete!"
	@echo "📊 Access at: http://localhost:8080"
	@echo "👤 Login with: admin / admin"
	@echo "🔑 If you get 401 error, run: make airflow-get-password"

airflow-get-password:
	@echo "🔑 Airflow Admin Password:"
	@echo "================================"
	@docker logs airflow-webserver 2>&1 | grep "Password for user" | tail -1 || echo "❌ Password not found. Is Airflow running?"

airflow-clean:
	docker-compose -f docker-compose.airflow.yml down -v
	@echo "✅ Airflow services stopped, the volumes are deleted"

airflow-logs:
	docker-compose -f docker-compose.airflow.yml logs -f

airflow-ui:
	@echo "Opening Airflow UI..."
	@open http://localhost:8080 || echo "Visit: http://localhost:8080"

airflow-trigger:
	@echo "Triggering contract_provisioning DAG..."
	@curl -X POST http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns \
		-H "Content-Type: application/json" \
		-u airflow:airflow \
		-d '{"conf": {}}'
	@echo "✅ DAG triggered. Check Airflow UI at http://localhost:8080"

# Local Commands
setup:
	source .env
	@echo "✅ Environment loaded"

generate:
	@echo "❌ Contract generation moved to static contracts/ folder"
	@echo "   Add or modify contracts in: contracts/"

test:
	@echo "⚠️  Test command requires registry_api container"
	@echo "   Run: make airflow-up (to start Airflow services)"
	@echo "   Then: docker-compose -f docker-compose.airflow.yml exec airflow-webserver pytest tests/ -v"


tf-init:
	source .env && cd infra/aws && rm -rf .terraform && rm -rf .terraform.lock.hcl && rm -rf terraform.tfstate && terraform init


tf-plan:
	source .env && cd infra/aws && terraform plan

tf-apply:
	source .env && cd infra/aws && terraform apply

# Check Commands
check-env:
	@if [ -f .env ]; then \
		echo "✅ .env file exists"; \
		grep -E "AWS_" .env | head -3; \
	else \
		echo "❌ .env file not found"; \
		echo "   Run: cp .env.example .env"; \
	fi

check-docker:
	@docker --version
	@docker-compose --version
	@echo "✅ Docker is installed"

check-aws:
	@if [ -n "$$AWS_ACCESS_KEY_ID" ]; then \
		echo "✅ AWS credentials loaded"; \
	else \
		echo "⚠️  AWS credentials not loaded"; \
		echo "   Run: source .env"; \
	fi

# Info Commands
info:
	@echo "Airflow Project Info"
	@echo "==================="
	docker-compose -f docker-compose.airflow.yml ps
	@echo ""
	@echo "Recent Images:"
	@docker images | grep airflow || echo "No Airflow images found"

version:
	@grep -E "version|image" docker-compose.airflow.yml | head -5

list-all-commands:
	@echo "All available targets:"
	@grep -E "^[a-zA-Z_-]+:" Makefile | sed 's/:.*//g' | column

# Contract Validation Commands
validate-contracts:
	@echo "🔍 Validating current contracts..."
	python scripts/validate-contracts.py contracts/current/

validate-contracts-all:
	@echo "🔍 Validating all contracts..."
	python scripts/validate-contracts.py contracts/all/

validate-contracts-%:
	@echo "🔍 Validating contracts in $*..."
	python scripts/validate-contracts.py contracts/$*

validate-export:
	@echo "🔍 Validating current contracts and exporting results..."
	python scripts/validate-contracts.py contracts/current/ --export validation_results.json
	@echo "✅ Results saved to validation_results.json"

validate-export-all:
	@echo "🔍 Validating all contracts and exporting results..."
	python scripts/validate-contracts.py contracts/all/ --export validation_results.json
	@echo "✅ Results saved to validation_results.json"

validate-remote:
	@echo "🔍 Validating current contracts against remote API..."
	@read -p "Enter registry API URL: " url; \
	python scripts/validate-contracts.py contracts/current/ --registry-url $$url

# Bore Tunnel Commands
bore-start:
	@echo "🚀 Starting Bore Tunnels..."
	@./start-bore.sh

bore-stop:
	@echo "🛑 Stopping Bore Tunnels..."
	@pkill -f "bore local" 2>/dev/null || echo "No bore tunnels running"
	@echo "✅ Bore tunnels stopped"