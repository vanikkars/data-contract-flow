#!/bin/bash

echo "🚀 Bore Tunnel"
echo "=============="
echo ""

# Check if bore is installed
if ! command -v bore &> /dev/null; then
    echo "❌ bore not installed"
    echo ""
    echo "Install with:"
    echo "  macOS: brew install bore-cli"
    echo "  Linux: curl https://k守-bore.pub/install.sh | bash"
    echo "  Rust: cargo install bore-cli"
    exit 1
fi

echo "✅ bore found: $(bore --version)"
echo ""

# Kill any existing bore processes
pkill -f "bore local" 2>/dev/null || true

sleep 1

echo "Starting bore tunnel..."
echo ""

# Start Airflow tunnel in background
echo "🚀 Airflow Webserver (port 8080)"
bore local 8080 --to bore.pub &
AIRFLOW_PID=$!

sleep 2

echo ""
echo "✅ Bore tunnel started!"
echo ""
echo "PID: $AIRFLOW_PID"
echo ""
echo "📝 Copy the AIRFLOW_URL from above and add to GitHub secrets:"
echo "   Repository → Settings → Secrets and variables → Actions"
echo "   Secret: AIRFLOW_URL"
echo ""
echo "⚠️  Keep this terminal open while using GitHub Actions!"
echo "To stop: pkill -f 'bore local' or Ctrl+C"
echo ""

wait

## Schema Validation & Auto-Merge / validate-contracts (pull_request)