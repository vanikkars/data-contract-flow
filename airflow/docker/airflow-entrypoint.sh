#!/bin/bash
set -e

# Initialize Airflow database
echo "🚀 Initializing Airflow database..."
airflow db migrate

# Create default connections
echo "🔗 Setting up AWS connection..."
airflow connections add \
  'aws_default' \
  --conn-type 'aws' \
  --conn-host 'amazonaws.com' \
  2>/dev/null || echo "ℹ️  AWS connection already exists"

echo ""
echo "✅ Airflow is ready!"
echo "📊 Access at: http://localhost:8080"
echo ""
echo "👤 Default Admin User:"
echo "   Username: admin"
echo "   Password: admin (see note below)"
echo ""
echo "⚠️  NOTE: Airflow 3.3.0 SimpleAuthManager generates random passwords."
echo "   If you get a 401 error, use one of these:"
echo ""
echo "   1. Get password from logs:"
echo "      docker logs airflow-webserver | grep 'Password for user'"
echo ""
echo "   2. Get password from file:"
echo "      docker exec airflow-webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated"
echo ""
echo "   3. Reset admin password:"
echo "      docker exec airflow-webserver airflow users delete admin --yes"
echo "      docker exec airflow-webserver airflow users create \\"
echo "        --username admin --password admin --role Admin \\"
echo "        --firstname Admin --lastname User --email admin@example.com"
echo ""

# Start Airflow webserver and scheduler
echo "🚀 Starting Airflow webserver and scheduler..."
exec airflow standalone
