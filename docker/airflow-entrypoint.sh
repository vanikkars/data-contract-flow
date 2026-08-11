#!/bin/bash
set -e

# Initialize Airflow database
echo "🚀 Initializing Airflow database..."
airflow db migrate

# Create default admin user if it doesn't exist
echo "👤 Setting up admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin \
  || echo "Admin user already exists"

# Create default connections
echo "🔗 Setting up connections..."

# AWS connection (uses environment variables for credentials)
airflow connections add \
  'aws_default' \
  --conn-type 'aws' \
  --conn-host 'amazonaws.com' \
  || echo "AWS connection already exists"

# Start Airflow in standalone mode (all-in-one)
echo "✅ Starting Airflow webserver in standalone mode..."
exec airflow standalone
