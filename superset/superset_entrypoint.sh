#!/bin/bash
set -e

echo "Starting Superset initialization..."

# Upgrade metadata database
superset db upgrade

# Tạo admin user nếu chưa có
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin || true

# Init roles + permissions
superset init

# Register Trino database connection
superset set_database_uri -d Trino -u trino://trino@trino:8080/iceberg/ || echo "Trino connection already exists or failed."

echo "Superset initialization completed"

# Start Superset
exec superset run -h 0.0.0.0 -p 8088 --with-threads