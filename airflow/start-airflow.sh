#!/bin/bash

# Fix docker socket permissions 
if [ -S /var/run/docker.sock ]; then
  DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)
  echo "Fixing Docker socket permissions (GID: $DOCKER_GID)..."
  sudo groupadd -g "$DOCKER_GID" -o docker_host 2>/dev/null || true
  sudo usermod -aG docker_host airflow 2>/dev/null || true
  # Also allow direct access if group doesn't work
  sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
fi

# Chờ database sẵn sàng
echo "Đang chờ Database (Postgres) sẵn sàng..."
until pg_isready -h postgres -p 5432; do
  sleep 2
done
# Khởi tạo database (Sử dụng db migrate thay cho db init nếu bản mới)
echo "Khởi tạo/Cập nhật Airflow DB..."
${AIRFLOW_HOME}/venv/bin/airflow db migrate

# Tạo User admin (nếu chưa có)
echo "Tạo User admin..."
${AIRFLOW_HOME}/venv/bin/airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Chạy Webserver ở chế độ background
echo "Khởi động Webserver..."
${AIRFLOW_HOME}/venv/bin/airflow webserver -p 8080 &

# Chạy Scheduler ở chế độ foreground (tiến trình chính của container)
echo "Khởi động Scheduler..."
exec ${AIRFLOW_HOME}/venv/bin/airflow scheduler
