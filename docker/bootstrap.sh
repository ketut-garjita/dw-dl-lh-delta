#!/usr/bin/env bash
set -e

# Simpan UID user lokal ke file .env agar dikenali docker-compose
echo "AIRFLOW_UID=$(id -u)" > .env

# Buat folder jika belum ada
mkdir -p airflow_dags airflow_logs postgres_data

# Set permission supaya container bisa nulis log & data
chmod -R 777 airflow_logs postgres_data

echo "✅ Environment prepared. UID=$(id -u)"
echo "➡️  Next: docker compose up airflow-init"

