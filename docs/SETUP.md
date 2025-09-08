# Quickstart (Local Docker)

Requirements:
- Docker & docker-compose
- (optional) Python 3.11 for local scripts

1. Make ./docker/.env
   ```bash
   ./docker/bootstrap.sh
   ```
   
2. Start services
   ```bash
   docker compose -f docker/docker-compose.yml up -d
   ```

3. Seed Postgres with sample data:
   ```bash
   python scripts/load_seed_to_postgres.py
   ```

4. Start the sample Kafka producer (optional):
   ```bash
   python scripts/ingest_kafka.py
   ```

5. Run Spark job to build Delta silver:
   ```bash
   # inside container, or use docker exec to spark container
   ./docker/spark/jars/download_jars.sh  # download jars needed (one-time)
   docker compose -f docker/docker-compose.yml exec spark      spark-submit --packages io.delta:delta-core_2.12:2.4.0 /opt/jobs/bronze_to_silver_delta.py
   ```

7. Run dbt (locally configured to point to Postgres) and CI steps as desired.
