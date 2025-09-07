up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down -v

spark-run:
	./docker/spark/jars/download_jars.sh
	docker compose -f docker/docker-compose.yml exec spark \
	  spark-submit --packages io.delta:delta-core_2.12:2.4.0 /opt/jobs/bronze_to_silver_delta.py
