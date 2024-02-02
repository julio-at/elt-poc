# Upload file to spark-master, if FILE is not set, it will upload all files in the files directory
upload:
	docker cp files/${FILE} spark-master:/opt/bitnami/spark/${FILE}

# Submit a job to spark-master, if SCRIPT is not set, it will submit all files in the jobs directory
submit:
	docker cp scripts/${SCRIPT} spark-master:/opt/bitnami/spark/${SCRIPT}

# Run a job on spark-master, SCRIPT is required
run:
	docker compose exec spark-master spark-submit --master spark://localhost:7077 ${SCRIPT}

pull:
	docker cp spark-master:/opt/bitnami/spark/instrument1.csv output/instrument1.csv && \
	docker cp spark-master:/opt/bitnami/spark/instrument2.csv output/instrument2.csv && \
	docker cp spark-master:/opt/bitnami/spark/instrument3.csv output/instrument3.csv

# Submit and run a job on spark-master, SCRIPT is required
submit-run: submit run