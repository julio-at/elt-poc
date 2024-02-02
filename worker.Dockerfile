FROM bitnami/spark:3.5.0

ENV SPARK_MODE=worker
ENV SPARK_WORKER_CORES=4
ENV SPARK_WORKER_MEMORY=1g
ENV SPARK_MASTER_URL=spark://spark-master:7077

CMD [ "bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077" ]
