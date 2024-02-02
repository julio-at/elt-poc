FROM bitnami/spark:3.5.0

RUN pip install \
    pandas \
    numpy \
    pyarrow

EXPOSE 8080
EXPOSE 7077

CMD [ "bin/spark-class", "org.apache.spark.deploy.master.Master" ]
