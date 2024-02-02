from pyspark.sql import SparkSession
from settings import INPUT_FILE_PATH, MASTER_URL, APP_NAME

spark: SparkSession = SparkSession.builder \
    .master(MASTER_URL) \
    .appName(APP_NAME) \
    .getOrCreate()

df = spark.read.csv(INPUT_FILE_PATH, header=True, inferSchema=True)

spark.stop()

