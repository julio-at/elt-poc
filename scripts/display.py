from pyspark.sql import SparkSession

FILE_NAME = "example.csv"

spark = SparkSession.builder \
    .master("local") \
    .appName("py-elt") \
    .getOrCreate()

df = spark.read.csv(FILE_NAME, header=True, inferSchema=True)
df.show()

spark.stop()