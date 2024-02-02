from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.pandas as ps
import os

# Defining the input data and the date range
FILE_NAME = "example.csv"
START_DATE = "01-Nov-2014"
END_DATE = "30-Nov-2014"

# Converting the start and end date to a datetime object, to be used in the filtering process
START_DATE = ps.to_datetime(START_DATE, format='%d-%b-%Y')
END_DATE = ps.to_datetime(END_DATE, format='%d-%b-%Y')

# Create a spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("py-elt") \
    .getOrCreate()

# Define the schema of the input data, to speed up the reading process
schema = StructType([
    StructField("NAME", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("VALUE", FloatType(), True)
])

# Read the input data
df = spark.read.csv(FILE_NAME,
                    header=True, 
                    schema=schema)
df.cache()                  # caching the dataframe to speed up subsquent processes
psdf = df.pandas_api()      # converting the dataframe to a pandas dataframe in spark

# Converting the DATE column to a DateType
# This is done on this part for performance reasons, as the conversion is faster in pandas
psdf["DATE"] = ps.to_datetime(psdf["DATE"], format='%d-%b-%Y')

# INSTRUMENT1
ins1 = psdf[psdf["NAME"] == "INSTRUMENT1"]

# INSTRUMENT2
ins2 = psdf[(psdf["NAME"] == "INSTRUMENT2") & 
            (psdf["DATE"] >= START_DATE) & 
            (psdf["DATE"] <= END_DATE)]

# INSTRUMENT3
ins3 = psdf[psdf["NAME"] == "INSTRUMENT3"]

# Calculating the results
ins1_result = ins1["VALUE"].mean()
ins2_result = ins2["VALUE"].mean()
ins3_result = ins3["VALUE"].max()

# Printing the results
print(f"INSTRUMENT1 MEAN: {ins1_result}")
print(f"INSTRUMENT2 MEAN (NOV 2014): {ins2_result}")
print(f"INSTRUMENT3 MAX: {ins3_result}")

# Saving the results to a file
# The use of cat and the p*.csv files is to merge the partitions into a single file
ins1.to_csv(path=r'%s/output/instrument1', header=False)
os.system("cat %s/output/instrument1/p*.csv > instrument1.csv")
ins2.to_csv(path=r'%s/output/instrument2', header=False)
os.system("cat %s/output/instrument2/p*.csv > instrument2.csv")
ins3.to_csv(path=r'%s/output/instrument3', header=False)
os.system("cat %s/output/instrument3/p*.csv > instrument3.csv")

spark.stop()
