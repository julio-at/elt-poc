from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.pandas as ps
import os
import logging

from settings import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """
    Create a Spark session.
    """
    spark: SparkSession = SparkSession.builder \
        .master(MASTER_URL) \
        .appName(APP_NAME) \
        .getOrCreate()
    return spark

def read_input_data(spark: SparkSession, file_path: str = INPUT_FILE_PATH) -> ps.DataFrame:
    """
    Read input data from file with provided schema.
    """
    data_schema: StructType = StructType([ #? Data schema could be passed as a parameter
        StructField(Columns.NAME, StringType(), True),
        StructField(Columns.DATE, StringType(), True),
        StructField(Columns.VALUE, FloatType(), True)
    ])
    data_frame: ps.DataFrame = spark.read.csv(file_path, header=True, schema=data_schema)
    data_frame.cache()
    return data_frame

def show_data(data_frame: ps.DataFrame) -> None:  
    data_frame.show()

def process_data(data_frame: ps.DataFrame) -> tuple[float, float, float]:
    """
    Process input data for calculation of results for each instrument.
    """
    ps_data_frame: ps.DataFrame = data_frame.pandas_api()
    ps_data_frame[Columns.DATE] = ps.to_datetime(ps_data_frame[Columns.DATE], format=DATE_FORMAT)

    # Calculate mean Inst 1
    instrument_1: ps.DataFrame = ps_data_frame[ps_data_frame[Columns.NAME] == "INSTRUMENT1"]
    
    # Calculate mean Inst 2 filtered by date
    instrument_2: ps.DataFrame = ps_data_frame[(ps_data_frame[Columns.NAME] == "INSTRUMENT2") & 
                                               (ps_data_frame[Columns.DATE] >= FILTERED_START_DATE) & 
                                               (ps_data_frame[Columns.DATE] <= FILTERED_END_DATE)]
    # Calculate max Inst 3 
    instrument_3: ps.DataFrame = ps_data_frame[ps_data_frame[Columns.NAME] == "INSTRUMENT3"]

    instrument_1_result: float = instrument_1["VALUE"].mean()
    instrument_2_result: float = instrument_2["VALUE"].mean()
    instrument_3_result: float = instrument_3["VALUE"].max()

    return instrument_1_result, instrument_2_result, instrument_3_result

def save_results(instrument: ps.DataFrame, result: str, output_path: str) -> None:
    """
    Save instrument results to an output directory file.
    """
    instrument.to_csv(path=output_path, header=False)
    os.system(f"cat {output_path}/p*.csv > {result}.csv")


def main() -> None:
    """
    Main function to process input data and calculate results for each instrument.
    """
    # Create a Spark session and read input data
    spark: SparkSession = create_spark_session()
    data_frame: ps.DataFrame = read_input_data(spark)

    # Process data to calculate instrument results
    instrument_results: tuple[float, float, float] = process_data(data_frame)
    
    # Log instrument results
    logger.debug("Instrument 1 Mean: %s", instrument_results[0])
    logger.debug("Instrument 2 Mean filtered by : %s", instrument_results[1])
    logger.debug("Instrument 3 Max: %s", instrument_results[2])

    # Save results for each instrument
    for inst_id, result in enumerate(instrument_results, start=1):
        save_results(data_frame, f"instrument{inst_id}", result, f"{OUTPUT_FILE_PATH}/output_results/instrument{inst_id}")

    # Stop Spark session
    spark.stop()

main()
