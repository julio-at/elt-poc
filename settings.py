from enum import StrEnum
import os
import pandas as ps

class Columns(StrEnum):
    NAME = "NAME"
    DATE = "DATE"
    VALUE = "VALUE"

# ENVIRONMENT VARIABLES
LOGGER_LEVEL = os.environ.get("LOGGER_LEVEL", "INFO")

DATE_FORMAT = os.environ.get("DATE_FORMAT", "%d-%b-%Y")
MAX_DATE = os.environ.get("MAX_DATE", "19-12-2014")

# Converting the start and end date to a datetime object, to be used in the filtering process
# Pandas date is more efficient than datetime.datime hence, the conversion is done here
FILTERED_START_DATE = ps.to_datetime("01-Nov-2014", format=DATE_FORMAT)
FILTERED_END_DATE = ps.to_datetime("30-Nov-2014", format=DATE_FORMAT)

INPUT_FILE_PATH = os.environ.get("INPUT_FILE_PATH", "example_data.csv")
OUTPUT_FILE_PATH = os.environ.get("OUTPUT_FILE_PATH",  "output.csv")

# Spark settings
MASTER_URL = os.environ.get("MASTER_URL", "local")
APP_NAME = os.environ.get("APP_NAME", "py-elt")
WORKERS = int(os.environ.get("WORKERS", 5))
