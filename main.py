#  -----------------------------------  1. IMPORT LIBRARIES  -----------------------------------

# Import the load_dotenv function to load values from an .env file into environment variables
from dotenv import load_dotenv
# Built-in Python module to interact with the OS, including reading environment variables at runtime
import os
# SQLAlchemy function to create a connection to PostgreSQL
from sqlalchemy import create_engine
# Pandas for single-core data preprocessing. Used here to read from SQL before converting to Spark
import pandas as pd
# Import SparkSession to create a Spark context and work with Spark DataFrames and parallel preprocessing
from pyspark.sql import SparkSession


#  --------------------  2. CONNECT TO THE PostgreSQL DATABASE & READ THE RAW DATA  --------------------

load_dotenv()  # Load the .env file into environment variables, so they can be accessed with os.getenv()

# Read PostgreSQL credentials from environment variables
username = os.getenv('postgresuser')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
db_name = os.getenv('db_name')

# Set up the connection to the local PostgreSQL database using the above credentials.
# SQLAlchemy uses psycopg2 (got installed in the environment) automatically to manage the connection.
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')

sql_query = "SELECT * FROM table_raw"  # Query the entire raw hotel_booking dataset
df_raw = pd.read_sql(sql_query, engine)  # Read the SQL data to a pandas DataFrame

# Check the shape of the pandas DataFrame
print('Raw Pandas DataFrame Shape:', df_raw.shape)  # Expected: (119390, 36)


#  --------------------  3. CONVERT RAW DATA TO A SPARK DATAFRAME  --------------------

# Create a Spark session for local processing
spark = SparkSession.builder \
    .appName("HotelBookingSparkPreprocessing") \
    .getOrCreate()

# Convert the Pandas DataFrame to a Spark DataFrame
spark_raw = spark.createDataFrame(df_raw)

# Check the shape of Spark DataFrame
print("Raw Spark DataFrame Shape:", (spark_raw.count(), len(spark_raw.columns)))
