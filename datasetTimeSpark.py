from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum as pysum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import time as t
from memory_profiler import memory_usage
import argparse
import sys

instances = "3"

# The code below is used in order to input the number of years we want from our datasets
parser = argparse.ArgumentParser(description="Process some integers.")
parser.add_argument("num_years", type=int, help="Number of years of data to process")

args = parser.parse_args()
num_years = args.num_years

# Validate num_years
if num_years not in {1, 2, 3}:
    print("Error: num_years must be one of the following values: 1, 2, 3.")
    sys.exit(1)


def read_dataframe(num_years, taxi_schema):
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    years = [0, 2019, 2020, 2021] # 0 is added for easier iteration

    df = spark.read.parquet(fr"hdfs:///taxi_drivers/2019/fhvhv_tripdata_2019-02.parquet",header=True, schema=taxi_schema)

    # Change num_years to the number of years that you want, if you want 1,2 or 3 years respectively
    for year in years[:(num_years+1)]:
        if year == 0:
            continue
        for month in months:
            if not (year == 2019 and month == "02"):
                try:
                    df1 = spark.read.parquet(fr"hdfs:///taxi_drivers/{year}/fhvhv_tripdata_{year}-{month}.parquet", header=True, schema=taxi_schema)
                    df = df.union(df1)
                    
                except Exception:
                    print(f"File {year}-{month} does not exist.")

    return df

# Start calculating time and memory for dataset reading
start_time_dataset = t.time()

# Create a Spark session
spark = SparkSession.builder.appName("DatasetTimeSpark")\
        .config("spark.executor.instances", instances) \
        .getOrCreate()

# Create the schema. We read everything in order to count the time to read the whole dataset
taxi_schema = StructType([
    StructField("hvfhs_license_num", StringType(), nullable=False),
    StructField("dispatching_base_num", StringType()),
    StructField("originating_base_num", StringType()),
    StructField("request_datetime", DateType()),
    StructField("on_scene_datetime", DateType()),
    StructField("pickup_datetime", DateType()),
    StructField("dropoff_datetime", DateType()),
    StructField("PULocationID", IntegerType(), nullable=False),
    StructField("DOLocationID", IntegerType(), nullable=False),
    StructField("trip_miles", DoubleType()),
    StructField("trip_time", DoubleType()),
    StructField("base_passenger_fare", DoubleType()),
    StructField("tolls", DoubleType()),
    StructField("bcf", DoubleType()),
    StructField("sales_tax", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("tips", DoubleType()),
    StructField("driver_pay", DoubleType()),
    StructField("shared_request_flag", StringType()),
    StructField("shared_match_flag", StringType()),
    StructField("access_a_ride_flag", StringType()),
    StructField("wav_request_flag", StringType())
])

memory, df = memory_usage((read_dataframe,(num_years,taxi_schema)), retval=True)

# Stop counting time and memory for the datasets
finish_time_dataset = t.time()

dataset_time = finish_time_dataset - start_time_dataset
dataset_memory = memory[-1]

# We write the results in a txt file for easier reading later on
with open('datasetSparkTimes.txt', 'a') as file:
    file.write(f"\nRun\n")
    file.write(f"- Dataset: {num_years} years and {df.count()} rows\n")
    file.write(f"- Number of workers: {instances} workers\n")
    file.write(f"- Time it took to read dataset: {dataset_time} seconds\n")
    file.write(f"- Memory used to read dataset: {dataset_memory} MB\n")