import ray
import ray.data
import time as t
from memory_profiler import memory_usage
import argparse

# Used to read the dataset based on the years and the columns needed
def read_dataset(num_years, columns):
    years = [0,2019, 2020, 2021]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    file_paths = [f"local:///home/taxi_drivers/data/{year}/fhvhv_tripdata_{year}-{month}.parquet"
        for year in years[:(num_years+1)]
        for month in months
        if (not (year == 2019 and month == "01")) and (not (year == 2022 and month == "12")) and year!=0
        ]

    # Read Parquet files into Ray Dataset
    return ray.data.read_parquet(file_paths, columns=columns)

# Use to receive the number of years as argument when calling execution
parser = argparse.ArgumentParser(description='Process the number of years to read dataset.')
parser.add_argument('num_years', type=int, choices=[1, 2, 3], help='The number of years (1, 2, or 3) of data to read.')
args = parser.parse_args()
num_years = args.num_years

# Setting the columns needed to be loaded
columns = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag"
    ]

'''
    In this part we will calculate the time it takes Ray to read the dataset
    for 1 year of data (around 5-6GB), 2 years (around 8-9GB) or 3 years (around 11-12GB)
'''

# Start calculating time
start_time_dataset = t.time()

# Calculate the memory that is used in order to read the dataset
dataset_mem, ray_dataset = memory_usage((read_dataset,(num_years, columns)), retval=True)

print("rows: ", ray_dataset.count())

# Stop counting time and memory for the datasets
finish_time_dataset = t.time()
dataset_time = finish_time_dataset - start_time_dataset

with open('datasetRayTimes.txt', 'a') as file:
    file.write(
        f"\nRun\n"
        f"- Dataset: {num_years} years, Rows: {ray_dataset.count()} rows\n"
        f"- Dataset Memory: {dataset_mem[-1]} MB\n"
        f"- Time it took to read dataset: {dataset_time} seconds\n"
    )