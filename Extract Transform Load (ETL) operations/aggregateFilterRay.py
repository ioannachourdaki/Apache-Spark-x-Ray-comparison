import ray
import ray.data
import time as t
from memory_profiler import memory_usage
import argparse

ray.init()

# Function that reads the data based on years
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

# Function that performs the aggregate filter
def agg_filt(ds):
    # Filter for rows where DOLocationID == 1
    filtered_ds = ds.filter(lambda row: row["DOLocationID"] == 1)
    # Group by PULocationID and calculate the average trip_time
    grouped_ds = filtered_ds.groupby("PULocationID").mean("trip_time")
    return grouped_ds

# Used to receive the number of years as argument when calling execution
parser = argparse.ArgumentParser(description='Process the number of years to read dataset.')
parser.add_argument('num_years', type=int, choices=[1, 2, 3], help='The number of years (1, 2, or 3) of data to read.')
args = parser.parse_args()
num_years = args.num_years

#Setting the necessary columns that we need
columns = [
    "PULocationID",
    "DOLocationID",
    "trip_time"
]

# Start counting time it takes to read the dataset
start_time_dataset = t.time()

# Memory usage is used to calculate the memory used for a function
dataset_mem, ray_dataset = memory_usage((read_dataset,(num_years, columns)), retval=True)
read_time = t.time()

# Used to run the aggregate and filter function and calculate the memory it used at the same time
query_mem, processed_dataset = memory_usage((agg_filt,(ray_dataset,)), retval=True)

print("rows: ", processed_dataset.count())

# Stop counting time and memory for the datasets
finish_time_dataset = t.time()
readTime = read_time - start_time_dataset
process_time = finish_time_dataset - read_time

with open('aggregateFilterRayResults.txt', 'a') as file:
    file.write(
        f"\nRun Aggregate Filter\n"
        f"- Dataset: {num_years} years, Rows: {ray_dataset.count()} rows\n"
        f"- Dataset Memory: {dataset_mem[-1]} MB\n"
        f"- Query Memory: {query_mem[-1]} MB\n"
        f"- Time it took to read dataset: {readTime} seconds\n"
        f"- Time it took to process dataset: {process_time} seconds\n"
    )