import ray
import ray.data
import time as t
import psutil
from memory_profiler import memory_usage
from ray.train.xgboost import XGBoostTrainer
from ray.train import ScalingConfig
import argparse
import sys
import gc
from pprint import pprint

num_workers = 3
def read_dataset(num_years, columns):
    years = [0,2019, 2020, 2021]
    
    # we are going to be using 4 months of data from each year to achieve 4-8 and 12 months for the comparison
    months = ["01", "02", "03", "04"] #, "05", "06", "07", "08", "09", "10", "11", "12"]

    file_paths = [f"local:///home/taxi_drivers/data/{year}/fhvhv_tripdata_{year}-{month}.parquet"
        for year in years[:(num_years+1)]
        for month in months
        if (not (year == 2019 and month == "01")) and (not (year == 2022 and month == "12")) and year!=0
        ]

    # Read Parquet files into Ray Dataset
    return ray.data.read_parquet(file_paths, columns=columns)

# Reading the taxi zones corresponding to the Location IDs  
taxizones = ray.data.read_csv(f"local:///home/taxi_drivers/data/taxizones.csv")

def mapFunc(batch):
    zone_mapping = {
    'EWR': 1.0,
    'Boro Zone': 2.0,
    'Yellow Zone': 3.0,
    'Airports': 4.0
    }
    batch['service_zone'] = batch['service_zone'].map(lambda x: zone_mapping[x] if x in zone_mapping else 0.0)
    return batch
    
def preprocess(ds, taxizones):
    
    taxizones = taxizones.map_batches(mapFunc, batch_format="pandas")
    print("Zone Mapping done")
    
    # Create a dictionary to map LocationID to service_zone
    location_to_zone_dict = {}
    for batch in taxizones.iter_batches(batch_size=None):  # batch_size=None to process all at once
        for location_id, zone in zip(batch['LocationID'], batch['service_zone']):
            location_to_zone_dict[location_id] = zone
    print("Dictionary created")
    
    def labelFunc(batch):
        batch['shared_request_flag'] = batch['shared_request_flag'].map(lambda x: 1 if x == 'Y' else 0)
        # Use location_to_zone_dict to map PULocationID to service_zone
        batch['PULocationID'] = batch['PULocationID'].map(lambda x: location_to_zone_dict[x] if x in location_to_zone_dict else 0.0)
        batch['DOLocationID'] = batch['DOLocationID'].map(lambda x: location_to_zone_dict[x] if x in location_to_zone_dict else 0.0)
        return batch
    
    train_data = ds.map_batches(labelFunc, batch_format="pandas")
    print("Labeling done")
    
    return train_data

parser = argparse.ArgumentParser(description='Process the number of years to read dataset.')
parser.add_argument('num_years', type=int, choices=[1, 2, 3], help='The number of years (1, 2, or 3) of data to read.')
args = parser.parse_args()
num_years = args.num_years

columns = [
    "PULocationID",
    "DOLocationID",
    "shared_request_flag"
]

start_time = t.time()

dataset_mem, ray_dataset = memory_usage((read_dataset,(num_years, columns)), retval=True)

read_time = t.time()

proc_mem, processed_dataset = memory_usage((preprocess,(ray_dataset, taxizones)), retval=True)
print("rows: ", processed_dataset.count())
processed_dataset.show(500) 
print("Splitting dataset...")
train_ds, test_ds = processed_dataset.train_test_split(0.3)

print("Preprocessing done!")
proc_time = t.time()

# Configure the XGBoost model
randomForest = XGBoostTrainer(
    label_column = "shared_request_flag",
    num_boost_round = 5,
    scaling_config = ScalingConfig(num_workers = num_workers, resources_per_worker={"CPU": 2},),
    datasets = {"train": train_ds, "valid": test_ds},
    # Set the parameters for a Random Forest Classifier
    params = {
        "colsample_bynode": 0.8,
        "learning_rate": 1,
        "max_depth": 5,
        "num_parallel_tree": 5,
        "objective": "binary:logistic",
        "subsample": 0.8,
        "eval_metric": ["error"],
        "tree_method": "hist",
    }
)

def training_func():
    return randomForest.fit()
    
del ray_dataset, processed_dataset, taxizones
gc.collect()
# Train the model
print("Training...")
train_mem, result = memory_usage((training_func, ()), retval=True)
print("Training done!")

# Stop Measurements
end_time = t.time()
read_dataset_t = read_time - start_time
preprocess_t = proc_time - read_time
train_model_t = end_time - start_time

# Print and Save Metrics
print("\n================================ Metrics ================================\n")
print(f"Number of months: {num_years} * 4, Workers: {num_workers}")
print(f"Reading Dataset:\n\tExecution Time: {read_dataset_t} s\n\tMemory Usage: {dataset_mem[-1]} MB")
print(f"Preprocessing:\n\tExecution Time: {preprocess_t} s\n\tMemory Usage: {proc_mem[-1]} MB")
print(f"Random Forest:\n\tExecution Time: {train_model_t} s\n\tMemory Usage: {train_mem[-1]} MB")
print(result.metrics)

with open("/home/taxi_drivers/ML/resultsRay/results.txt", "a") as file:
    file.write(f"Number of months: {num_years} * 4, Workers: {num_workers}\n")
    file.write(f"Reading Dataset:\n\tExecution Time: {read_dataset_t} s\n\tMemory Usage: {dataset_mem[-1]} MB\n")
    file.write(f"Preprocessing:\n\tExecution Time: {preprocess_t} s\n\tMemory Usage: {proc_mem[-1]} MB\n")
    file.write(f"Random Forest:\n\tExecution Time: {train_model_t} s\n\tMemory Usage: {train_mem[-1]} MB\n")
    pprint(result.metrics, stream=file)
    file.write("\n\n")  # Add a new line for next run
    



