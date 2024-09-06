import ray
from ray.data import read_parquet
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import pandas as pd
import numpy as np
import time as t
from memory_profiler import memory_usage
import gc


num_years = 2

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


# Setting the runtime environment
runtime_env = {
    "pip": ["pandas", "pyarrow", "scikit-learn"]
}

# Initialize Ray with the runtime environment
ray.init(runtime_env=runtime_env)


print("Reading dataset...")

# Seeting the necessary columns for the Kmeans operation
columns = ['driver_pay', 'trip_miles']
combined_ds = read_dataset(num_years, columns)


### Preprocessing ###

print("Preprocessing...")

# Execution time measurment for preprocessing
start_time = t.time()

def preprocess(combined_ds):
    print("to pandas")
    df_pandas = combined_ds.to_pandas()

    # Delete combined_ds and free up memory
    del combined_ds
    gc.collect()

    print("drop na")
    df_pandas = df_pandas.dropna()
    print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    print(df_pandas.head(10))
    print(f"Rows: {df_pandas.shape[0]}")

    return df_pandas

preprocess_mem, df_pandas = memory_usage((preprocess, (combined_ds,)), retval=True )

print("Preprocessing done!")

# Stop preprocessing measurements
end_time = t.time()
preproc_time = end_time - start_time


### k-Means Clustering ###

print("Starting k-means clustering...")

# Start time measurement
start_time = t.time()

# Define a Ray remote function for KMeans clustering
@ray.remote
def kmeans_clustering(model, data):
    predictions = model.fit_predict(data)
    return predictions

# perform siilhouette score on chunks of data
@ray.remote
def compute_silhouette_score_chunk(data_chunk, labels_chunk):
    return silhouette_score(data_chunk, labels_chunk)

# Create chunks of data for only the dataset
def chunkifier(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Create chunks of data for dataset and labels
def chunkify(data, labels, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size], labels[i:i + chunk_size]

# Set features
features = ['driver_pay', 'trip_miles']
data = df_pandas[features].values

# Delete df_pandas and free up memory
del df_pandas
gc.collect()

# Model trains and performs ray clustering and then evaluates it
def model(data):
    # Apply KMeans clustering using Ray remote function
    print("Setting model...")
    model = KMeans(n_clusters=3, max_iter=5, random_state=42)

    # Define chunk size (adjust based on your system and data size)
    chunk_size = 1000

    # Split the data and labels into chunks
    print("Chunks...")
    chunks = list(chunkifier(data, chunk_size))
    print("Chunks done.")

    # perform ray clustering in parallel using ray remote on chunks of data
    print("Training...")
    kmeans_future = [kmeans_clustering.remote(model, chunk_data) for chunk_data in chunks]
    print("Training done.")

    # Get results
    print("Getting results...")
    predictions = ray.get(kmeans_future)
    print("Got results!")

    # Perform silhouette score evaluation for chunks of data
    print("Evaluating...")
    silhouette_futures = [compute_silhouette_score_chunk.remote(chunk_data, chunk_labels) for chunk_data, chunk_labels in zip(chunks, predictions)]
    
    # Get results
    print("Getiing evaluations...")
    silhouette_scores = ray.get(silhouette_futures)
    print("Evaluations done!")
    
    return np.mean(silhouette_scores)


# End memory and time measurement
memory, silhouette = memory_usage((model,(data,)), retval=True)
end_time = t.time()
model_time = end_time - start_time

### Print Measurements ###
print("\n================================ Metrics ================================\n")
print(f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preprocess_mem[-1]:.3f} MB")
print(f"kMeans:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {memory[-1]:.3f} MB")
print(f'\nSilhouette Score: {silhouette:.3f}')

with open('metrics.txt', 'a') as file:
    file.write(
        "\n================================ Metrics ================================\n"
        f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preprocess_mem[-1]:.3f} MB"
        f"kMeans:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {memory[-1]:.3f} MB\n"
        f"Silhouette Score: {silhouette:.3f}\n"
    )