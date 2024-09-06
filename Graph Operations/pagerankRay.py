import ray
import ray.data
import time as t
import networkx as nx
from memory_profiler import memory_usage
import argparse

# Used to have all available resources: 6 for 2 nodes and 8 for 3
ray.autoscaler.sdk.request_resources(num_cpus=8)

# Read the dataset for the needed number of years
def read_dataset(num_years, columns):
    years = [0,2019, 2020, 2021]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    file_paths = [f"local:///home/taxi_drivers/data/{year}/fhvhv_tripdata_{year}-{month}.parquet"
        for year in years[:(num_years+1)]
        for month in months[:7]
        if (not (year == 2019 and month == "01")) and (not (year == 2022 and month == "12")) and year!=0
        ]

    # Read Parquet files into Ray Dataset
    return ray.data.read_parquet(file_paths, columns=columns)

parser = argparse.ArgumentParser(description='Process the number of years to read dataset.')
parser.add_argument('num_years', type=int, choices=[1, 2, 3], help='The number of years (1, 2, or 3) of data to read.')
args = parser.parse_args()
num_years = args.num_years

# Start calculating time and memory for dataset reading
start_time_dataset = t.time()

# Set the columns required
columns = ["PULocationID", "DOLocationID"]

# We use memory usage to also calculate the memory needed to read the files
dataset_mem, ray_dataset = memory_usage((read_dataset,(num_years, columns)), retval=True)

print("rows: ", ray_dataset.count())

# Stop counting time and memory for the datasets
finish_time_dataset = t.time()
dataset_time = finish_time_dataset - start_time_dataset

# Start counting time and memory for Counting Triangles
start_time = t.time()

# We rename to edges since we want to use it for graph operations
edges = ray_dataset

@ray.remote
def pagerank_remote(G):
    return nx.pagerank(G, max_iter=10)

def pagerank_fn(edges):

    # Create a directed graph
    G = nx.DiGraph()

    # Insert rows to the created graph
    for edge in edges.iter_rows():
        src, dst = edge["PULocationID"], edge["DOLocationID"]
        G.add_edge(src, dst)
    
    # As pagerank is more complex and memory consuming we need to use ray.put
    # to make Graph G accessible to multiple nodes
    G_ref = ray.put(G)

    # Calls the ray remote for parallel execution
    pagerank = ray.get(pagerank_remote.remote(G_ref))

    return pagerank

# Memory usage is used to calculate the memory needed to perform pagerank
# Results are not returned as we need only time and memory calculations
memory = memory_usage((pagerank_fn,(edges,)),retval=False)

# Stop counting the time and memory
end_time = t.time()

time_it_took = end_time - start_time

# Write the results to a text file
with open('pagerankRayTimes.txt', 'a') as file:
    file.write(
        f"\nRun\n"
        f"- Dataset: taxi {num_years} years, Rows: {ray_dataset.count()} rows\n"
        f"- Dataset Memory: {dataset_mem[-1]} MB\n"
        f"- Time it took to read dataset: {dataset_time} seconds\n"
        f"- Time it took for code: {time_it_took} seconds\n"
        f"- Memory used: {memory[-1]} MB\n"
    )