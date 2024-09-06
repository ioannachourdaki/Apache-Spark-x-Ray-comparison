import ray
import ray.data
import time as t
import networkx as nx
from memory_profiler import memory_usage

# Used to have all available resources: 6 for 2 nodes and 8 for 3
ray.autoscaler.sdk.request_resources(num_cpus=8)

num_years = 1

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
def count_triangles_remote(G):
    
    # Count the number of triangles in the graph
    total_triangles = sum(nx.triangles(G).values()) // 3
    
    return total_triangles

# count_triangles is used to run then on ray remote the graph and calculate the triangles
def count_triangles(edges):

    # Create an undirected graph
    G = nx.Graph()
    
    # Insert rows to the created graph
    for edge in edges.iter_rows():
        src, dst = edge["PULocationID"], edge["DOLocationID"]
        G.add_edge(src, dst)
    
    G_ref = ray.put(G)

    # Calls the ray remote for parallel execution
    total_triangles = ray.get(count_triangles_remote.remote(G))

    print(f"Total number of triangles: {total_triangles}")
    return total_triangles

# Memory usage is used to calculate the memory needed to perform triangle counts
memory, total_triangles = memory_usage((count_triangles,(edges,)), retval=True)

# Stop counting the time and memory
end_time = t.time()
time_it_took = end_time - start_time

# Write the results to a text file
with open('triangleRayTimes.txt', 'a') as file:
    file.write(
        f"\nRun\n"
        f"- Dataset: taxi {num_years} years, Rows: {ray_dataset.count()} rows\n"
        f"- Dataset Memory: {dataset_mem[-1]} MB\n"
        f"- Time it took to read dataset: {dataset_time} seconds\n"
        f"- Time it took for code: {time_it_took} seconds\n"
        f"- Memory used: {memory[-1]} MB\n"
        f"- Total triangles: {total_triangles}\n"
    )