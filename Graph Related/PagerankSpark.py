from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from graphframes import GraphFrame
import time as t
from memory_profiler import memory_usage

instances = "3"
num_years = 1


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

def pagerank(df):
    # Keep only the columns 'PULocationID' and 'DOLocationID' as edges
    df_filtered = df.select("PULocationID", "DOLocationID")

    # Create vertices DataFrame by extracting unique nodes
    vertices = df_filtered.select(col("PULocationID").alias("id"))\
        .union(df_filtered.select(col("DOLocationID").alias("id")))\
        .distinct()

    # The edges DataFrame is simply the filtered DataFrame with renamed columns to fit the normal graphs
    edges = df_filtered.withColumnRenamed("PULocationID", "src")\
                    .withColumnRenamed("DOLocationID", "dst")

    # We use GraphFrame for an easier implementation of the counting triangles algorithm
    G = GraphFrame(vertices,edges)
    # We set the parameters for the pagerank in order to much the NetworkX pagerank as much as possible
    results = G.pageRank(resetProbability=0.01, maxIter=5)

    return results

# Start calculating time and memory for dataset reading
start_time_dataset = t.time()

# Create a Spark session
spark = SparkSession.builder.appName("PagerankSpark")\
        .master("yarn")\
        .config("spark.executor.instances", instances)\
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12")\
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
    StructField("DOLocationID", IntegerType(), nullable=False)
])

# Memory_usage is used to calculate the memory used in order to read the dataset
memory, df = memory_usage((read_dataframe,(num_years,taxi_schema)),retval=True)

# Stop counting time and memory for the datasets
finish_time_dataset = t.time()

dataset_time = finish_time_dataset - start_time_dataset
dataset_memory = memory[-1]

# Start counting time and memory for Counting Triangles
start_time_pagerank = t.time()

# Calculate the memory used to perform pagerank. We do not return a value as is takes much space in memory and in our results file
memory = memory_usage((pagerank,(df,)), retval=False)

#Stop counting the time and memory
finish_time_pagerank = t.time()

pagerank_memory = memory[-1]
pagerank_time = finish_time_pagerank - start_time_pagerank

# We write the results in a txt file for easier reading later on
with open('triangleSparkTimes.txt', 'a') as file:
    # Write text to the file, with each entry on a separate line
    file.write(f"\nRun\n")
    file.write(f"- Dataset: {num_years} years and {df.count()} rows\n")
    file.write(f"- Number of workers: {instances} workers\n")
    file.write(f"- Time it took to read dataset: {dataset_time} seconds\n")
    file.write(f"- Memory used to read dataset: {dataset_memory} MB\n")
    file.write(f"- Time it took for code: {pagerank_time} seconds\n")
    file.write(f"- Memory used for code: {pagerank_memory} MB\n")
