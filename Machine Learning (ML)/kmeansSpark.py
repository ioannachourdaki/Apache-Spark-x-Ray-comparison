from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from memory_profiler import memory_usage
import time as t
from sparkmeasure import StageMetrics

def read(taxi_schema):

    # Read all files (2019-2022) and concatenate them
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    years = ["2019", "2020", "2021"]

    print("Reading dataset...")

    df = spark.read.parquet(r"hdfs:///taxi_drivers/2019/fhvhv_tripdata_2019-02.parquet",
                            header=True, schema=taxi_schema)


    for year in years:
        for month in months:
            if not (year == "2019" and month == "02"):
                try:
                    dfNew = spark.read.parquet(fr"hdfs:///taxi_drivers/{year}/fhvhv_tripdata_{year}-{month}.parquet",
                                            header=True, schema=taxi_schema)
                    df = df.union(dfNew)
                except Exception:
                    print(f"File {year}-{month} does not exist.")

    return df

def preprocessing(df):
    # Select relevant features
    features = ['driver_pay', 'trip_miles']
    # Get features needed
    df = df.select('driver_pay', 'trip_miles').na.drop()

    # Assemble features into a vector
    print("Setting Assembler...")
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df_assembled = assembler.transform(df)
    print("Assembler is set!")

    return df_assembled

def kmeans_clustering(df_assembled):
    # k-Means for k=3 clusters
    print("Loading model...")
    kmeans = KMeans(featuresCol='features', k=3, seed=42, maxIter=5)
    print("Training...")
    model = kmeans.fit(df_assembled)

    # Predict the clusters
    print("Tranforming...")
    df_clustered = model.transform(df_assembled)
    print("Training done!")

    # Show the results
    df_clustered.select('driver_pay', 'trip_miles', 'prediction').show(10)

    # Evaluate clusters
    print("Evaluating...")
    evaluator = ClusteringEvaluator(featuresCol='features', metricName='silhouette', distanceMeasure='squaredEuclidean')
    silhouette = evaluator.evaluate(df_clustered)

    return silhouette

spark = SparkSession.builder \
    .appName("TaxiTripsKMeans") \
    .config("spark.executor.instances", "3") \
    .getOrCreate()

stagemetrics = StageMetrics(spark)

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
    StructField("airport_fee", DoubleType()),
    StructField("tips", DoubleType()),
    StructField("driver_pay", DoubleType()),
    StructField("shared_request_flag", StringType()),
    StructField("shared_match_flag", StringType()),
    StructField("access_a_ride_flag", StringType()),
    StructField("wav_request_flag", StringType()),
    StructField("wav_match_flag", StringType())
])

# Execution time and Memory usage measurments for reading dataset
start_time = t.time()

dataset_mem, df = memory_usage((read, (taxi_schema,)), retval=True)

# Stop measurements
end_time = t.time()

input_memory = dataset_mem[-1] # In MB
input_time = end_time - start_time

# Calculate number of rows
print(f"Number of rows: {df.count()}")

### Preprocessing ###
print("Preprocessing...")

# Execution time and Memory usage measurments for preprocessing
start_time = t.time()

preprocess_mem, df_assembled = memory_usage((preprocessing, (df,)), retval=True)

# Stop measurements
end_time = t.time()
preproc_memory = preprocess_mem[-1] # In MB
preproc_time = end_time - start_time

print("Preprocessing done!")

### k-Means Clustering ###

# Execution time and Memory usage measurments for model
start_time = t.time()
stagemetrics.begin()

kmeans_mem, silhouette = memory_usage((kmeans_clustering, (df_assembled,)), retval=True)

# Stop measurements
end_time = t.time()

model_memory = kmeans_mem[-1] # In MB
model_time = end_time - start_time

stagemetrics.end()

### Print Measurements ###
print("\n================================ Metrics ================================\n")
stagemetrics.print_report()
print("\n")
print(f"Reading Dataset:\n\tExecution Time: {input_time:.3f}\n\tMemory Usage: {input_memory:.3f}")
print(f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preproc_memory:.3f}")
print(f"kMeans:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {model_memory:.3f}")
print(f'\nSilhouette Score: {silhouette:.3f}')

# Opening a file in write mode
with open("resultsKmeans/sparkResults.txt", "w") as file:
    # Writing formatted strings to the file
    file.write(f"Reading Dataset:\n\tExecution Time: {input_time:.3f}\n\tMemory Usage: {input_memory:.3f}\n")
    file.write(f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preproc_memory:.3f}\n")
    file.write(f"kMeans:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {model_memory:.3f}\n")

