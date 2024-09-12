from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import time as t
from memory_profiler import memory_usage
from decimal import Decimal
from sparkmeasure import StageMetrics

def read(taxi_schema, taxizones_schema):
    # We use 4, 8 and 12 months for this experiment
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    years = ["2019"]

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
    
    # Load taxizones DataFrame with schema
    taxizones = spark.read.csv("hdfs:///taxi_drivers/taxizones.csv", header=True, schema=taxizones_schema)

    return df, taxizones  


def preprocess(df, taxizones):
    # Rename columns in taxizones DataFrame for pickup and drop-off zones
    taxizones_pickup = taxizones.withColumnRenamed("LocationID", "PU_LocationID")\
                                .withColumnRenamed("service_zone", "PUZone")
    taxizones_dropoff = taxizones.withColumnRenamed("LocationID", "DO_LocationID")\
                                .withColumnRenamed("service_zone", "DOZone")

    # Join the DataFrames
    df = df.select(
        col("PULocationID"),
        col("DOLocationID"),
        col("shared_request_flag")
    )

    # Perform the join operation
    df = df.join(taxizones_pickup, df.PULocationID == taxizones_pickup.PU_LocationID, how='left')\
        .join(taxizones_dropoff, df.DOLocationID == taxizones_dropoff.DO_LocationID, how='left')

    # Select only the required columns
    df = df.select(
        col("PUZone"),
        col("DOZone"),
        col("shared_request_flag")
    )

    df = df.dropna(subset=['shared_request_flag'])

    # Map features and labels to numbers
    df = df.withColumn('shared_request_flag_numeric',
                    when(df['shared_request_flag'] == 'Y', 1.0)
                    .otherwise(0.0))

    df = df.withColumn('PUZone_numeric',
                    when(df['PUZone'] == 'EWR', 1.0)
                    .when(df['PUZone'] == 'Boro Zone', 2.0)
                    .when(df['PUZone'] == 'Yellow Zone', 3.0)
                    .when(df['PUZone'] == 'Airports', 4.0)
                    .otherwise(0.0))

    df = df.withColumn('DOZone_numeric',
                    when(df['DOZone'] == 'EWR', 1.0)
                    .when(df['DOZone'] == 'Boro Zone', 2.0)
                    .when(df['DOZone'] == 'Yellow Zone', 3.0)
                    .when(df['DOZone'] == 'Airports', 4.0)
                    .otherwise(0.0))


    # Assemble features
    assembler = VectorAssembler(
        inputCols=['PUZone_numeric', 'DOZone_numeric'],
        outputCol='features'
    )

    df = assembler.transform(df)

    return df


def randomForest(df):
    # Define the RandomForestClassifier
    print("Setting model...")
    rf = RandomForestClassifier(labelCol='shared_request_flag_numeric', featuresCol='features', numTrees=5)
    print("Model is set.")

    print("Splitting data...")
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
    print("Data is split!")

    # Train the model
    print("Training...")
    rf_model = rf.fit(train_df)

    print("Predicting...")
    predictions = rf_model.transform(test_df)
    print("Predictions done!")


    # Evaluate the model
    print("Evaluating model...")
    evaluator = MulticlassClassificationEvaluator(
        labelCol='shared_request_flag_numeric', predictionCol='prediction', metricName='accuracy'
    )

    print("Evaluation done.")

    # Extract and print the parameter map
    param_map = rf_model.extractParamMap()
    print(param_map)

    # Calculate accuracy
    accuracy = evaluator.evaluate(predictions)
    print(f"Test Accuracy = {accuracy:.3f}")

    return accuracy


spark = SparkSession.builder \
    .appName("NYC Taxi") \
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

# Define schema for taxizones dataset
taxizones_schema = StructType([
    StructField("LocationID", IntegerType(), nullable=False),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType())
])

# Execution time and Memory usage measurments for reading
start_time = t.time()  

dataset_mem, results = memory_usage((read, (taxi_schema,taxizones_schema)), retval=True)

df = results[0]
taxizones = results[1]

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

preprocess_mem, df = memory_usage((preprocess, (df,taxizones)), retval=True)

# Stop measurements
end_time = t.time()

preproc_memory = preprocess_mem[-1] # In MB
preproc_time = end_time - start_time

print("Preprocessing done!")


### Random Forest Model ###

# Execution time and Memory usage measurments for model 
start_time = t.time()
stagemetrics.begin()

model_mem, accuracy = memory_usage((randomForest, (df)), retval=True)

# Stop measurements
end_time = t.time()

model_memory = model_mem[-1] # In MB
model_time = end_time - start_time

stagemetrics.end()

### Print Measurements ###
print("\n================================ Metrics ================================\n")
stagemetrics.print_report()
print("\n")
print(f"Reading Dataset:\n\tExecution Time: {input_time:.3f}\n\tMemory Usage: {input_memory:.3f}")
print(f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preproc_memory:.3f}")
print(f"Random Forest:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {model_memory:.3f}")
print(f"\nTest Accuracy = {accuracy:.3f}")

# Opening a file in write mode
with open("resultsRandomForest/sparkResults.txt", "w") as file:
    # Writing formatted strings to the file
    file.write(f"Reading Dataset:\n\tExecution Time: {input_time:.3f}\n\tMemory Usage: {input_memory:.3f}\n")
    file.write(f"Preprocessing:\n\tExecution Time: {preproc_time:.3f}\n\tMemory Usage: {preproc_memory:.3f}\n")
    file.write(f"Random Forest:\n\tExecution Time: {model_time:.3f}\n\tMemory Usage: {model_memory:.3f}\n")
