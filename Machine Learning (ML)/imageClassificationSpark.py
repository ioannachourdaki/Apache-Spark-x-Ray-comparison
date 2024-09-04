from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BinaryType, StringType
import numpy as np
import os
from PIL import Image
import io
import time as t
from memory_profiler import memory_usage

from tensorflow.keras.applications.resnet50 import preprocess_input, decode_predictions
from tensorflow.keras.preprocessing.image import img_to_array
from tensorflow.keras.applications import ResNet50

instances = "3"

# UDF to load images from disk
def load_image(image_path):
    with open(image_path, 'rb') as f:
        return f.read()

load_image_udf = udf(lambda x: load_image(os.path.join(image_dir, x)), BinaryType())

# Load image paths and annotations as a PySpark DataFrame
def read(image_dir,annotations_file):
    annotations_df = spark.read.csv(annotations_file, sep=',', header=True, inferSchema=True)
    # The limit below is used to check the time needed for subsets of the dataset
    #annotations_df = annotations_df.limit(50000)

    # Apply the UDF to load the image data
    annotations_df = annotations_df.withColumn("image_data", load_image_udf(col("image_id")))

    return annotations_df

# Preprocess the image and prepare it for predictions
def preprocess_image(image_data):
    image = Image.open(io.BytesIO(image_data)).resize((224, 224))
    image_array = img_to_array(image)
    preprocessed_image = preprocess_input(image_array)
    return preprocessed_image.tobytes() 

preprocess_image_udf = udf(lambda x: preprocess_image(x), BinaryType())

# Apply the UDF to preprocess the image data
def preprocess(annotations_df):
    annotations_df = annotations_df.withColumn("preprocessed_image", preprocess_image_udf(col("image_data")))

    return annotations_df

# UDF to perform inference using ResNet50
def infer_image_class(image_data):
    # Convert image bytes back to numpy array and reshape
    image_array = np.frombuffer(image_data, dtype=np.float32).reshape((224, 224, 3))
    # Preprocess the image for ResNet50
    image_array = preprocess_input(image_array)
    # Add batch dimension
    image_batch = np.expand_dims(image_array, axis=0)
    # Perform prediction
    predictions = model.predict(image_batch)
    # Decode predictions
    decoded_predictions = decode_predictions(predictions, top=1)[0][0][1]  # Return top predicted label
    return decoded_predictions

infer_image_class_udf = udf(lambda x: infer_image_class(x), StringType())

# Function to do the final predictions
def modelFunction(annotations_df):
    # Apply the UDF to classify the preprocessed images
    classified_df = annotations_df.withColumn("predicted_label", infer_image_class_udf(col("preprocessed_image")))

    classified_df.select("image_id", "predicted_label")

    final_df = classified_df.select("image_id", "predicted_label")

    return final_df

#Initiate spark and reading the dataset
start_dataset = t.time()

spark = SparkSession.builder\
    .appName("imageClassification")\
    .config("spark.sql.debug.maxToStringFields", '100')\
    .config("spark.executor.instances", instances)\
    .getOrCreate()

# URL to the dataset with celebrities
image_dir = "hdfs://master:54310/complex/img_align_celeba/img_align_celeba/"
annotations_file = "/complex/list_attr_celeba.csv"

# Read the files needed
dataset_mem, annotations_df = memory_usage((read, (image_dir,annotations_file)), retval=True)

#End of reading dataset
end_dataset = t.time()
dataset_time = end_dataset - start_dataset
dataset_memory = dataset_mem[-1]

annotations_df.show()

# Initiate the preprocessing
start_preprocess = t.time()

preprocess_mem, annotations_df = memory_usage((preprocess, (annotations_df,)), retval=True)

# End of preprocessing
end_preprocess = t.time()
preprocess_time = end_preprocess - start_preprocess
preprocess_memory = preprocess_mem[-1]

annotations_df.show()

# Initiate the model for predictions
start_model = t.time()

# We load a pretrained model for imagenet
model = ResNet50(weights='imagenet')

model_mem, final_df = memory_usage((modelFunction, (annotations_df,)), retval=True)

# End of model
end_model = t.time()

model_time =end_model - start_model
model_memory = model_mem[-1]

final_df.show()

with open('sweetRelease.txt', 'a') as file:
    # Write text to the file with each item on a new line
    file.write(f"\nRun\n")
    file.write(f"- Dataset: {final_df.count()} rows\n")
    file.write(f"- Number of workers: 2 workers\n")
    file.write(f"- Time it took to read dataset: {dataset_time} seconds\n")
    file.write(f"- Memory used to read dataset: {dataset_memory} MB\n")
    file.write(f"- Preprocess time: {preprocess_time} seconds\n")
    file.write(f"- Preprocess meory: {preprocess_memory} MB\n")
    file.write(f"- Model time: {model_time} seconds\n")
    file.write(f"- Model memory: {model_memory} MB\n")
