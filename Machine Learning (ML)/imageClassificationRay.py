import ray
import ray.data
import numpy as np
from PIL import Image
import io
import time as t
import os
import argparse
import pandas as pd
from memory_profiler import memory_usage

# Needed for the model we will use, ResNet50
from tensorflow.keras.applications import ResNet50
from tensorflow.keras.applications.resnet50 import preprocess_input, decode_predictions
from tensorflow.keras.preprocessing.image import img_to_array

dataset_start = t.time()

# Custom function to validate that the number of lines is within the valid range
def valid_lines(value):
    ivalue = int(value)
    if ivalue < 1 or ivalue > 202559:
        raise argparse.ArgumentTypeError(f"Number of lines must be between 1 and 202559. You provided {ivalue}.")
    return ivalue

# Set up the argument parser
parser = argparse.ArgumentParser(description='Process the number of lines to read from the dataset.')
parser.add_argument('lines', type=valid_lines, help='The number of lines must be between 1 and 202559.')
args = parser.parse_args()

# Store the validated number of lines
lines = args.lines

# Number of Ray workers that we have
workers = len(ray.nodes())
# 202559

# Initialize Ray
ray.init(ignore_reinit_error=True)

# Paths needed for the directory of the images and the annotation file
image_dir = "/home/taxi_drivers/data/complex/img_align_celeba/img_align_celeba/"
annotations_file = "/home/taxi_drivers/data/complex/list_attr_celeba.csv"

# Function that loads images from the paths provided
def load_image(image_path):
    with open(image_path, 'rb') as f:
        return f.read()

# Used to read the annotations file and all the images
def read(image_dir,annotations_file):
    # Load annotations using Ray Data
    annotations_df = pd.read_csv(annotations_file)

    # Used to limit the number of lines that we perform the prediction for
    annotations_df = annotations_df.head(lines)

    # Create a list with all the image paths
    print("Create image paths")
    image_paths = [os.path.join(image_dir, img_id) for img_id in annotations_df['image_id']]

    # Execute the image loading
    print("Load images...")
    image_data_results = [load_image(image_path) for image_path in image_paths]
    print("Images loaded!")

    # Add the image data to the DataFrame
    annotations_df['image_data'] = image_data_results

    return annotations_df

# Memory usage helps us read all the data we need and also calculates the memory needed
dataset_mem, annotations_df = memory_usage((read,(image_dir,annotations_file)), retval=True)

print(annotations_df)

# Stop counting time and memory for the dataset read
dataset_end = t.time()
dataset_time = dataset_end - dataset_start
dataset_memory = dataset_mem[-1]

# Start calculating time for the preprocessing
preprocess_start = t.time()

# Used to make images into hex upper
def hexify(image_data):
    hex_data = image_data.hex().upper()
    return hex_data

# Make the images into Hex files
@ray.remote
def process_image_row(image_data):
    return hexify(image_data)

# Do the proper preprocessing needed for the image to be ready for the prediction
@ray.remote
def preprocess_image_remote(image_hex):
    image = Image.open(io.BytesIO(bytes.fromhex(image_hex)))
    resized_image = image.resize((224, 224))
    image_array = img_to_array(resized_image)
    preprocessed_image = preprocess_input(image_array)
    return preprocessed_image.tobytes()

# Function that executes map that preprocesses the images using ray remote. It only keeps the image_id 
# and the preprocessed_image columns that we need
def preprocess_images_dataset(dataset):
    # Map over the dataset to preprocess images. We do not specify number of CPUs so that it scales properly
    return dataset.map(lambda row: {
        'image_id': row['image_id'],
        'preprocessed_image': ray.get(preprocess_image_remote.remote(row['image_hex']))
    }, concurrency=workers)

def preprocess(annotations_df):
    
    # images in HEX
    image_hex_results = ray.get([process_image_row.remote(row['image_data']) for _, row in annotations_df.iterrows()])

    # Add image hex in the annotations dataframe
    annotations_df['image_hex'] = image_hex_results
    del image_hex_results
    
    print(annotations_df.head())
    print(len(annotations_df))

    # Turn the annotations dataframe into a ray dataset for faster prediction
    annotations_ds = ray.data.from_pandas(annotations_df)

    print(annotations_ds)

    # Run the preprocessing in the annotations dataset
    print("Preprocessing images...")
    preprocessed_images_ds = preprocess_images_dataset(annotations_ds)
    print("Preprocessing done!")

    return preprocessed_images_ds

# memory usage helps us run our preprocessing and calculate the memory it requires to happen.
preprocess_mem, preprocessed_images_ds = memory_usage((preprocess,(annotations_df,)), retval=True)

# Stop counting time and memory for the preprocessing
preprocess_end = t.time()
preprocess_time = preprocess_end - preprocess_start
preprocess_memory = preprocess_mem[-1]

# Start counting time for the prediction
prediction_start = t.time()

# Class made to help us run map with our resNet model
class ResNetPredictor:
    def __init__(self):
        # Initialize a pre-trained ResNet50 model.
        self.model = ResNet50(weights='imagenet')

    # Logic for inference on each row of data.
    def __call__(self, row: dict[str, str]) -> dict[str, str]:
                
        preprocessed_image = row["preprocessed_image"]
        # Reshape the image properly into an array
        image_array = np.frombuffer(preprocessed_image, dtype=np.float32).reshape((224, 224, 3))
        
        # Perform inference on the image array after we expand the dimention in order to fit to the expected one.
        image_batch = np.expand_dims(image_array, axis=0)

        # Run prediction for the image
        predictions = self.model.predict(image_batch)
        
        # Decode the predictions to get the class names.
        decoded_predictions = decode_predictions(predictions, top=1)[0][0][1] # for prediction in predictions]

        # Store the predicted outputs in the prediction column        
        row["prediction"] = decoded_predictions

        return row

def prediction(preprocessed_images_ds):
    # Perform inference
    print("Performing inference...")

    # Map is used to run the ResNetPredictor for each row. In total we use 2-3 workers and each has to use 2 CPUs    
    predictions_ds = preprocessed_images_ds.map(ResNetPredictor, num_cpus=2 ,concurrency=workers)

    print("Inference complete!")

    print(predictions_ds)
    # Select the columns we want for the prediction
    predictions_ds = predictions_ds.select_columns(["image_id","prediction"])
    
    # Print everyything there is on the prediction dataset. As Ray uses lazy running this is needed to calculate the real execution time
    print(predictions_ds.take_all())

    return predictions_ds

# Used to calculate the memory needed for the predictions to happen. We do not return results as they are printed
prediction_mem = memory_usage((prediction,(preprocessed_images_ds,)), retval=False)

# Stop counting time and memory for the prediction part
prediction_end = t.time()
prediction_time = prediction_end - prediction_start
prediction_memory = prediction_mem[-1]


# We write the results in a txt file for easier reading later on
with open('imageClassificationRayTimes.txt', 'a') as file:
    # Write text to the file, with each entry on a separate line
    file.write(f"\nRun\n")
    file.write(f"- Dataset: {lines} rows\n")
    file.write(f"- Number of workers: 3 workers\n")
    file.write(f"- Time it took to read dataset: {dataset_time} seconds\n")
    file.write(f"- Memory used to read dataset: {dataset_memory} MB\n")
    file.write(f"- Time it took for preprocessing: {preprocess_time} seconds\n")
    file.write(f"- Memory used for preprocessing: {preprocess_memory} MB\n")
    file.write(f"- Time it took for prediction: {prediction_time} seconds\n")
    file.write(f"- Memory used for prediction: {prediction_memory} MB\n")