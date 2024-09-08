# Guide on downloading the data required

## Download datasets
All the data we used were from Kaggle and we have 2 datasets: one of taxi records for the years 2019-2022 and celebA with images of celebrities. The download happens as following:
```bash
# For the Taxi records
kaggle datasets download -d jeffsinsel/nyc-fhvhv-data

# For celebA images
mkdir complex
cd complex
kaggle datasets download -d jessicali9530/celeba-dataset
```

And then you need to unzip the files properly with the `unzip` command.

## Include to HDFS
The taxi records need to be in HDFS under the taxi_drivers folder, therefore as:
```bash
hdfs dfs -mkdir taxi_drivers
hdfs dfs -mkdir /taxi_drivers/2019 /taxi_drivers/2020 /taxi_drivers/2021
# For the rest of the files in the folders according to their year
hdfs dfs -put /taxi_drivers/{2019, 2020, 2021}/
```

And like that your datasets are ready to be used!

Note: in our program the parquet files are split in directories based on the years they are in, therefore 2019 ones under 2019 folder etc. 
The celebA files were loaded from a local storage in both cases of Spark and Ray. They just need to be under the /data/complex directory. Add it under this directory in all Machines in order for this to work properly.