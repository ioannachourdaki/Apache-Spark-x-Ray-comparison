#!/bin/bash

# Define the number of executors to run the jobs with
executors=(2 3)

# Define the Python scripts to run
scripts=("aggregateFilter.py" "sort.py" "transform.py")

# Loop through the scripts
for script in "${scripts[@]}"; do
    for num_executors in "${executors[@]}"; do
        echo "Running $script with $num_executors executors..."
        spark-submit --conf spark.log.level=WARN \
                     --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
                     $script $num_executors
    done
done
