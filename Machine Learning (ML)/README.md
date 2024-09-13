# How to run the Machine Learning operations
--------------------------------------------------------------

### Random Forest Classification
We train the Random Forest model on our Taxi dataset for 4, 8 or 12 months and then evaluate it based on the metric Accuracy.

### K-Means Clustering
We train the K-Means model on our Taxi dataset for 1, 2 or 3 years and then evaluate it based on the metric Silhouette Score.

### Image Classification
We use a different dataset, CelebA, along with the ResNet50 model from TensorFlow Keras, to classify images based on their contents.

Note: All results are printed before stopping the execution time measurement, as both Spark UDFs and Ray maps use lazy execution. This means they only produce results
      when explicitly requested, such as when they are printed or used in further operations.


## Spark runs

```bash
spark-submit --conf spark.log.level=WARN --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 --packages ch.cern.sparkmeasure:spark-measure_2.12:0.24 randomForestSpark.py
spark-submit --conf spark.log.level=WARN --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 --packages ch.cern.sparkmeasure:spark-measure_2.12:0.24 kmeansSpark.py
spark-submit --conf spark.log.level=WARN imageClassificationSpark.py {0 < num_lines <= 202559}
```

## Ray runs

The amount of workers is based on your clusters configuration. To run the ray programs you need to run the below commands:
```bash
python3 randomForestRay.py {num_years}
python3 kmeansRay.py 
python3 imageClassificationRay.py {0 < num_lines <= 202559}
```
