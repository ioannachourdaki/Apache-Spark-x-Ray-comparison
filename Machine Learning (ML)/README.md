# How to run the graph operations
--------------------------------------------------------------

### Kmeans
We train a Kmeans model on our Taxi dataset for 1, 2 or 3 years and then evaluate it based on its accuracy.

### Random Forest
We train a Random Forest model on our Taxi dataset for 1, 2 or 3 years and then evaluate it based on the metric silhouette score.

### Image Classification
We use a different dataset (CelebA) with ResNet50 model from tensorflow keras that classifies an image based on the items it has on it.
Note: we print all the results before we stop counting the execution time because Spark UDFs and Ray maps perform lazy execution, therefore only actually
      producting results after we ask for them to be printed or used.


## Spark runs

```bash
spark-submit --conf spark.log.level=WARN --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 --packages ch.cern.sparkmeasure:spark-measure_2.12:0.24 kmeansSpark.py
spark-submit --conf spark.log.level=WARN --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 --packages ch.cern.sparkmeasure:spark-measure_2.12:0.24 randomForestSpark.py
spark-submit --conf spark.log.level=WARN imageClassificationSpark.py {0 < num_lines <= 202559}
```

## Ray runs

The amount of workers is based on your clusters configuration. To run the ray programs you need to run the below commands:
```bash
python3 kmeansRay.py 
python3 randomForestRay.py 
python3 imageClassificationRay.py {0 < num_lines <= 202559}
```