# How to run the Graph Operations
--------------------------------------------------------------

### Pagerank
Pagerank executes the pagerank algorithm on a set of edges from the taxi dataset to see which locations are more popular

### Triangle Count
Triangle Count is counting the trinagles in the edges representing all the trips the taxis had taken customers

## Spark runs

```bash
spark-submit --conf spark.log.level=WARN --packages graphframes:graphframes:0.8.2-spark3.0-s_2.12 pagerankSpark.py {num_years=1,2,3} {num_workers=2,3}
spark-submit --conf spark.log.level=WARN --packages graphframes:graphframes:0.8.2-spark3.0-s_2.12 triangleCountSpark.py {num_years=1,2,3} {num_workers=2,3}
```

## Ray runs

The amount of workers is based on your clusters configuration. To run the ray programs you need to run the below commands:
```bash
python3 pagerankRay.py {num_years=1,2,3}
python3 triangleCountRay {num_years=1,2,3}
```
