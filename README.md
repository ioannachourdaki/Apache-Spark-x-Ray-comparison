# Apache-Spark-x-Ray-Comparison

## Authors
----------------------------------------------------------------

* Kefalinos Dionysios, 03119030, dion397kef@gmail.com
* Pyliotis Athanasios, 03119050, athan.pyl@gmail.com
* Chourdaki Ioanna, 03119208, ioannaxourdaki@gmail.com


## Repository Contents
----------------------------------------------------------------

This GitHub repository contains code for executing and comparing the performance of Apache Spark and Ray frameworks across various operations. These include ETL tasks (filtering, sorting, and transformation), Graph operations (PageRank and Triangle Count), and Machine Learning tasks (Random Forest training, K-Means clustering, and Image classification). The repository is organized into separate folders for each category, with all relevant programs and a detailed guide on how to properly execute them.

Additionally, the repository includes initialization scripts to simplify starting and stopping Apache Spark (HDFS and Yarn), as well as setting up a Ray cluster with 1 or 2 workers. It also features a script named "loadTime" used to compare dataset loading times between the two frameworks.

There is also a "data" folder that contains instructions for downloading the required Kaggle datasets and adding them to HDFS for Apache Spark.
Note: HDFS is only used by Apache Spark, so for Ray, the datasets will need to remain stored locally.

Finally, there is an "installation" folder that contains all the necessary commands for properly installing HDFS, Yarn, Ray, and the other dependencies required for the project to function correctly.

Feel free to explore the results of our work in the "Ray_vs_Spark_Report.pdf" provided in this GitHub repository, along with the detailed analytical results in the "Results.xlsx" spreadsheet!
