# Apache-Spark-x-Ray-comparison

## Authors
----------------------------------------------------------------

* Kefalinos Dionysios, 03119030, dion397kef@gmail.com
* Pyliotis Athanasios, 03119050, athan.pyl@gmail.com
* Chourdaki Ioanna, 03119208, ioannaxourdaki@gmail.com


## Contents of this repository
----------------------------------------------------------------
This github contains the programs used to run and compare the frameworks of Apache Spark and Ray through different operations, like ETL operations (filtering, sorting and transforming), Graph operations (Pagerank and Triangle Count) and ML Operations (Kmeans and Random Forest training and Image Classification). There is one folder for each of these containing all the relevant programs along with a guide on how to execute them properly.

There exist also some initiation files in order to start and stop spark (HDFS and Yarn) easier and in order to Start a ray cluster with one of 2 workers. There is also the code named "loadTime" that we used to compare the reading time of our dataset for both frameworks.

Also, there is a folder named "data" which contains a file instructing you how to download the kaggle datasets needed and how to add them to HDFS.
Note: HDFS is only used by Apache Spark, the files will need to remain locally for Ray.

Finally, there is a folder names "installation" which has all the necessary commands for a proper installation of HDFS, Yarn, Ray and the rest of the requirements for this project to work.

Feel free to read the outcomes of our work on the report provide in this Github.