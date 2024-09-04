#!/bin/bash
# Script to start HDFS and YARN services

echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "HDFS and YARN services have been started."

