#!/bin/bash
# Script to stop HDFS and YARN services

echo "Stopping HDFS..."
$HADOOP_HOME/sbin/stop-dfs.sh

echo "Stopping YARN..."
$HADOOP_HOME/sbin/stop-yarn.sh

echo "HDFS and YARN services have been stopped."

