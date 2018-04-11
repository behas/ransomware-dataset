#!/bin/sh

# echo "Updating to most recent code."
# git pull

echo "Cleaning up previous builds."
sbt clean package

echo "Purging all CSV files from HDFS"
hadoop fs -rm -R /YOUR_HDFS_PATH/*.csv

echo "Copying seed address file to HDFS"
hadoop fs -put data/*.csv /YOUR_HDFS_PATH/

echo "Executing SPARK Job"

$SPARK_HOME/bin/spark-submit \
	--class "at.ac.ait.ExtractionJob" \
	--master "YOUR SPARK MASTER" \
  	--conf spark.executor.memory="180g" \
  	--conf spark.cassandra.connection.host="YOUR CASSANDRA HOSTS" \
  	--packages datastax:spark-cassandra-connector:2.0.3-s_2.11 \
  	target/scala-2.11/ransomware-dataset_2.11-0.1.jar

exit $?
