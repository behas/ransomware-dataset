#!/bin/bash

HDFS_PATH='ENTER HDFS PATH THIS JOB WRITES TO (hdfs://HOST:8020/PATH/'

TARGET_PATH='./dataset/blockchain/'


for file in `hadoop fs -ls $HDFS_PATH | sed '1d;s/  */ /g' | cut -d\  -f8`
do
	filename=`basename $file`
	echo "Copy and merge $filename from HDFS to local..."
	hadoop fs -getmerge $file $TARGET_PATH$filename
done

rm $TARGET_PATH.*.crc
