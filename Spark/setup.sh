#!/bin/bash
export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"
pip install httplib2

#pyspark --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0,org.apache.hbase:hbase-spark:2.0.0-alpha-1
