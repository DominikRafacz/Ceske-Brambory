#!/bin/bash
export JAVA_TOOL_OPTIONS="-Dhttps.protocols=TLSv1.2"
pip install httplib2

sed -i 's/log4j.rootCategory=[A-Z]*, console/log4j.rootCategory=WARN, console/' /usr/hdp/current/spark-client/conf/log4j.properties

#pyspark --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0,org.apache.hbase:hbase-spark:2.0.0-alpha-1,com.databricks:spark-csv_2.10:1.5.0,org.apache.spark:spark-streaming-kafka-assembly_2.10:1.6.3
