from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

def populate_hive_table(event_ids, hiveContext):
    if event_ids.isEmpty():
        return
    event_ids = event_ids.collect()
    for event_id in event_ids:
        with open("/file.txt", "w") as f:
            schema = StructType([
                StructField("hit_id", StringType(), True),
                StructField("particles_id", StringType(), True),
                StructField("tx", DoubleType(), True),
                StructField("ty", DoubleType(), True),
                StructField("tz", DoubleType(), True),
                StructField("tpx", DoubleType(), True),
                StructField("tpy", DoubleType(), True),
                StructField("tpz", DoubleType(), True),
                StructField("weight", DoubleType(), True)])
            df = hiveContext.read.format('com.databricks.spark.csv').options(header='false', inferschema='false').schema(schema).load("hdfs://sandbox.hortonworks.com:8020/user/brambory/events-raw/" + event_id + "/*")
            df_with_event_id = df.withColumn("event_id", lit(event_id))
            df_with_event_id.write.mode("append").saveAsTable("event_hits")

        # with open("/file.txt", "w") as f:
        #
        #     c = sparkContext.sql.csv("hdfs://sandbox.hortonworks.com:8020/user/brambory/events-raw/" + event_id + "/*").count()
        #     f.write(str(c) + "\n")


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 5)
hiveContext = HiveContext(sparkContext)

kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'successes': 1})
kafkaStream \
    .map(lambda x: x[0].encode("utf-8")) \
    .foreachRDD(lambda x: populate_hive_table(x, hiveContext))


streamingContext.start()
