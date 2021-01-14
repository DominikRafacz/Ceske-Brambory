from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import functions as F
from pyspark.sql.types import *

def populate_hive_table(event_ids, hiveContext):
    if event_ids.isEmpty():
        return
    event_ids = event_ids.collect()
    for event_id in event_ids:
        #with open("/file.txt", "w") as f:
        hits_schema = StructType([
            StructField("hit_id", StringType(), True),
            StructField("particle_id", StringType(), True),
            StructField("tx", DoubleType(), True),
            StructField("ty", DoubleType(), True),
            StructField("tz", DoubleType(), True),
            StructField("tpx", DoubleType(), True),
            StructField("tpy", DoubleType(), True),
            StructField("tpz", DoubleType(), True),
            StructField("weight", DoubleType(), True)])
        stats_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("particles_count", IntegerType(), True),
            StructField("hits_per_particle", DoubleType(), True),
            StructField("furthest_hit_distance", DoubleType(), True),
            StructField("mean_absolute_momentum", DoubleType(), True)
        ])
        hits_df = hiveContext\
            .read\
            .format('com.databricks.spark.csv')\
            .options(header='false', inferschema='false')\
            .schema(hits_schema)\
            .load("hdfs://sandbox.hortonworks.com:8020/user/brambory/events-raw/" + event_id + "/*")
        hits_df\
            .withColumn("event_id", F.lit(event_id))\
            .write\
            .mode("append")\
            .insertInto("event_hits")
        particles_count = hits_df.select('particle_id').distinct().count()
        hits_per_particle = float(hits_df.count()) / particles_count
        aggregates = hits_df\
            .withColumn("distance", F.sqrt(hits_df['tx'] * hits_df['tx'] + hits_df['ty'] * hits_df['ty'] + hits_df['tz'] * hits_df['tz']))\
            .withColumn("momentum", F.sqrt(hits_df['tpx'] * hits_df['tpx'] + hits_df['tpy'] * hits_df['tpy'] + hits_df['tpz'] * hits_df['tpz']))\
            .agg(F.max('distance').alias('furthest_hit_distance'), F.mean('momentum').alias('mean_absolute_momentum'))\
            .collect()[0]
        hiveContext\
            .createDataFrame([(event_id, particles_count, hits_per_particle, aggregates.furthest_hit_distance, aggregates.mean_absolute_momentum)],
                             stats_schema)\
            .write\
            .mode('append')\
            .insertInto('event_stats')


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 5)
hiveContext = HiveContext(sparkContext)

kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'successes': 1})
kafkaStream \
    .map(lambda x: x[0].encode("utf-8")) \
    .foreachRDD(lambda x: populate_hive_table(x, hiveContext))


streamingContext.start()
