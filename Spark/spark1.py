from pyspark.sql import SQLContext, Row, DataFrame
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from httplib2 import Http
import json

key_conv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
value_conv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
hbase_conf = {"hbase.zookeeper.quorum": 'sandbox.hortonworks.com:8000',
              "hbase.mapred.outputtable": 'events',
              "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
              "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
              "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}


# def already_collected_old(event_id, loadedEvents):
#     return loadedEvents.filter(loadedEvents['event_id'] == event_id).first()['collected_messages']

def already_collected(event_id):
    response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'GET')
    status = response[0]['status']
    return int(response[1]) if status == '200' else 0



# def split_row(event_row):
#     row = [
#         event_row['event_id'],
#         already_collected(event_row['event_id']) + len(event_row['data']),
#         event_row['total_messages']
#     ]
#     row.extend([str(event_row['data'][i]) for i in range(len(event_row['data']))])
#     return row
#
#
# def schema_for_event(event):
#     types = [
#         StructField('event_id', IntegerType(), False),
#         StructField('r:collected', IntegerType(), False),
#         StructField('r:total', IntegerType(), False)
#     ]
#     types.extend([StructField('h:' + str(i), StringType(), False)
#                   for i in list(map(lambda hit: hit['hit_id'], event['data']))])
#     schema = StructType(types)
#     return schema

def put_in_hbase(events):
    if events.isEmpty():
        return
    events = events.collect()
    with open('/file.txt', 'a') as myfile:
        myfile.write(str(events) + "\n")
    for event in events:
        with open('/file.txt', 'a') as myfile:
            myfile.write(str(event) + "\n")
        event_id = event['event_id']
        already_collected = already_collected(event_id)
        if already_collected==0:
            Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:total_messages', 'PUT', 
                                    body=event["total_messages"], headers={'content-type':'text/plain'})
        Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'PUT', 
                                    body=str(already_collected + len(event['data'])), headers={'content-type':'text/plain'})
        for hit in event['data']:
            Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'PUT', 
                                    body=hit, headers={'content-type':'text/json'})
        # row = [(event_id, [event_id, 'r', 'collected', already_collected + len(event['data'])]),
        #        (event_id, [event_id, 'r', 'total', event['total_messages']])]
        # row.extend(
            # [(event_id, [event_id, 'h', str(i), val]) for (i, val) in [(hit['hit_id'], hit) for hit in event['data']]]
        # )

        
        

        # status = response[0]['status']
        # sparkContext.parallelize(row).saveAsNewAPIHadoopDataset(hbase_conf, key_conv, value_conv)

    # sqlContext.createDataFrame(Row(row_and_schema[0]), row_and_schema[1]) \
    #     .write\
    #     .options()\
    #     .format("org.apache.spark.sql.execution.datasources.hbase")\
    #     .save()


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 1)
sqlContext = SQLContext(sparkContext)

# loadedEvents = sqlContext.read.format('org.apache.hadoop.hbase.spark') \
#     .option('hbase.table','events') \
#     .option('hbase.columns.mapping', \
#             'event_id STRING :key, \
#             collected_messages STRING r:collected, \
#             total_messages STRING r:total') \
#     .option('hbase.use.hbase.context', False) \
#     .option('hbase.config.resources', 'file:///etc/hbase/conf/hbase-site.xml') \
#     .option('hbase-push.down.column.filter', False) \
#     .load()


kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'events': 1})
kafkaStream\
    .map(lambda event: json.loads(event[1].encode('utf-8')))\
    .foreachRDD(put_in_hbase)


#    .filter(lambda event: not event.isEmpty())\
# .flatMap(lambda event: (split_row(event), schema_for_event(event)))\


streamingContext.start()
