from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from httplib2 import Http
import json


def already_collected(event_id):
    response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'GET')
    status = response[0]['status']
    return int(response[1]) if status == '200' else 0


def send_completion_msg_to_kafka_topic(event_id):
    pass


def put_in_hbase(events):
    if events.isEmpty():
        return
    events = events.collect()
    for event in events:
        event_id = event['event_id']
        response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:total_hits', 'PUT',
                           body=str(event["total_hits"]), headers={'content-type': 'application/octet-stream'})
        for hit in event['data']:
            response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/h:' + str(hit['hit_id']), 'PUT',
                           body=str(hit), headers={'content-type': 'application/octet-stream'})
        ac = already_collected(event_id)
        new_ac = ac + len(event['data'])
        response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'PUT',
                       body=str(new_ac), headers={'content-type': 'application/octet-stream'})
        if new_ac == event['total_hits']:

            with open("/file.txt", "a") as f:
                f.write("siemaaaa " + str(event_id))
            send_completion_msg_to_kafka_topic(event_id)


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 5)
sqlContext = SQLContext(sparkContext)


kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'events': 1})
kafkaStream \
    .map(lambda event: json.loads(event[1].encode('utf-8'))) \
    .foreachRDD(put_in_hbase)


streamingContext.start()
