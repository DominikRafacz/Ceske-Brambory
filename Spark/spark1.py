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
        with open('/file.txt', 'a') as myfile:
            event_id = event['event_id']
            myfile.write(str(event_id) + "\n")
            ac = already_collected(event_id)
            if ac == 0:
                response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:total_messages', 'PUT',
                               body=str(event["total_messages"]), headers={'content-type': 'application/octet-stream'})
                myfile.write(response[0]['status'] + "\n")
            new_ac = ac + len(event['data'])
            response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'PUT',
                           body=str(new_ac), headers={'content-type': 'application/octet-stream'})
            myfile.write(response[0]['status'] + "\n")
            for hit in event['data']:
                response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/h:' + str(hit['hit_id']), 'PUT',
                               body=str(hit), headers={'content-type': 'application/octet-stream'})
                myfile.write(response[0]['status'] + "\n")

            if new_ac == event['total_messages']:
                send_completion_msg_to_kafka_topic(event_id)


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 1)
sqlContext = SQLContext(sparkContext)


kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'events': 1})
kafkaStream \
    .map(lambda event: json.loads(event[1].encode('utf-8'))) \
    .foreachRDD(put_in_hbase)


streamingContext.start()
