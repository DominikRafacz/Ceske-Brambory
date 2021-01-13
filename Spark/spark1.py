from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from httplib2 import Http
import json
from kafka import SimpleClient, SimpleProducer

kafkaClient = SimpleClient('sandbox.hortonworks.com:6667')
producer = SimpleProducer(kafkaClient)


def get_collected(event_id):
    response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'GET')
    status = response[0]['status']
    return int(response[1]) if status == '200' else 0

def get_corrupted_count(event_id):
    response = Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/c:count', 'GET')
    status = response[0]['status']
    return int(response[1]) if status == '200' else 0

def put_total_hits(event_id, total_hits):
    return Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:total_hits', 'PUT',
                           body=str(total_hits), headers={'content-type': 'application/octet-stream'})

def put_corrupted_columns(event_id, hit_id, corrupted_columns):
    return Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/c:' + str(hit_id), 'PUT',
                           body=str(corrupted_columns), headers={'content-type': 'application/octet-stream'})

def put_corrupted_count(event_id, corrupted_count):
    return Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/c:count', 'PUT',
                       body=str(corrupted_count), headers={'content-type': 'application/octet-stream'})

def put_hit(event_id, hit_id, hit):
    return Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/h:' + str(hit_id), 'PUT',
                           body=str(hit), headers={'content-type': 'application/octet-stream'})

def put_collected(event_id, collected):
    return Http().request('http://sandbox.hortonworks.com:8000/events/' + str(event_id) + '/r:collected', 'PUT',
                       body=str(collected), headers={'content-type': 'application/octet-stream'})

def send_completion_msg_to_kafka_topic(event_id):
    producer.send('successes', str(event_id))
    producer.flush()

def verify_hit(hit):
    corrupted_columns = []
    cols_to_verify = ["tx","ty","tz","tpx","tpy","tpz","weight"]
    for col in cols_to_verify:
        try: float(hit[col])
        except: corrupted_columns.append(col)
    return corrupted_columns

def put_in_hbase(events):
    if events.isEmpty():
        return
    events = events.collect()
    for event in events:
        event_id = event['event_id']
        total_hits = event['total_hits']
        hits = event['data']
        put_total_hits(event_id, total_hits)
        corrupted_count = 0
        for hit in hits:
            hit_id = hit['hit_id']
            corrupted_columns = verify_hit(hit)
            if len(corrupted_columns) != 0:
                corrupted_count += 1
                put_corrupted_columns(event_id, hit_id, corrupted_columns)
            put_hit(event_id, hit_id, hit)
        corrupted_count += get_corrupted_count(event_id)
        put_corrupted_count(event_id, corrupted_count)
        ac = get_collected(event_id)
        new_ac = ac + len(event['data'])
        put_collected(event_id, new_ac)
        if new_ac == event['total_hits'] and get_corrupted_count(event_id) == 0 :
            send_completion_msg_to_kafka_topic(event_id)


sparkContext = SparkContext.getOrCreate()
streamingContext = StreamingContext(sparkContext, 5)
sqlContext = SQLContext(sparkContext)


kafkaStream = KafkaUtils.createStream(streamingContext, 'sandbox.hortonworks.com:2181', 'defaultGroup', {'events': 1})
kafkaStream \
    .map(lambda event: json.loads(event[1].encode('utf-8'))) \
    .foreachRDD(put_in_hbase)


streamingContext.start()
