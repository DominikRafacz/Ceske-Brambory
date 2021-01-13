DROP TABLE event_hits;

CREATE TABLE event_hits (hit_id STRING, particle_id STRING, tx DOUBLE, ty DOUBLE, tz DOUBLE, tpx DOUBLE, tpy DOUBLE, tpz DOUBLE, weight DOUBLE, event_id STRING)
STORED AS AVRO;