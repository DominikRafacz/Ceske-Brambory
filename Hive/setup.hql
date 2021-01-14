DROP TABLE event_hits;

CREATE TABLE event_hits (hit_id STRING, particle_id STRING, tx DOUBLE, ty DOUBLE, tz DOUBLE, tpx DOUBLE, tpy DOUBLE, tpz DOUBLE, weight DOUBLE, event_id STRING)
STORED AS AVRO;

DROP TABLE event_stats;

CREATE TABLE event_stats (event_id STRING, particles_count INT, hits_per_particle DOUBLE, furthest_hit_distance DOUBLE, mean_absolute_momentum DOUBLE)
STORED AS AVRO;