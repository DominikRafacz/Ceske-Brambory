DROP TABLE event_hits;

CREATE TABLE event_hits (event_id INT, event_messages INT, collected_messages INT, particles_count INT, average_hit_count INT, max_true_particle_momentum DOUBLE) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\;'
LINES TERMINATED BY '\n';