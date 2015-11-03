DROP KEYSPACE IF EXISTS killrlog_ks;
CREATE KEYSPACE killrlog_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 2};

USE killrlog_ks;

CREATE TABLE IF NOT EXISTS logs (
 source_id TIMEUUID,
 bucket_ts TIMESTAMP,
 ts TIMESTAMP,
 id UUID,
 type TEXT,
 tags MAP<TEXT,TEXT>,
 timestamps MAP<TIMESTAMP, TEXT>,
 raw TEXT,
 PRIMARY KEY ((source_id, bucket_ts),  ts, id)
)
WITH default_time_to_live = 26780400;


-- 1 hour partitions
-- 5k cells per partition
CREATE TABLE IF NOT EXISTS counters (
 source_id TIMEUUID,
 serie_id TEXT,
 bucket_ts TIMESTAMP,
 ts TIMESTAMP,
 id TEXT,
 count INT,
 PRIMARY KEY ((source_id, serie_id, bucket_ts),  ts, id)
);


-- 1 day partition
-- 24 cells per partition
CREATE TABLE IF NOT EXISTS counter_rollups_1h (
 source_id TIMEUUID,
 serie_id TEXT,
 bucket_ts TIMESTAMP,
 ts TIMESTAMP,
 count INT,
 PRIMARY KEY ((source_id, serie_id, bucket_ts),  ts)
);


-- 1 month partitions
-- 30 cells per partition
CREATE TABLE IF NOT EXISTS counter_rollups_1d (
 source_id TIMEUUID,
 serie_id TEXT,
 bucket_ts TIMESTAMP,
 ts TIMESTAMP,
 count INT,
 PRIMARY KEY ((source_id, serie_id, bucket_ts),  ts)
);

-- 1 year partitions
-- 12 cells per partition
CREATE TABLE IF NOT EXISTS counter_rollups_1m (
 source_id TIMEUUID,
 serie_id TEXT,
 bucket_ts TIMESTAMP,
 ts TIMESTAMP,
 count INT,
 PRIMARY KEY ((source_id, serie_id, bucket_ts),  ts)
);
