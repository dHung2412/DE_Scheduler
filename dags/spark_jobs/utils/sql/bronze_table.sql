CREATE TABLE IF NOT EXISTS {full_table_name} (
    id STRING,
    type STRING,
    
    actor_id BIGINT,
    actor_login STRING,
    actor_gravatar_id STRING,
    actor_url STRING,
    actor_avatar_url STRING,
    
    repo_id BIGINT,
    repo_name STRING,
    repo_url STRING,
    
    payload STRING,
    public BOOLEAN,
    created_at STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE,
    kafka_partition INT,
    kafka_offset BIGINT
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    -- Write optimization
    'write.target-file-size-bytes' = '20971520',  -- 20MB
    'write.distribution-mode' = 'none',
    'write.spark.fanout.enabled' = 'true',
    
    -- Metadata optimization
    'write.metadata.compression-codec' = 'gzip',
    'write.metadata.metrics.default' = 'truncate(16)',
    
    -- Snapshot retention LIMITS (không auto-cleanup)
    'history.expire.min-snapshots-to-keep' = '10',
    'history.expire.max-snapshot-age-ms' = '86400000'  -- 1 day (no trailing comma)
)