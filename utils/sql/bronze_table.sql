CREATE TABLE IF NOT EXISTS {full_table_name} (
    id STRING,
    type STRING,
    actor STRUCT<
        id: BIGINT,
        login: STRING,
        gravatar_id: STRING,
        url: STRING,
        avatar_url: STRING
    >,
    repo STRUCT<
        id: BIGINT,
        name: STRING,
        url: STRING
    >,
    payload STRING,
    public BOOLEAN,
    created_at STRING,
    org STRUCT<
        id: BIGINT,
        login: STRING,
        gravatar_id: STRING, 
        url: STRING,
        avatar_url: STRING
    >,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE,
    kafka_partition INT,
    kafka_offset BIGINT
)
USING iceberg
PARTITIONED BY (ingestion_date)