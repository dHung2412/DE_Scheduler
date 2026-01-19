CREATE TABLE IF NOT EXISTS {full_table_name} (
    event_id STRING,
    event_type STRING,
    created_at TIMESTAMP,
    public BOOLEAN,

    actor_id BIGINT,
    actor_login STRING,
    actor_url STRING,         
    actor_avatar_url STRING,

    repo_id BIGINT,
    repo_name STRING,
    repo_url STRING,

    payload_action STRING,
    payload_ref STRING,
    payload_ref_type STRING,

    push_size INT,
    push_distinct_size INT,
    push_head_sha STRING,

    pull_request_number INT,
    pull_request_id BIGINT,
    pull_request_state STRING,
    pull_request_title STRING,
    pull_request_merge BOOLEAN,
    pull_request_merge_at TIMESTAMP,

    issue_number INT,
    issue_title STRING,
    issue_state STRING,

    ingestion_date DATE,
    processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (ingestion_date, event_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)