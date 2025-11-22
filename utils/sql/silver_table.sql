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

    org_id BIGINT,
    org_login STRING,

    payload_action STRING,
    payload_ref STRING,
    payload_ref_type STRING,

    push_size INT,            -- Số lượng commit trong lần push này
    push_distinct_size INT,   -- Số commit thực tế (không phải merge commit)
    push_head_sha STRING,     -- Hash mới nhất

    pr_number INT,
    pr_id BIGINT,
    pr_state STRING,          -- open/closed
    pr_title STRING,
    pr_merged BOOLEAN,        -- True = Code đã được chấp nhận (Quan trọng)
    pr_merged_at TIMESTAMP,   -- Thời điểm được chấp nhận

    issue_number INT,
    issue_title STRING,
    issue_state STRING,

    ingestion_date DATE,      -- Partition Key
    processed_at TIMESTAMP    -- Thời điểm chạy job ETL
)
USING iceberg
PARTITIONED BY (ingestion_date, event_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
)