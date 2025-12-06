-- Silver Model: Clean GitHub Events
-- Materialization: Incremental (chỉ process dữ liệu mới)
-- Source: demo.silver.github_events_parsed (từ Spark job)

{{
  config(
    materialized='incremental',
    unique_key='event_id',
    file_format='iceberg',
    incremental_strategy='merge',
    partition_by=['event_date'],
    merge_update_columns=['event_type', 'actor_login', 'repo_name']
  )
}}

WITH source_data AS (
    SELECT
        -- Event identifiers
        event_id,
        event_type,
        created_at,
        CAST(ingestion_date AS DATE) as event_date,
        HOUR(created_at) as event_hour,
        public,
        
        -- Actor information
        actor_id,
        LOWER(TRIM(actor_login)) AS actor_login,
        actor_avatar_url, 
        actor_url,
        
        -- Repository information
        repo_id,
        TRIM(repo_name) AS repo_name,
        repo_url,
        SPLIT(repo_name, '/')[0] AS repo_owner,
        SPLIT(repo_name, '/')[1] AS repo_project,
        
        -- Payload fields
        payload_action,
        payload_ref,
        payload_ref_type,
        
        -- Push event details
        push_size AS push_commits_count,
        push_distinct_size AS push_distinct_count,
        push_head_sha,
        
        -- Pull request details
        pr_id,
        pr_number,
        TRIM(pr_title) AS pr_title,
        pr_state,
        pr_merged,
        pr_merged_at,
        
        -- Issue details
        issue_number,
        TRIM(issue_title) AS issue_title,
        issue_state,
        
        -- Metadata
        processed_at AS processing_timestamp,
        'demo.silver.github_events_parsed' AS source_table
        
    FROM demo.silver.github_events_parsed
    
    WHERE 1=1
        -- Data quality filters
        AND event_id IS NOT NULL
        AND event_type IS NOT NULL
        AND created_at IS NOT NULL
        
        {% if is_incremental() %}
        AND processed_at > (SELECT MAX(processing_timestamp) FROM {{ this }})
        {% endif %}
),

-- Add computed fields using macros
enriched_data AS (
    SELECT
        *,
        
        -- Sử dụng macro cho event category
        {{ categorize_event('event_type') }} AS event_category,
        
        -- Extract branch/tag name
        CASE
            WHEN payload_ref LIKE 'refs/heads/%' THEN SUBSTRING(payload_ref, 12)
            WHEN payload_ref LIKE 'refs/tags/%' THEN SUBSTRING(payload_ref, 11)
            ELSE payload_ref
        END AS branch_or_tag_name,
        
        -- Main branch flag
        CASE
            WHEN payload_ref IN ('refs/heads/main', 'refs/heads/master') THEN TRUE
            ELSE FALSE
        END AS is_main_branch,
        
        -- Sử dụng macro cho activity score
        {{ calculate_activity_score('event_type', 'pr_merged', 'push_commits_count') }} AS activity_score
        
    FROM source_data
)

SELECT * FROM enriched_data
