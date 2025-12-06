-- Gold Model: User Activity Aggregations
-- Materialization: Table (full refresh daily)
-- Purpose: Analyze user activity patterns and metrics

{{
  config(
    materialized='table',
    file_format='iceberg',
    partition_by=['activity_date']
  )
}}

WITH daily_user_activity AS (
    SELECT
        -- Grouping Keys: Ai làm gì vào ngày nào?
        actor_id,
        actor_login,
        event_date AS activity_date,
        
        -- 1. METRICS CƠ BẢN
        COUNT(*) AS total_events,
        COUNT(DISTINCT event_id) AS unique_events,
        COUNT(DISTINCT repo_id) AS repos_contributed_to,
        
        -- 2. METRICS THEO LOẠI SỰ KIỆN
        SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) AS push_events,
        SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pull_request_events,
        SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) AS issue_events,
        SUM(CASE WHEN event_type = 'IssueCommentEvent' THEN 1 ELSE 0 END) AS issue_comment_events,
        SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) AS watch_events,
        SUM(CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END) AS fork_events,
        SUM(CASE WHEN event_type = 'CreateEvent' THEN 1 ELSE 0 END) AS create_events,
        SUM(CASE WHEN event_type = 'DeleteEvent' THEN 1 ELSE 0 END) AS delete_events,
        
        -- 3. METRICS THEO NHÓM NGHIỆP VỤ
        SUM(CASE WHEN event_category = 'code_change' THEN 1 ELSE 0 END) AS code_change_events,
        SUM(CASE WHEN event_category = 'pull_request' THEN 1 ELSE 0 END) AS pull_request_related_events,
        SUM(CASE WHEN event_category = 'issue' THEN 1 ELSE 0 END) AS issue_related_events,
        SUM(CASE WHEN event_category = 'social' THEN 1 ELSE 0 END) AS social_events,
        
        -- 4. CHẤT LƯỢNG ĐÓNG GÓP
        SUM(COALESCE(push_commits_count, 0)) AS total_commits,
        SUM(CASE WHEN pr_merged = TRUE THEN 1 ELSE 0 END) AS merged_pull_requests,
        SUM(CASE WHEN is_main_branch = TRUE THEN 1 ELSE 0 END) AS main_branch_events,
        
        -- 5. ĐIỂM HOẠT ĐỘNG
        SUM(activity_score) AS total_activity_score,
        AVG(activity_score) AS avg_activity_score,
        
        -- Public vs private
        SUM(CASE WHEN public = TRUE THEN 1 ELSE 0 END) AS public_events,
        SUM(CASE WHEN public = FALSE THEN 1 ELSE 0 END) AS private_events,
        
        -- 6. THÓI QUEN LÀM VIỆC
        MODE(event_hour) AS most_active_hour
        
    FROM {{ ref('silver_github_events') }}
    GROUP BY actor_id, actor_login, event_date
),

-- Top repositories per user (per day)
top_repos_per_user AS (
    SELECT
        -- Grouping Keys: Ai đóng góp vào repository nào vào ngày nào?
        actor_id,
        activity_date,
        repo_name AS top_repo,
        event_count,
        row_num
    FROM (
        SELECT
            actor_id,
            event_date AS activity_date,
            repo_name,
            COUNT(*) AS event_count,
            ROW_NUMBER() OVER (PARTITION BY actor_id, event_date ORDER BY COUNT(*) DESC) AS row_num
        FROM {{ ref('silver_github_events') }}
        GROUP BY actor_id, event_date, repo_name
    )
    WHERE row_num = 1
),

-- User activity trends (7-day rolling average)
user_trends AS (
    SELECT
        actor_id,
        actor_login,
        activity_date,
        total_events,
        
        -- Rolling averages
        AVG(total_events) OVER (
            PARTITION BY actor_id 
            ORDER BY activity_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS events_7day_avg,
        
        AVG(total_commits) OVER (
            PARTITION BY actor_id 
            ORDER BY activity_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS commits_7day_avg,
        
        -- Cumulative totals
        SUM(total_events) OVER (
            PARTITION BY actor_id 
            ORDER BY activity_date
        ) AS cumulative_events,
        
        SUM(total_commits) OVER (
            PARTITION BY actor_id 
            ORDER BY activity_date
        ) AS cumulative_commits
        
    FROM daily_user_activity
),

-- Final aggregation
final AS (
    SELECT
        dua.*,
        
        -- Top repository
        tr.top_repo,
        tr.event_count AS top_repo_event_count,
        
        -- Trends
        ut.events_7day_avg,
        ut.commits_7day_avg,
        ut.cumulative_events,
        ut.cumulative_commits,
        
        -- User classification
        CASE
            WHEN dua.total_activity_score >= 50 THEN 'highly_active'
            WHEN dua.total_activity_score >= 20 THEN 'active'
            WHEN dua.total_activity_score >= 5 THEN 'moderate'
            ELSE 'low_activity'
        END AS user_activity_level,
        
        -- Specialization
        CASE
            WHEN dua.code_change_events > (dua.total_events * 0.6) THEN 'developer'
            WHEN dua.pull_request_related_events > (dua.total_events * 0.4) THEN 'reviewer'
            WHEN dua.issue_related_events > (dua.total_events * 0.4) THEN 'issue_manager'
            WHEN dua.social_events > (dua.total_events * 0.4) THEN 'community_member'
            ELSE 'generalist'
        END AS user_specialization,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS created_at
        
    FROM daily_user_activity dua
    LEFT JOIN top_repos_per_user tr 
        ON dua.actor_id = tr.actor_id 
        AND dua.activity_date = tr.activity_date
    LEFT JOIN user_trends ut
        ON dua.actor_id = ut.actor_id
        AND dua.activity_date = ut.activity_date
)

SELECT * FROM final
