{% macro calculate_activity_score(event_type_col, pr_merged_col, push_commits_col) %}
    CASE
        WHEN {{ event_type_col }} = 'PushEvent' 
            THEN COALESCE({{ push_commits_col }}, 1) * 2
        WHEN {{ event_type_col }} = 'PullRequestEvent' AND {{ pr_merged_col }} = TRUE 
            THEN 10
        WHEN {{ event_type_col }} = 'PullRequestEvent' 
            THEN 5
        WHEN {{ event_type_col }} = 'IssuesEvent' 
            THEN 3
        WHEN {{ event_type_col }} = 'WatchEvent' 
            THEN 1
        WHEN {{ event_type_col }} = 'ForkEvent' 
            THEN 2
        ELSE 1
    END
{% endmacro %}
