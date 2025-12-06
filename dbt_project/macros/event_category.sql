{% macro categorize_event(event_type_column) %}
    CASE
        WHEN {{ event_type_column }} IN ('PushEvent') THEN 'code_change'
        WHEN {{ event_type_column }} IN ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent') THEN 'pull_request'
        WHEN {{ event_type_column }} IN ('IssuesEvent', 'IssueCommentEvent') THEN 'issue'
        WHEN {{ event_type_column }} IN ('WatchEvent', 'ForkEvent') THEN 'social'
        WHEN {{ event_type_column }} IN ('CreateEvent', 'DeleteEvent') THEN 'repo_management'
        WHEN {{ event_type_column }} IN ('ReleaseEvent') THEN 'release'
        ELSE 'other'
    END
{% endmacro %}
