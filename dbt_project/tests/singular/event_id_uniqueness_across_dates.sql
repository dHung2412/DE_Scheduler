-- Kiểm tra event_id phải global unique, không chỉ unique trong ngày
WITH duplicate_event_ids AS (
    SELECT 
        event_id,
        COUNT(*) as occurrence_count,
        COUNT(DISTINCT event_date) as date_count,
        MIN(event_date) as first_date,
        MAX(event_date) as last_date
    FROM {{ ref('silver_github_events') }}
    GROUP BY event_id
    HAVING COUNT(*) > 1
)

SELECT 
    event_id,
    occurrence_count,
    date_count,
    first_date,
    last_date
FROM duplicate_event_ids
