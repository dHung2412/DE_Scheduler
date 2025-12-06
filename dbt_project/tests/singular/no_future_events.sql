-- Giữ dữ liệu hợp lệ -> Tránh các record bị lệch timezone, lỗi ETL hoặc input sai làm phá dashboard
SELECT 
    event_id,
    event_type,
    created_at,
    event_date,
    CURRENT_TIMESTAMP() as current_time,
    DATEDIFF(created_at, CURRENT_TIMESTAMP()) as days_in_future
FROM {{ ref('silver_github_events') }}
WHERE created_at > CURRENT_TIMESTAMP()
   OR event_date > CURRENT_DATE()
