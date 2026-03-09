CREATE OR REPLACE VIEW v_user_activity AS
WITH session_stats AS (
    SELECT
        user_id,
        COUNT(*) AS session_count,
        ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time)) / 60)::NUMERIC, 1) AS avg_session_minutes,
        SUM(jsonb_array_length(pages_visited)) AS total_pages_visited,
        MIN(start_time) AS first_session,
        MAX(start_time) AS last_session
    FROM user_sessions
    GROUP BY user_id
),
device_stats AS (
    SELECT
        user_id,
        device->>'type' AS device_type,
        COUNT(*) AS device_count
    FROM user_sessions
    GROUP BY user_id, device->>'type'
),
device_dominant AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        device_type AS most_used_device
    FROM device_stats
    ORDER BY user_id, device_count DESC
),
page_stats AS (
    SELECT
        user_id,
        page,
        COUNT(*) AS visit_count
    FROM user_sessions,
         LATERAL jsonb_array_elements_text(pages_visited) AS page
    GROUP BY user_id, page
),
top_page AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        page AS most_visited_page
    FROM page_stats
    ORDER BY user_id, visit_count DESC
)
SELECT
    s.user_id,
    s.session_count,
    s.avg_session_minutes,
    s.total_pages_visited,
    tp.most_visited_page,
    dd.most_used_device,
    s.first_session,
    s.last_session
FROM session_stats s
LEFT JOIN top_page tp ON s.user_id = tp.user_id
LEFT JOIN device_dominant dd ON s.user_id = dd.user_id
ORDER BY s.session_count DESC;

CREATE OR REPLACE VIEW v_support_efficiency AS
SELECT
    issue_type,
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE status = 'open') AS open_count,
    COUNT(*) FILTER (WHERE status = 'in_progress') AS in_progress_count,
    COUNT(*) FILTER (WHERE status = 'resolved') AS resolved_count,
    COUNT(*) FILTER (WHERE status = 'closed') AS closed_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600)::NUMERIC, 1) AS avg_resolution_hours,
    ROUND(AVG(jsonb_array_length(messages))::NUMERIC, 1) AS avg_messages_per_ticket
FROM support_tickets
GROUP BY issue_type
ORDER BY total_tickets DESC;
