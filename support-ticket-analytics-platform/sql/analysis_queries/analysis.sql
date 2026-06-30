-- Task 1 — Resolution-target compliance rate by priority
SELECT priority, COUNT(*) AS total_tickets, SUM(CASE WHEN resolution_target_met = TRUE THEN 1 ELSE 0 END)  AS met_target,
    ROUND(
        100.0 * SUM(CASE WHEN resolution_target_met = TRUE THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    ) AS compliance_rate_pct
FROM fact_tickets WHERE resolution_time IS NOT NULL GROUP BY priority ORDER BY priority;

-- Task 2 — Top 5 agents by tickets resolved last month
WITH dataset_end AS (
    SELECT MAX(resolution_time) AS max_ts FROM fact_tickets
)
SELECT
    da.agent_name,
    COUNT(ft.ticket_id) AS tickets_resolved
FROM fact_tickets ft
JOIN dim_agents da ON ft.agent_id = da.agent_id, dataset_end
WHERE ft.resolution_time >= DATE_TRUNC('month', dataset_end.max_ts - INTERVAL '1 month')
  AND ft.resolution_time <  DATE_TRUNC('month', dataset_end.max_ts)
GROUP BY da.agent_name
ORDER BY tickets_resolved DESC
LIMIT 5;

-- Task 3 — View: average resolution time by project
CREATE OR REPLACE VIEW view_avg_resolution_by_project AS
SELECT
    dp.project_name,
    COUNT(ft.ticket_id) AS total_tickets,
    ROUND(AVG(ft.resolution_duration_hours), 2) AS avg_resolution_hours
FROM fact_tickets ft
JOIN dim_projects dp ON ft.project_id = dp.project_id
WHERE ft.resolution_duration_hours IS NOT NULL
GROUP BY dp.project_name
ORDER BY avg_resolution_hours;

SELECT * FROM view_avg_resolution_by_project LIMIT 20;


-- Task 4 — Rank agents by avg resolution time (window function)
SELECT
    da.agent_name,
    ROUND(AVG(ft.resolution_duration_hours), 2) AS avg_resolution_hours,
    RANK() OVER (
        ORDER BY AVG(ft.resolution_duration_hours) ASC
    ) AS rank_by_speed
FROM fact_tickets ft
JOIN dim_agents da ON ft.agent_id = da.agent_id
WHERE ft.resolution_duration_hours IS NOT NULL
GROUP BY da.agent_name
ORDER BY rank_by_speed
LIMIT 20;


-- Task 5 — Reporters with more than 5 tickets in a single month
SELECT
    dc.reporter_name,
    DATE_TRUNC('month', ft.start_time) AS ticket_month,
    COUNT(*) AS ticket_count
FROM fact_tickets ft
JOIN dim_customers dc ON ft.customer_id = dc.customer_id
GROUP BY dc.reporter_name, DATE_TRUNC('month', ft.start_time)
HAVING COUNT(*) > 5
ORDER BY ticket_count DESC
LIMIT 20;

-- Task 6 — Running monthly total of resolved tickets (window function)
SELECT
    DATE_TRUNC('month', resolution_time) AS month,
    COUNT(*) AS monthly_resolved,
    SUM(COUNT(*)) OVER (
        ORDER BY DATE_TRUNC('month', resolution_time)
    ) AS running_total
FROM fact_tickets
WHERE resolution_time IS NOT NULL
GROUP BY DATE_TRUNC('month', resolution_time)
ORDER BY month;

-- Task 7 — Categories with ZERO tickets resolved within target (30 days)
WITH dataset_end AS (
    SELECT MAX(start_time) AS max_ts FROM fact_tickets
),
recent_tickets AS (
    SELECT category, resolution_target_met
    FROM fact_tickets, dataset_end
    WHERE start_time >= dataset_end.max_ts - INTERVAL '30 days'
      AND category IS NOT NULL          
)
SELECT DISTINCT category
FROM recent_tickets
WHERE category NOT IN (
    SELECT DISTINCT category
    FROM recent_tickets
    WHERE resolution_target_met = TRUE
)
ORDER BY category;


-- Task 8 — Average resolution time per priority level 
SELECT
    priority,
    ROUND(AVG(resolution_duration_hours), 2) AS avg_resolution_hours,
    COUNT(*)                                  AS total_tickets,
    ROUND(MIN(resolution_duration_hours), 2) AS min_hours,
    ROUND(MAX(resolution_duration_hours), 2) AS max_hours
FROM fact_tickets WHERE resolution_duration_hours IS NOT NULL GROUP BY priority ORDER BY avg_resolution_hours;


-- Task 9 — View: ticket status distribution 
CREATE OR REPLACE VIEW view_status_distribution AS
SELECT status,
    COUNT(*) AS ticket_count,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2
    ) AS percentage FROM fact_tickets WHERE status IS NOT NULL GROUP BY status ORDER BY ticket_count DESC;

SELECT * FROM view_status_distribution;


-- Task 10 — Fastest agent per project (window function) 
SELECT project_name, agent_name, avg_resolution_hours
FROM ( SELECT dp.project_name, da.agent_name, ROUND(AVG(ft.resolution_duration_hours), 2) AS avg_resolution_hours,
        RANK() OVER (
            PARTITION BY dp.project_name
            ORDER BY AVG(ft.resolution_duration_hours) ASC
        ) AS rank_in_project FROM fact_tickets  ft
    JOIN dim_agents    da ON ft.agent_id   = da.agent_id
    JOIN dim_projects  dp ON ft.project_id = dp.project_id
    WHERE ft.resolution_duration_hours IS NOT NULL
    GROUP BY dp.project_name, da.agent_name
) ranked
WHERE rank_in_project = 1
ORDER BY project_name
LIMIT 20;

