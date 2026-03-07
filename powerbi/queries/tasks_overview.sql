-- Task volume and completion rate by month
SELECT
    DATE_TRUNC('month', task_date) AS month,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
    ROUND(SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS completion_rate,
    ROUND(AVG(duration_hours), 2) AS avg_duration_hours
FROM report_tasks_full
WHERE task_date IS NOT NULL
GROUP BY DATE_TRUNC('month', task_date)
ORDER BY month;
