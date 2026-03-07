-- Department performance: volume, speed, completion
SELECT
    division,
    team,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN taskstatus = 'Completed' THEN 1 ELSE 0 END) AS completed,
    ROUND(AVG(duration_hours), 2) AS avg_hours,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hours), 2) AS median_hours
FROM report_tasks_full
WHERE division IS NOT NULL
GROUP BY division, team
ORDER BY total_tasks DESC;
