-- Carrier workload with completion rates
SELECT
    carrier,
    flowname,
    task_count,
    completed,
    in_progress,
    pending,
    ROUND(completed * 100.0 / NULLIF(task_count, 0), 1) AS completion_rate
FROM report_carrier_workload
ORDER BY task_count DESC;
