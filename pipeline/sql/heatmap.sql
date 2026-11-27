-- Weekday x half-hour grid of the average Flight of Passage wait (Monday = 0).
SELECT isodow(CAST(park_date AS DATE)) - 1 AS dow,
       (CAST(split_part(local_time, ':', 1) AS INT) * 60 + CAST(split_part(local_time, ':', 2) AS INT)) // 30 AS slot,
       round(avg(wait_min), 1) AS mean,
       count(*) AS n
FROM good
WHERE ride_id = 4439 AND status = 'open'
GROUP BY ALL
ORDER BY dow, slot;
