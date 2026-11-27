-- One row per park day for Flight of Passage: average, peak, and the best hour to ride.
-- "Best hour" = the 60-minute window (starting at a reading) with the lowest average wait, needing 3+ readings.
WITH fop AS (
    SELECT park_date, local_time, wait_min,
           CAST(split_part(local_time, ':', 1) AS INT) * 60 + CAST(split_part(local_time, ':', 2) AS INT) AS m
    FROM good
    WHERE ride_id = 4439 AND status = 'open'
),
windows AS (
    SELECT *,
           avg(wait_min)   OVER w AS win_avg,
           count(wait_min) OVER w AS win_n
    FROM fop
    WINDOW w AS (PARTITION BY park_date ORDER BY m RANGE BETWEEN CURRENT ROW AND 60 FOLLOWING)
),
best AS (
    SELECT park_date,
           arg_min(local_time, (win_avg, m)) AS best_start,
           min(win_avg)                      AS best_avg
    FROM windows
    WHERE win_n >= 3
    GROUP BY park_date
),
daily AS (
    SELECT park_date,
           count(*)                           AS readings,
           avg(wait_min)                      AS mean,
           max(wait_min)                      AS peak,
           arg_max(local_time, (wait_min, -m)) AS peak_at
    FROM fop
    GROUP BY park_date
    HAVING count(*) >= 3
)
SELECT d.park_date AS date, d.readings, round(d.mean, 1) AS mean, d.peak, d.peak_at,
       b.best_start, round(b.best_avg, 1) AS best_avg
FROM daily d
LEFT JOIN best b USING (park_date)
ORDER BY d.park_date;
