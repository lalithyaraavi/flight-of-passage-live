-- Today's curve: every believable open reading for the five headline rides on the latest park day.
SELECT ride_id, ride, local_time, wait_min
FROM good
WHERE park_date = (SELECT max(park_date) FROM silver)
  AND ride_id IN (4439, 4438, 110, 113, 112)
  AND status = 'open'
ORDER BY ride_id, source_updated_at;
