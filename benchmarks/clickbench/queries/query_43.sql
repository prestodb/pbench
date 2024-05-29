SELECT DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') AS M, COUNT(*) AS PageViews
FROM hits
WHERE CounterID = 62
  AND EventDate >= DATE('2013-07-14')
  AND EventDate <= DATE('2013-07-15')
  AND IsRefresh = 0
  AND DontCountHits = 0
GROUP BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00')
ORDER BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') OFFSET 1000 LIMIT 10;
