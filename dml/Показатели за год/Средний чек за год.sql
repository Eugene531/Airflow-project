SELECT DATE_TRUNC('year', TO_DATE(year, 'YYYY')) AS "My column",
       sum(total_value) AS "SUM(total_value)"
FROM
  (SELECT SUBSTRING(date, 1, 4) AS year,
          ROUND(SUM(total_gross) / SUM(total_volume), 0) AS total_value
   FROM dm.total_stats
   GROUP BY year
   ORDER BY year) AS virtual_table
GROUP BY DATE_TRUNC('year', TO_DATE(year, 'YYYY'))
LIMIT 50000;