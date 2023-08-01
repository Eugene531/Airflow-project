SELECT DATE_TRUNC('year', TO_DATE(year, 'YYYY-MM')) AS "My column",
       sum(total_value) AS "SUM(total_value)"
FROM
  (SELECT SUBSTRING(date, 1, 4) AS year,
          SUM(total_gross) AS total_value
   FROM dm.total_stats
   GROUP BY year
   ORDER BY year) AS virtual_table
GROUP BY DATE_TRUNC('year', TO_DATE(year, 'YYYY-MM'))
LIMIT 50000;