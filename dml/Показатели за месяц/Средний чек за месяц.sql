SELECT DATE_TRUNC('month', datetime) AS datetime,
       sum(avg_check) AS "SUM(avg_check)"
FROM
  (SELECT TO_DATE(date, 'YYYY-MM') as datetime,
          avg_check
   FROM total_stats
   ORDER BY datetime) AS virtual_table
WHERE datetime >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
  AND datetime < TO_DATE('2023-01-01', 'YYYY-MM-DD')
GROUP BY DATE_TRUNC('month', datetime)
LIMIT 50000;