SELECT DATE_TRUNC('month', datetime) AS datetime,
       sum(total_gross) AS "SUM(total_gross)"
FROM
  (SELECT TO_DATE(date, 'YYYY-MM') as datetime,
          total_gross,
          (total_gross - prev_value) / prev_value AS difference
   FROM
     (SELECT date, total_gross,

        (SELECT total_gross
         FROM total_stats t2
         WHERE t2.date < t1.date
         ORDER BY t2.date DESC
         LIMIT 1) AS prev_value
      FROM total_stats t1) AS subquery
   ORDER BY date) AS virtual_table
WHERE datetime >= TO_DATE('2021-01-01', 'YYYY-MM-DD')
  AND datetime < TO_DATE('2023-01-01', 'YYYY-MM-DD')
GROUP BY DATE_TRUNC('month', datetime)
LIMIT 50000;