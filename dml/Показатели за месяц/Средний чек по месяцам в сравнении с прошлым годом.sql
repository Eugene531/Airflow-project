SELECT DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM')) AS datetime,
       avg(sum_price * sum_quantity) AS "Средний чек"
FROM dm.transactions_group_ymd
WHERE TO_DATE(date, 'YYYY-MM') >= TO_TIMESTAMP('2022-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND TO_DATE(date, 'YYYY-MM') < TO_TIMESTAMP('2023-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM'))
ORDER BY "Средний чек" DESC
LIMIT 10000;