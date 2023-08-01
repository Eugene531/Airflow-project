SELECT pos AS pos,
       sum(sum_price * sum_quantity) AS "sum(sum_price * sum_quantity)"
FROM dm.transactions_group_ymd
WHERE TO_DATE(date, 'YYYY-MM') >= TO_TIMESTAMP('2021-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND TO_DATE(date, 'YYYY-MM') < TO_TIMESTAMP('2023-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY pos
ORDER BY "sum(sum_price * sum_quantity)" DESC
LIMIT 100;