SELECT name_short AS name_short,
       SUM(sum_price * sum_quantity) AS "Прибыль"
FROM dm.transactions_group_ymd
WHERE TO_DATE(date, 'YYYY-MM') >= TO_TIMESTAMP('2021-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND TO_DATE(date, 'YYYY-MM') < TO_TIMESTAMP('2023-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY name_short
ORDER BY "Прибыль" ASC
LIMIT 5;