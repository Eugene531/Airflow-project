SELECT DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM')) AS datetime,
       sum(sum_price) AS "Цена"
FROM dm.transactions_group_ymd
WHERE TO_DATE(date, 'YYYY-MM') >= TO_TIMESTAMP('2021-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND TO_DATE(date, 'YYYY-MM') < TO_TIMESTAMP('2023-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM'))
ORDER BY "Цена" DESC
LIMIT 10000;

SELECT DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM')) AS datetime,
       sum(sum_quantity) AS "Количество"
FROM dm.transactions_group_ymd
WHERE TO_DATE(date, 'YYYY-MM') >= TO_TIMESTAMP('2021-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
  AND TO_DATE(date, 'YYYY-MM') < TO_TIMESTAMP('2023-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.US')
GROUP BY DATE_TRUNC('day', TO_DATE(date, 'YYYY-MM'))
ORDER BY "Количество" DESC
LIMIT 10000;