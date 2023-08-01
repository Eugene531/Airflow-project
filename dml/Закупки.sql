SELECT product_id AS "ID продукта",
       category AS "Категория",
       name_short AS "Наименование",
       sold_total AS "Продано",
       stock_total AS "Остатки на складах",
       update_date_in_stock AS "Дата обновления на складе",
       cost_per_item AS "Себестоимость"
FROM dm.purchases
ORDER BY "ID продукта" DESC
LIMIT 1000;