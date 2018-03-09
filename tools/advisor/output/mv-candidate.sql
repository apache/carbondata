SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#61947 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#61950 as decimal(12,2)))), DecimalType(18,2)))` 
FROM
  store_sales
  INNER JOIN customer ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
GROUP BY customer.`c_customer_sk`;

SELECT item.`i_item_desc`, item.`i_item_sk`, item.`i_brand_id`, item.`i_manufact_id`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`, item.`i_category`, date_dim.`d_date`, item.`i_current_price`, item.`i_manager_id`, substring(item.`i_item_desc`, 1, 30) AS `substring(i_item_desc#62177, 1, 30)`, item.`i_class`, date_dim.`d_year`, date_dim.`d_moy`, item.`i_brand`, item.`i_item_id`, count(1) AS `count(1)` 
FROM
  store_sales
  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
GROUP BY item.`i_item_desc`, item.`i_item_sk`, item.`i_brand_id`, item.`i_manufact_id`, item.`i_category`, date_dim.`d_date`, item.`i_current_price`, item.`i_manager_id`, substring(item.`i_item_desc`, 1, 30), item.`i_class`, date_dim.`d_year`, date_dim.`d_moy`, item.`i_brand`, item.`i_item_id`;

