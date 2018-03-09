/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.mv.rewrite.matching

object TestTPCDS_1_4_Batch {
  val tpcds_1_4_testCases = Seq(
      // sequence of triples.  each triple denotes (MV, user query, rewritten query)
      // test case 1: test SELECT-SELECT-EXACT_MATCH with simple SELECT (extract from q45)
      ("case_1",
       """
        |SELECT i_item_id, i_item_sk
        |FROM item
        |WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
        """.stripMargin.trim,
       """
        |SELECT i_item_id
        |FROM item
        |WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19)
        """.stripMargin.trim,
       """
        |SELECT
        |FROM 
        |WHERE 
        """.stripMargin.trim),
      // test case 2: test SELECT-SELECT-EXACT_MATCH with SELECT containing join (derive from q64)
      ("case_2",
       """
        |SELECT cs1.product_name, cs1.store_name, cs1.store_zip, cs1.b_street_number,
        |       cs1.b_streen_name, cs1.b_city, cs1.b_zip, cs1.c_street_number, cs1.c_street_name,
        |       cs1.c_city, cs1.c_zip, cs1.syear, cs1.cnt, cs1.s1, cs1.s2, cs1.s3, cs2.s1,
        |       cs2.s2, cs2.s3, cs2.syear, cs2.cnt
        |FROM cross_sales cs1,cross_sales cs2
        |WHERE cs1.item_sk=cs2.item_sk AND
        |     cs1.syear = 1999 AND
        |     cs2.syear = 1999 + 1 AND
        |     cs2.cnt <= cs1.cnt AND
        |     cs1.store_name = cs2.store_name AND
        |     cs1.store_zip = cs2.store_zip
        """.stripMargin.trim,
       """
        |SELECT cs1.product_name, cs1.store_name, cs1.store_zip, cs1.b_street_number,
        |       cs1.b_streen_name, cs1.b_city, cs1.b_zip, cs1.c_street_number, cs1.c_street_name,
        |       cs1.c_city, cs1.c_zip, cs1.syear, cs1.cnt, cs1.s1, cs1.s2, cs1.s3, cs2.s1,
        |       cs2.s2, cs2.s3
        |FROM cross_sales cs1,cross_sales cs2
        |WHERE cs1.item_sk=cs2.item_sk AND
        |     cs1.syear = 1999 AND
        |     cs2.syear = 1999 + 1 AND
        |     cs2.cnt <= cs1.cnt AND
        |     cs1.store_name = cs2.store_name AND
        |     cs1.store_zip = cs2.store_zip
        |ORDER BY cs1.product_name, cs1.store_name, cs2.cnt
        """.stripMargin.trim,
       """
        |SELECT
        |FROM
        |WHERE
        """.stripMargin.trim),
      // test case 3: test simple SELECT with GROUPBY (from q99)
      ("case_3",
       """
        |SELECT count(ss_sold_date_sk) as not_null_total,
        |       max(ss_sold_date_sk) as max_ss_sold_date_sk,
        |       max(ss_sold_time_sk) as max_ss_sold_time_sk,
        |       ss_item_sk,
        |       ss_store_sk
        |FROM store_sales
        |GROUP BY ss_item_sk, ss_store_sk
        """.stripMargin.trim,
       """
        |SELECT count(ss_sold_date_sk) as not_null_total,
        |       max(ss_sold_date_sk) as max_ss_sold_date_sk,
        |       ss_item_sk,
        |       ss_store_sk
        |FROM store_sales
        |GROUP BY ss_item_sk, ss_store_sk
        """.stripMargin.trim,
       """
        |SELECT gen_subsumer_0.`not_null_total`,
        |       gen_subsumer_0.`max_ss_sold_date_sk`,
        |       gen_subsumer_0.`ss_item_sk`,
        |       gen_subsumer_0.`ss_store_sk`
        |FROM
        |  (SELECT count(`ss_sold_date_sk`) AS `not_null_total`, max(`ss_sold_date_sk`) AS `max_ss_sold_date_sk`, max(`ss_sold_time_sk`) AS `max_ss_sold_time_sk`, `ss_item_sk`, `ss_store_sk` 
        |  FROM store_sales
        |  GROUP BY `ss_item_sk`, `ss_store_sk`) gen_subsumer_0
        """.stripMargin.trim),
      // test case 4 test SELECT containing join with GROUPBY (from q65)
      ("case_4",
       """
        |SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
        |FROM store_sales, date_dim
        |WHERE ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11
        |GROUP BY ss_store_sk, ss_item_sk
        """.stripMargin.trim,
       """
        |SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
        |FROM store_sales, date_dim
        |WHERE ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11
        |GROUP BY ss_store_sk, ss_item_sk
        """.stripMargin.trim,
       """
        |SELECT `ss_store_sk`, `ss_item_sk`, sum(`ss_sales_price`) AS `revenue` 
        |FROM
        |  store_sales
        |  INNER JOIN date_dim ON (`d_month_seq` >= 1176) AND (`d_month_seq` <= 1187) AND (`ss_sold_date_sk` = `d_date_sk`)
        |GROUP BY `ss_store_sk`, `ss_item_sk`
        """.stripMargin.trim),
      // the following 6 cases involve an MV of store_sales, item, date_dim
      // q3
      ("case_5",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       item.i_manufact_id, substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         item.i_manufact_id, substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
        """.stripMargin.trim,
       """
        | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
        | FROM  date_dim dt, store_sales, item
        | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |   AND store_sales.ss_item_sk = item.i_item_sk
        |   AND item.i_manufact_id = 128
        |   AND dt.d_moy=11
        | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        | ORDER BY dt.d_year, sum_agg desc, brand_id
        | LIMIT 100
        """.stripMargin.trim,
       """
        |SELECT gen_subsumer_0.`d_year`, gen_subsumer_0.`i_brand_id` AS `brand_id`, gen_subsumer_0.`i_brand` AS `brand`, sum(gen_subsumer_0.`sum_agg`) AS `sum_agg` 
        |FROM
        |  (SELECT `d_date`, `d_moy`, `d_year`, `i_brand`, `i_brand_id`, `i_item_id`, `i_item_desc`, `i_manufact_id`, substring(`i_item_desc`, 1, 30) AS `itemdesc`, `i_category`, `i_class`, `i_current_price`, `i_item_sk`, `ss_store_sk`, sum(`ss_ext_sales_price`) AS `sum_agg`, sum((CAST(CAST(`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(`ss_list_price` AS DECIMAL(12,2)))) AS `sales`, count(1) AS `number_sales` 
        |  FROM
        |    date_dim dt 
        |    INNER JOIN store_sales ON (`d_date_sk` = `ss_sold_date_sk`)
        |    INNER JOIN item   ON (`ss_item_sk` = `i_item_sk`)
        |  GROUP BY `d_date`, `d_moy`, `d_year`, `i_brand`, `i_brand_id`, `i_item_id`, `i_item_desc`, `i_manufact_id`, substring(`i_item_desc`, 1, 30), `i_category`, `i_category_id`, `i_class`, `i_class_id`, `i_current_price`, `i_manager_id`, `i_item_sk`, `ss_store_sk`) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`d_moy` = 11) AND (gen_subsumer_0.`i_manufact_id` = 128)
        |GROUP BY gen_subsumer_0.`d_year`, gen_subsumer_0.`i_brand`, gen_subsumer_0.`i_brand_id`
        |ORDER BY gen_subsumer_0.`d_year` ASC NULLS FIRST, `sum_agg` DESC NULLS LAST, `brand_id` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      // q23a
      ("case_6",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       item.i_manufact_id, substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         item.i_manufact_id, substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
        """.stripMargin.trim,
       """
        | with frequent_ss_items as
        | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
        |  from store_sales, date_dim, item
        |  where ss_sold_date_sk = d_date_sk
        |    and ss_item_sk = i_item_sk
        |    and d_year in (2000, 2000+1, 2000+2,2000+3)
        |  group by substr(i_item_desc,1,30),i_item_sk,d_date
        |  having count(*) >4),
        | max_store_sales as
        | (select max(csales) tpcds_cmax
        |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        |        from store_sales, customer, date_dim
        |        where ss_customer_sk = c_customer_sk
        |         and ss_sold_date_sk = d_date_sk
        |         and d_year in (2000, 2000+1, 2000+2,2000+3)
        |        group by c_customer_sk) x),
        | best_ss_customer as
        | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
        |  from store_sales, customer
        |  where ss_customer_sk = c_customer_sk
        |  group by c_customer_sk
        |  having sum(ss_quantity*ss_sales_price) > (50/100.0) *
        |    (select * from max_store_sales))
        | select sum(sales)
        | from ((select cs_quantity*cs_list_price sales
        |       from catalog_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and cs_sold_date_sk = d_date_sk
        |         and cs_item_sk in (select item_sk from frequent_ss_items)
        |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer))
        |      union all
        |      (select ws_quantity*ws_list_price sales
        |       from web_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and ws_sold_date_sk = d_date_sk
        |         and ws_item_sk in (select item_sk from frequent_ss_items)
        |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))) y
        | limit 100
        """.stripMargin.trim,
       """
        |SELECT sum(gen_subquery_4.`sales`) AS `sum(sales)` 
        |FROM
        |  (SELECT (CAST(CAST(catalog_sales.`cs_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(catalog_sales.`cs_list_price` AS DECIMAL(12,2))) AS `sales` 
        |  FROM
        |    catalog_sales
        |    LEFT SEMI JOIN (SELECT gen_subsumer_0.`i_item_sk` AS `item_sk`, sum(gen_subsumer_0.`number_sales`) AS `count(1)` 
        |    FROM
        |      (SELECT dt.`d_date`, dt.`d_moy`, dt.`d_year`, item.`i_brand`, item.`i_brand_id`, item.`i_item_id`, item.`i_item_desc`, item.`i_manufact_id`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, item.`i_category`, item.`i_class`, item.`i_current_price`, item.`i_item_sk`, store_sales.`ss_store_sk`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_list_price` AS DECIMAL(12,2)))) AS `sales`, count(1) AS `number_sales` 
        |      FROM
        |        date_dim dt 
        |        INNER JOIN store_sales ON (dt.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |        INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |      GROUP BY dt.`d_date`, dt.`d_moy`, dt.`d_year`, item.`i_brand`, item.`i_brand_id`, item.`i_item_id`, item.`i_item_desc`, item.`i_manufact_id`, substring(item.`i_item_desc`, 1, 30), item.`i_category`, item.`i_category_id`, item.`i_class`, item.`i_class_id`, item.`i_current_price`, item.`i_manager_id`, item.`i_item_sk`, store_sales.`ss_store_sk`) gen_subsumer_0 
        |    WHERE
        |      (gen_subsumer_0.`d_year` IN (2000, 2001, 2002, 2003))
        |    GROUP BY gen_subsumer_0.`itemdesc`, gen_subsumer_0.`i_item_sk`, gen_subsumer_0.`d_date`) gen_subquery_0  ON (gen_subquery_0.`count(1)` > 4L) AND (catalog_sales.`cs_item_sk` = gen_subquery_0.`item_sk`)
        |    LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#271 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#274 as decimal(12,2)))), DecimalType(18,2)))` 
        |    FROM
        |      store_sales
        |      INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |    GROUP BY customer.`c_customer_sk`) gen_subquery_1  ON (CAST(gen_subquery_1.`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#271 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#274 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_scalar_subquery_0_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`) gen_scalar_subquery_0_0 ) AS DECIMAL(32,6)))) AND (catalog_sales.`cs_bill_customer_sk` = gen_subquery_1.`c_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (catalog_sales.`cs_sold_date_sk` = date_dim.`d_date_sk`)
        |  UNION ALL
        |  SELECT (CAST(CAST(web_sales.`ws_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(web_sales.`ws_list_price` AS DECIMAL(12,2))) AS `sales` 
        |  FROM
        |    web_sales
        |    LEFT SEMI JOIN (SELECT gen_subsumer_1.`i_item_sk` AS `item_sk`, sum(gen_subsumer_1.`number_sales`) AS `count(1)` 
        |    FROM
        |      (SELECT dt.`d_date`, dt.`d_moy`, dt.`d_year`, item.`i_brand`, item.`i_brand_id`, item.`i_item_id`, item.`i_item_desc`, item.`i_manufact_id`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, item.`i_category`, item.`i_class`, item.`i_current_price`, item.`i_item_sk`, store_sales.`ss_store_sk`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_list_price` AS DECIMAL(12,2)))) AS `sales`, count(1) AS `number_sales` 
        |      FROM
        |        date_dim dt 
        |        INNER JOIN store_sales ON (dt.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |        INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |      GROUP BY dt.`d_date`, dt.`d_moy`, dt.`d_year`, item.`i_brand`, item.`i_brand_id`, item.`i_item_id`, item.`i_item_desc`, item.`i_manufact_id`, substring(item.`i_item_desc`, 1, 30), item.`i_category`, item.`i_category_id`, item.`i_class`, item.`i_class_id`, item.`i_current_price`, item.`i_manager_id`, item.`i_item_sk`, store_sales.`ss_store_sk`) gen_subsumer_1 
        |    WHERE
        |      (gen_subsumer_1.`d_year` IN (2000, 2001, 2002, 2003))
        |    GROUP BY gen_subsumer_1.`itemdesc`, gen_subsumer_1.`i_item_sk`, gen_subsumer_1.`d_date`) gen_subquery_2  ON (gen_subquery_2.`count(1)` > 4L) AND (web_sales.`ws_item_sk` = gen_subquery_2.`item_sk`)
        |    LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#271 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#274 as decimal(12,2)))), DecimalType(18,2)))` 
        |    FROM
        |      store_sales
        |      INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |    GROUP BY customer.`c_customer_sk`) gen_subquery_3  ON (CAST(gen_subquery_3.`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#271 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#274 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_scalar_subquery_1_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`) gen_scalar_subquery_1_0 ) AS DECIMAL(32,6)))) AND (web_sales.`ws_bill_customer_sk` = gen_subquery_3.`c_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)) gen_subquery_4 
        |LIMIT 100
        """.stripMargin.trim),
      // q14a
      ("case_7",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
        """.stripMargin.trim,
       """
        |with cross_items as
        | (select i_item_sk ss_item_sk
        | from item,
        |    (select iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
        |     from store_sales, item iss, date_dim d1
        |     where ss_item_sk = iss.i_item_sk
        |        and ss_sold_date_sk = d1.d_date_sk
        |       and d1.d_year between 1999 AND 1999 + 2
        |   intersect
        |     select ics.i_brand_id, ics.i_class_id, ics.i_category_id
        |     from catalog_sales, item ics, date_dim d2
        |     where cs_item_sk = ics.i_item_sk
        |       and cs_sold_date_sk = d2.d_date_sk
        |       and d2.d_year between 1999 AND 1999 + 2
        |   intersect
        |     select iws.i_brand_id, iws.i_class_id, iws.i_category_id
        |     from web_sales, item iws, date_dim d3
        |     where ws_item_sk = iws.i_item_sk
        |       and ws_sold_date_sk = d3.d_date_sk
        |       and d3.d_year between 1999 AND 1999 + 2) x
        | where i_brand_id = brand_id
        |   and i_class_id = class_id
        |   and i_category_id = category_id
        |),
        | avg_sales as
        | (select avg(quantity*list_price) average_sales
        |  from (
        |     select ss_quantity quantity, ss_list_price list_price
        |     from store_sales, date_dim
        |     where ss_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 2001
        |   union all
        |     select cs_quantity quantity, cs_list_price list_price
        |     from catalog_sales, date_dim
        |     where cs_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 1999 + 2
        |   union all
        |     select ws_quantity quantity, ws_list_price list_price
        |     from web_sales, date_dim
        |     where ws_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 1999 + 2) x)
        | select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
        | from(
        |     select 'store' channel, i_brand_id,i_class_id
        |             ,i_category_id,sum(ss_quantity*ss_list_price) sales
        |             , count(*) number_sales
        |     from store_sales, item, date_dim
        |     where ss_item_sk in (select ss_item_sk from cross_items)
        |       and ss_item_sk = i_item_sk
        |       and ss_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
        |   union all
        |     select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
        |     from catalog_sales, item, date_dim
        |     where cs_item_sk in (select ss_item_sk from cross_items)
        |       and cs_item_sk = i_item_sk
        |       and cs_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
        |   union all
        |     select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
        |     from web_sales, item, date_dim
        |     where ws_item_sk in (select ss_item_sk from cross_items)
        |       and ws_item_sk = i_item_sk
        |       and ws_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
        | ) y
        | group by rollup (channel, i_brand_id,i_class_id,i_category_id)
        | order by channel,i_brand_id,i_class_id,i_category_id
        | limit 100
        """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // q55
      ("case_8",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_manager_id, item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
       """.stripMargin.trim,
       """
        |select i_brand_id brand_id, i_brand brand,
        |   sum(ss_ext_sales_price) ext_price
        | from date_dim, store_sales, item
        | where d_date_sk = ss_sold_date_sk
        |   and ss_item_sk = i_item_sk
        |   and i_manager_id=28
        |   and d_moy=11
        |   and d_year=1999
        | group by i_brand, i_brand_id
        | order by ext_price desc, brand_id
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subsumer_0.`i_brand_id` AS `brand_id`, gen_subsumer_0.`i_brand` AS `brand`, sum(gen_subsumer_0.`sum_agg`) AS `ext_price` 
        |FROM
        |  (SELECT `d_date`, `d_moy`, `d_year`, `i_brand`, `i_brand_id`, `i_item_id`, `i_item_desc`, substring(`i_item_desc`, 1, 30) AS `itemdesc`, `i_category`, `i_class`, `i_manager_id`, `i_current_price`, `i_item_sk`, `ss_store_sk`, sum(`ss_ext_sales_price`) AS `sum_agg`, sum((CAST(CAST(`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(`ss_list_price` AS DECIMAL(12,2)))) AS `sales`, count(1) AS `number_sales` 
        |  FROM
        |    date_dim dt 
        |    INNER JOIN store_sales ON (`d_date_sk` = `ss_sold_date_sk`)
        |    INNER JOIN item   ON (`ss_item_sk` = `i_item_sk`)
        |  GROUP BY `d_date`, `d_moy`, `d_year`, `i_brand`, `i_brand_id`, `i_item_id`, `i_item_desc`, substring(`i_item_desc`, 1, 30), `i_category`, `i_category_id`, `i_class`, `i_class_id`, `i_current_price`, `i_manager_id`, `i_item_sk`, `ss_store_sk`) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`d_moy` = 11) AND (gen_subsumer_0.`d_year` = 1999) AND (gen_subsumer_0.`i_manager_id` = 28)
        |GROUP BY gen_subsumer_0.`i_brand`, gen_subsumer_0.`i_brand_id`
        |ORDER BY `ext_price` DESC NULLS LAST, `brand_id` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      // q98
      ("case_9",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_manager_id, item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
       """.stripMargin.trim,
       """
        |select i_item_desc, i_category, i_class, i_current_price
        |      ,sum(ss_ext_sales_price) as itemrevenue
        |      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
        |          (partition by i_class) as revenueratio
        |from
        |    store_sales, item, date_dim
        |where
        |   ss_item_sk = i_item_sk
        |   and i_category in ('Sports', 'Books', 'Home')
        |   and ss_sold_date_sk = d_date_sk
        |   and d_date between cast('1999-02-22' as date)
        |                           and (cast('1999-02-22' as date) + interval 30 days)
        |group by
        |   i_item_id, i_item_desc, i_category, i_class, i_current_price
        |order by
        |   i_category, i_class, i_item_id, i_item_desc, revenueratio
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // q76
      ("case_10",
       """
        |SELECT dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |       substr(item.i_item_desc, 1, 30) itemdesc, item.i_category, item.i_class,
        |       item.i_manager_id, item.i_current_price, item.i_item_sk, store_sales.ss_store_sk,
        |       SUM(store_sales.ss_ext_sales_price) sum_agg,
        |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
        |FROM date_dim dt, store_sales, item
        |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |      AND store_sales.ss_item_sk = item.i_item_sk
        |GROUP BY dt.d_date, dt.d_moy, dt.d_year, item.i_brand, item.i_brand_id, item.i_item_id, item.i_item_desc,
        |         substr(item.i_item_desc, 1, 30), item.i_category, item.i_category_id,
        |         item.i_class, item.i_class_id, item.i_current_price, item.i_manager_id,
        |         item.i_item_sk, store_sales.ss_store_sk
       """.stripMargin.trim,
       """
        | SELECT
        |    channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt,
        |    SUM(ext_sales_price) sales_amt
        | FROM(
        |    SELECT
        |        'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
        |        ss_ext_sales_price ext_sales_price
        |    FROM store_sales, item, date_dim
        |    WHERE ss_store_sk IS NULL
        |      AND ss_sold_date_sk=d_date_sk
        |      AND ss_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'web' as channel, ws_ship_customer_sk col_name, d_year, d_qoy, i_category,
        |        ws_ext_sales_price ext_sales_price
        |    FROM web_sales, item, date_dim
        |    WHERE ws_ship_customer_sk IS NULL
        |      AND ws_sold_date_sk=d_date_sk
        |      AND ws_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'catalog' as channel, cs_ship_addr_sk col_name, d_year, d_qoy, i_category,
        |        cs_ext_sales_price ext_sales_price
        |    FROM catalog_sales, item, date_dim
        |    WHERE cs_ship_addr_sk IS NULL
        |      AND cs_sold_date_sk=d_date_sk
        |      AND cs_item_sk=i_item_sk) foo
        | GROUP BY channel, col_name, d_year, d_qoy, i_category
        | ORDER BY channel, col_name, d_year, d_qoy, i_category
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // the following four cases involve a MV of catalog_sales, item, date_dim
      // q20
      ("case_11",
       """
        |SELECT cs_ship_addr_sk , d_date, d_year, d_qoy, d_moy, i_category, cs_ship_addr_sk,i_item_sk, i_item_id,
        |       i_item_desc, i_class, i_current_price, i_brand_id, i_class_id, i_category_id, i_manufact_id,
        |       SUM(cs_ext_sales_price) sales_amt, 
        |       SUM(cs_quantity*cs_list_price) sales,
        |       SUM(cs_ext_discount_amt) as `excess discount amount`,
        |       count(*) number_sales
        |FROM catalog_sales, item, date_dim
        |WHERE cs_item_sk = i_item_sk
        |  AND cs_sold_date_sk = d_date_sk      
        |GROUP BY i_brand_id, i_class_id, i_category_id, i_item_id, i_item_desc, i_category, i_class,
        |         i_current_price, i_manufact_id, d_date, d_moy, d_qoy, d_year, cs_ship_addr_sk, i_item_sk
       """.stripMargin.trim,
       """
        |select i_item_desc
        |       ,i_category
        |       ,i_class
        |       ,i_current_price
        |       ,sum(cs_ext_sales_price) as itemrevenue
        |       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
        |           (partition by i_class) as revenueratio
        | from catalog_sales, item, date_dim
        | where cs_item_sk = i_item_sk
        |   and i_category in ('Sports', 'Books', 'Home')
        |   and cs_sold_date_sk = d_date_sk
        | and d_date between cast('1999-02-22' as date)
        |                           and (cast('1999-02-22' as date) + interval 30 days)
        | group by i_item_id, i_item_desc, i_category, i_class, i_current_price
        | order by i_category, i_class, i_item_id, i_item_desc, revenueratio
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_1.`i_item_desc`, gen_subquery_1.`i_category`, gen_subquery_1.`i_class`, gen_subquery_1.`i_current_price`, gen_subquery_1.`itemrevenue`, ((gen_subquery_1.`_w0` * 100.00BD) / CAST(gen_subquery_1.`_we0` AS DECIMAL(28,2))) AS `revenueratio` 
        |FROM
        |  (SELECT gen_subquery_0.`i_item_desc`, gen_subquery_0.`i_category`, gen_subquery_0.`i_class`, gen_subquery_0.`i_current_price`, gen_subquery_0.`itemrevenue`, gen_subquery_0.`_w0`, gen_subquery_0.`_w1`, gen_subquery_0.`i_item_id`, sum(gen_subquery_0.`_w1`) OVER (PARTITION BY gen_subquery_0.`i_class` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `_we0` 
        |  FROM
        |    (SELECT gen_subsumer_0.`i_item_desc`, gen_subsumer_0.`i_category`, gen_subsumer_0.`i_class`, gen_subsumer_0.`i_current_price`, sum(gen_subsumer_0.`sales_amt`) AS `itemrevenue`, sum(gen_subsumer_0.`sales_amt`) AS `_w0`, sum(gen_subsumer_0.`sales_amt`) AS `_w1`, gen_subsumer_0.`i_item_id` 
        |    FROM
        |      (SELECT `cs_ship_addr_sk`, `d_date`, `d_year`, `d_qoy`, `d_moy`, `i_category`, `cs_ship_addr_sk`, `i_item_sk`, `i_item_id`, `i_item_desc`, `i_class`, `i_current_price`, `i_brand_id`, `i_class_id`, `i_category_id`, `i_manufact_id`, sum(`cs_ext_sales_price`) AS `sales_amt`, sum((CAST(CAST(`cs_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(`cs_list_price` AS DECIMAL(12,2)))) AS `sales`, sum(`cs_ext_discount_amt`) AS `excess discount amount`, count(1) AS `number_sales` 
        |      FROM
        |        catalog_sales
        |        INNER JOIN item   ON (`cs_item_sk` = `i_item_sk`)
        |        INNER JOIN date_dim ON (`cs_sold_date_sk` = `d_date_sk`)
        |      GROUP BY `i_brand_id`, `i_class_id`, `i_category_id`, `i_item_id`, `i_item_desc`, `i_category`, `i_class`, `i_current_price`, `i_manufact_id`, `d_date`, `d_moy`, `d_qoy`, `d_year`, `cs_ship_addr_sk`, `i_item_sk`) gen_subsumer_0 
        |    WHERE
        |      (gen_subsumer_0.`i_category` IN ('Sports', 'Books', 'Home')) AND (gen_subsumer_0.`d_date` >= DATE '1999-02-22') AND (gen_subsumer_0.`d_date` <= DATE '1999-03-24')
        |    GROUP BY gen_subsumer_0.`i_item_id`, gen_subsumer_0.`i_item_desc`, gen_subsumer_0.`i_category`, gen_subsumer_0.`i_class`, gen_subsumer_0.`i_current_price`) gen_subquery_0 ) gen_subquery_1 
        |ORDER BY gen_subquery_1.`i_category` ASC NULLS FIRST, gen_subquery_1.`i_class` ASC NULLS FIRST, gen_subquery_1.`i_item_id` ASC NULLS FIRST, gen_subquery_1.`i_item_desc` ASC NULLS FIRST, `revenueratio` ASC NULLS FIRST
        |LIMIT 100
       """.stripMargin.trim),
      // q32
      ("case_12",
       """
        |SELECT cs_ship_addr_sk , d_date, d_year, d_qoy, d_moy, i_category, cs_ship_addr_sk,i_item_sk, i_item_id,
        |       i_item_desc, i_class, i_current_price, i_brand_id, i_class_id, i_category_id, i_manufact_id,
        |       SUM(cs_ext_sales_price) sales_amt, 
        |       SUM(cs_quantity*cs_list_price) sales,
        |       SUM(cs_ext_discount_amt) as `excess discount amount`,
        |       count(*) number_sales
        |FROM catalog_sales, item, date_dim
        |WHERE cs_item_sk = i_item_sk
        |  AND cs_sold_date_sk = d_date_sk      
        |GROUP BY i_brand_id, i_class_id, i_category_id, i_item_id, i_item_desc, i_category, i_class,
        |         i_current_price, i_manufact_id, d_date, d_moy, d_qoy, d_year, cs_ship_addr_sk, i_item_sk
       """.stripMargin.trim,
       """
        | select sum(cs_ext_discount_amt) as `excess discount amount`
        | from
        |    catalog_sales, item, date_dim
        | where
        |   i_manufact_id = 977
        |   and i_item_sk = cs_item_sk
        |   and d_date between '2000-01-27' and (cast('2000-01-27' as date) + interval 90 days)
        |   and d_date_sk = cs_sold_date_sk
        |   and cs_ext_discount_amt > (
        |          select 1.3 * avg(cs_ext_discount_amt)
        |          from catalog_sales, date_dim
        |          where cs_item_sk = i_item_sk
        |           and d_date between '2000-01-27]' and (cast('2000-01-27' as date) + interval 90 days)
        |           and d_date_sk = cs_sold_date_sk)
        |limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // q58 debug
      ("case_13",
       """
        |SELECT cs_ship_addr_sk , d_date, d_year, d_qoy, d_moy, i_category, cs_ext_sales_price, cs_ship_addr_sk, i_item_sk, i_item_id,
        |       i_item_desc, i_class, i_current_price, i_brand_id, i_class_id, i_category_id, i_manufact_id,
        |       SUM(cs_ext_sales_price) sales_amt, 
        |       SUM(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
        |          (partition by i_class) as revenueratio
        |       SUM(cs_quantity*cs_list_price) sales,
        |       SUM(cs_ext_discount_amt) as `excess discount amount`,
        |       count(*) number_sales
        |FROM catalog_sales, item, date_dim
        |WHERE cs_item_sk = i_item_sk
        |  AND cs_sold_date_sk = d_date_sk      
        |GROUP BY i_brand_id, i_class_id, i_category_id, i_item_id, i_item_desc, i_category, i_class,
        |         i_current_price, i_manufact_id, d_date, d_moy, d_qoy, d_year, cs_ship_addr_sk, i_item_sk
       """.stripMargin.trim,
       """
        | with ss_items as
        | (select i_item_id item_id, sum(ss_ext_sales_price) ss_item_rev
        | from store_sales, item, date_dim
        | where ss_item_sk = i_item_sk
        |   and d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq = (select d_week_seq
        |                                      from date_dim
        |                                      where d_date = '2000-01-03'))
        |   and ss_sold_date_sk   = d_date_sk
        | group by i_item_id),
        | cs_items as
        | (select i_item_id item_id
        |        ,sum(cs_ext_sales_price) cs_item_rev
        |  from catalog_sales, item, date_dim
        | where cs_item_sk = i_item_sk
        |  and  d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq = (select d_week_seq
        |                                      from date_dim
        |                                      where d_date = '2000-01-03'))
        |  and  cs_sold_date_sk = d_date_sk
        | group by i_item_id),
        | ws_items as
        | (select i_item_id item_id, sum(ws_ext_sales_price) ws_item_rev
        |  from web_sales, item, date_dim
        | where ws_item_sk = i_item_sk
        |  and  d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq =(select d_week_seq
        |                                     from date_dim
        |                                     where d_date = '2000-01-03'))
        |  and ws_sold_date_sk   = d_date_sk
        | group by i_item_id)
        | select ss_items.item_id
        |       ,ss_item_rev
        |       ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
        |       ,cs_item_rev
        |       ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
        |       ,ws_item_rev
        |       ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
        |       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
        | from ss_items,cs_items,ws_items
        | where ss_items.item_id=cs_items.item_id
        |   and ss_items.item_id=ws_items.item_id
        |   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
        |   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
        |   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
        |   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
        |   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
        |   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
        | order by item_id, ss_item_rev
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // q76
      ("case_14",
       """
        |SELECT cs_ship_addr_sk , d_date, d_year, d_qoy, d_moy, i_category, cs_ext_sales_price, cs_ship_addr_sk, i_item_sk, i_item_id,
        |       i_item_desc, i_class, i_current_price, i_brand_id, i_class_id, i_category_id, i_manufact_id,
        |       SUM(cs_ext_sales_price) sales_amt, 
        |       SUM(cs_quantity*cs_list_price) sales,
        |       SUM(cs_ext_discount_amt) as `excess discount amount`,
        |       count(*) number_sales
        |FROM catalog_sales, item, date_dim
        |WHERE cs_item_sk = i_item_sk
        |  AND cs_sold_date_sk = d_date_sk      
        |GROUP BY i_brand_id, i_class_id, i_category_id, i_item_id, i_item_desc, i_category, i_class,
        |         i_current_price, i_manufact_id, d_date, d_moy, d_qoy, d_year, cs_ship_addr_sk, i_item_sk
       """.stripMargin.trim,
       """
        | SELECT
        |    channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt,
        |    SUM(ext_sales_price) sales_amt
        | FROM(
        |    SELECT
        |        'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
        |        ss_ext_sales_price ext_sales_price
        |    FROM store_sales, item, date_dim
        |    WHERE ss_store_sk IS NULL
        |      AND ss_sold_date_sk=d_date_sk
        |      AND ss_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'web' as channel, ws_ship_customer_sk col_name, d_year, d_qoy, i_category,
        |        ws_ext_sales_price ext_sales_price
        |    FROM web_sales, item, date_dim
        |    WHERE ws_ship_customer_sk IS NULL
        |      AND ws_sold_date_sk=d_date_sk
        |      AND ws_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'catalog' as channel, cs_ship_addr_sk col_name, d_year, d_qoy, i_category,
        |        cs_ext_sales_price ext_sales_price
        |    FROM catalog_sales, item, date_dim
        |    WHERE cs_ship_addr_sk IS NULL
        |      AND cs_sold_date_sk=d_date_sk
        |      AND cs_item_sk=i_item_sk) foo
        | GROUP BY channel, col_name, d_year, d_qoy, i_category
        | ORDER BY channel, col_name, d_year, d_qoy, i_category
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // the following two cases involve a MV of store_sales and customer
      // q23a
      ("case_15",
       """
        | SELECT c_customer_sk,
        |        sum(ss_quantity*ss_sales_price) csales
        | FROM customer, store_sales
        | WHERE c_customer_sk = ss_customer_sk
        | GROUP BY c_customer_sk
       """.stripMargin.trim,
       """
        | with frequent_ss_items as
        | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
        |  from store_sales, date_dim, item
        |  where ss_sold_date_sk = d_date_sk
        |    and ss_item_sk = i_item_sk
        |    and d_year in (2000, 2000+1, 2000+2,2000+3)
        |  group by substr(i_item_desc,1,30),i_item_sk,d_date
        |  having count(*) >4),
        | max_store_sales as
        | (select max(csales) tpcds_cmax
        |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        |        from store_sales, customer, date_dim
        |        where ss_customer_sk = c_customer_sk
        |         and ss_sold_date_sk = d_date_sk
        |         and d_year in (2000, 2000+1, 2000+2,2000+3)
        |        group by c_customer_sk) x),
        | best_ss_customer as
        | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
        |  from store_sales, customer
        |  where ss_customer_sk = c_customer_sk
        |  group by c_customer_sk
        |  having sum(ss_quantity*ss_sales_price) > (50/100.0) *
        |    (select * from max_store_sales))
        | select sum(sales)
        | from ((select cs_quantity*cs_list_price sales
        |       from catalog_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and cs_sold_date_sk = d_date_sk
        |         and cs_item_sk in (select item_sk from frequent_ss_items)
        |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer))
        |      union all
        |      (select ws_quantity*ws_list_price sales
        |       from web_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and ws_sold_date_sk = d_date_sk
        |         and ws_item_sk in (select item_sk from frequent_ss_items)
        |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))) y
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // q23b
      ("case_16",
       """
        | SELECT c_customer_sk,
        |        sum(ss_quantity*ss_sales_price) csales
        | FROM customer, store_sales
        | WHERE c_customer_sk = ss_customer_sk
        | GROUP BY c_customer_sk
       """.stripMargin.trim,
       """
        |
        | with frequent_ss_items as
        | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
        |  from store_sales, date_dim, item
        |  where ss_sold_date_sk = d_date_sk
        |    and ss_item_sk = i_item_sk
        |    and d_year in (2000, 2000+1, 2000+2,2000+3)
        |  group by substr(i_item_desc,1,30),i_item_sk,d_date
        |  having count(*) > 4),
        | max_store_sales as
        | (select max(csales) tpcds_cmax
        |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        |        from store_sales, customer, date_dim
        |        where ss_customer_sk = c_customer_sk
        |         and ss_sold_date_sk = d_date_sk
        |         and d_year in (2000, 2000+1, 2000+2,2000+3)
        |        group by c_customer_sk) x),
        | best_ss_customer as
        | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
        |  from store_sales
        |      ,customer
        |  where ss_customer_sk = c_customer_sk
        |  group by c_customer_sk
        |  having sum(ss_quantity*ss_sales_price) > (50/100.0) *
        |    (select * from max_store_sales))
        | select c_last_name,c_first_name,sales
        | from ((select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        |        from catalog_sales, customer, date_dim
        |        where d_year = 2000
        |         and d_moy = 2
        |         and cs_sold_date_sk = d_date_sk
        |         and cs_item_sk in (select item_sk from frequent_ss_items)
        |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
        |         and cs_bill_customer_sk = c_customer_sk
        |       group by c_last_name,c_first_name)
        |      union all
        |      (select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
        |       from web_sales, customer, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and ws_sold_date_sk = d_date_sk
        |         and ws_item_sk in (select item_sk from frequent_ss_items)
        |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
        |         and ws_bill_customer_sk = c_customer_sk
        |       group by c_last_name,c_first_name)) y
        |     order by c_last_name,c_first_name,sales
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_6.`c_last_name`, gen_subquery_6.`c_first_name`, gen_subquery_6.`sales` 
        |FROM
        |  (SELECT gen_subquery_2.`c_last_name`, gen_subquery_2.`c_first_name`, sum((CAST(CAST(gen_subquery_2.`cs_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(gen_subquery_2.`cs_list_price` AS DECIMAL(12,2)))) AS `sales` 
        |  FROM
        |    (SELECT `cs_quantity`, `cs_list_price`, `c_first_name`, `c_last_name` 
        |    FROM
        |      catalog_sales
        |      LEFT SEMI JOIN (SELECT item.`i_item_sk` AS `item_sk`, count(1) AS `count(1)` 
        |      FROM
        |        store_sales
        |        INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |        INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |      GROUP BY substring(item.`i_item_desc`, 1, 30), item.`i_item_sk`, date_dim.`d_date`) gen_subquery_0  ON (gen_subquery_0.`count(1)` > 4L) AND (catalog_sales.`cs_item_sk` = gen_subquery_0.`item_sk`)
        |      LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales` 
        |      FROM
        |        customer  
        |        INNER JOIN store_sales ON (customer.`c_customer_sk` = store_sales.`ss_customer_sk`)
        |      GROUP BY customer.`c_customer_sk`) gen_subquery_1  ON (catalog_sales.`cs_bill_customer_sk` = gen_subquery_1.`c_customer_sk`)
        |      INNER JOIN customer ON (catalog_sales.`cs_bill_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (catalog_sales.`cs_sold_date_sk` = date_dim.`d_date_sk`)
        |    WHERE
        |      (CAST(`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#219 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#222 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_expression_0_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`) gen_expression_0_0 ) AS DECIMAL(32,6))))) gen_subquery_2 
        |  GROUP BY gen_subquery_2.`c_last_name`, gen_subquery_2.`c_first_name`
        |  UNION ALL
        |  SELECT gen_subquery_5.`c_last_name`, gen_subquery_5.`c_first_name`, sum((CAST(CAST(gen_subquery_5.`ws_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(gen_subquery_5.`ws_list_price` AS DECIMAL(12,2)))) AS `sales` 
        |  FROM
        |    (SELECT `ws_quantity`, `ws_list_price`, `c_first_name`, `c_last_name` 
        |    FROM
        |      web_sales
        |      LEFT SEMI JOIN (SELECT item.`i_item_sk` AS `item_sk`, count(1) AS `count(1)` 
        |      FROM
        |        store_sales
        |        INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |        INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |      GROUP BY substring(item.`i_item_desc`, 1, 30), item.`i_item_sk`, date_dim.`d_date`) gen_subquery_3  ON (gen_subquery_3.`count(1)` > 4L) AND (web_sales.`ws_item_sk` = gen_subquery_3.`item_sk`)
        |      LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales` 
        |      FROM
        |        customer  
        |        INNER JOIN store_sales ON (customer.`c_customer_sk` = store_sales.`ss_customer_sk`)
        |      GROUP BY customer.`c_customer_sk`) gen_subquery_4  ON (web_sales.`ws_bill_customer_sk` = gen_subquery_4.`c_customer_sk`)
        |      INNER JOIN customer ON (web_sales.`ws_bill_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |    WHERE
        |      (CAST(`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#219 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#222 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_expression_1_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`) gen_expression_1_0 ) AS DECIMAL(32,6))))) gen_subquery_5 
        |  GROUP BY gen_subquery_5.`c_last_name`, gen_subquery_5.`c_first_name`) gen_subquery_6 
        |ORDER BY gen_subquery_6.`c_last_name` ASC NULLS FIRST, gen_subquery_6.`c_first_name` ASC NULLS FIRST, gen_subquery_6.`sales` ASC NULLS FIRST
        |LIMIT 100
       """.stripMargin.trim),
      // the following cases involve a MV of store_sales, customer and date
      // q4
      ("case_17",
       """
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        d_date ddate,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        sum(ss_ext_list_price-ss_ext_discount_amt) year_total1,
        |        sum(ss_net_paid) year_total_74,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year,
        |          d_date
       """.stripMargin.trim,
       """
        |WITH year_total AS (
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year
        | UNION ALL
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total,
        |        'c' sale_type
        | FROM customer, catalog_sales, date_dim
        | WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year
        | UNION ALL
        | SELECT c_customer_id customer_id
        |       ,c_first_name customer_first_name
        |       ,c_last_name customer_last_name
        |       ,c_preferred_cust_flag customer_preferred_cust_flag
        |       ,c_birth_country customer_birth_country
        |       ,c_login customer_login
        |       ,c_email_address customer_email_address
        |       ,d_year dyear
        |       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
        |       ,'w' sale_type
        | FROM customer, web_sales, date_dim
        | WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year)
        | SELECT
        |   t_s_secyear.customer_id,
        |   t_s_secyear.customer_first_name,
        |   t_s_secyear.customer_last_name,
        |   t_s_secyear.customer_preferred_cust_flag,
        |   t_s_secyear.customer_birth_country,
        |   t_s_secyear.customer_login,
        |   t_s_secyear.customer_email_address
        | FROM year_total t_s_firstyear, year_total t_s_secyear, year_total t_c_firstyear,
        |      year_total t_c_secyear, year_total t_w_firstyear, year_total t_w_secyear
        | WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
        |   and t_s_firstyear.customer_id = t_c_secyear.customer_id
        |   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
        |   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
        |   and t_s_firstyear.customer_id = t_w_secyear.customer_id
        |   and t_s_firstyear.sale_type = 's'
        |   and t_c_firstyear.sale_type = 'c'
        |   and t_w_firstyear.sale_type = 'w'
        |   and t_s_secyear.sale_type = 's'
        |   and t_c_secyear.sale_type = 'c'
        |   and t_w_secyear.sale_type = 'w'
        |   and t_s_firstyear.dyear = 2001
        |   and t_s_secyear.dyear = 2001+1
        |   and t_c_firstyear.dyear = 2001
        |   and t_c_secyear.dyear = 2001+1
        |   and t_w_firstyear.dyear = 2001
        |   and t_w_secyear.dyear = 2001+1
        |   and t_s_firstyear.year_total > 0
        |   and t_c_firstyear.year_total > 0
        |   and t_w_firstyear.year_total > 0
        |   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
        |           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
        |   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
        |           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
        | ORDER BY
        |   t_s_secyear.customer_id,
        |   t_s_secyear.customer_first_name,
        |   t_s_secyear.customer_last_name,
        |   t_s_secyear.customer_preferred_cust_flag,
        |   t_s_secyear.customer_birth_country,
        |   t_s_secyear.customer_login,
        |   t_s_secyear.customer_email_address
        | LIMIT 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      //q11
      ("case_18",
       """
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        d_date ddate,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        sum(ss_ext_list_price-ss_ext_discount_amt) year_total1,
        |        sum(ss_net_paid) year_total_74,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year,
        |          d_date
       """.stripMargin.trim,
       """
        | with year_total as (
        | select c_customer_id customer_id
        |       ,c_first_name customer_first_name
        |       ,c_last_name customer_last_name
        |       ,c_preferred_cust_flag customer_preferred_cust_flag
        |       ,c_birth_country customer_birth_country
        |       ,c_login customer_login
        |       ,c_email_address customer_email_address
        |       ,d_year dyear
        |       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
        |       ,'s' sale_type
        | from customer, store_sales, date_dim
        | where c_customer_sk = ss_customer_sk
        |   and ss_sold_date_sk = d_date_sk
        | group by c_customer_id
        |         ,c_first_name
        |         ,c_last_name
        |         ,d_year
        |         ,c_preferred_cust_flag
        |         ,c_birth_country
        |         ,c_login
        |         ,c_email_address
        |         ,d_year
        | union all
        | select c_customer_id customer_id
        |       ,c_first_name customer_first_name
        |       ,c_last_name customer_last_name
        |       ,c_preferred_cust_flag customer_preferred_cust_flag
        |       ,c_birth_country customer_birth_country
        |       ,c_login customer_login
        |       ,c_email_address customer_email_address
        |       ,d_year dyear
        |       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
        |       ,'w' sale_type
        | from customer, web_sales, date_dim
        | where c_customer_sk = ws_bill_customer_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |    c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country,
        |    c_login, c_email_address, d_year)
        | select
        |    t_s_secyear.customer_preferred_cust_flag
        | from year_total t_s_firstyear
        |     ,year_total t_s_secyear
        |     ,year_total t_w_firstyear
        |     ,year_total t_w_secyear
        | where t_s_secyear.customer_id = t_s_firstyear.customer_id
        |         and t_s_firstyear.customer_id = t_w_secyear.customer_id
        |         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
        |         and t_s_firstyear.sale_type = 's'
        |         and t_w_firstyear.sale_type = 'w'
        |         and t_s_secyear.sale_type = 's'
        |         and t_w_secyear.sale_type = 'w'
        |         and t_s_firstyear.dyear = 2001
        |         and t_s_secyear.dyear = 2001+1
        |         and t_w_firstyear.dyear = 2001
        |         and t_w_secyear.dyear = 2001+1
        |         and t_s_firstyear.year_total > 0
        |         and t_w_firstyear.year_total > 0
        |         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
        |             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
        | order by t_s_secyear.customer_preferred_cust_flag
        | LIMIT 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_1.`customer_preferred_cust_flag` 
        |FROM
        |  (SELECT gen_subsumer_0.`customer_id` AS `customer_id`, sum(gen_subsumer_0.`year_total1`) AS `year_total` 
        |  FROM
        |    (SELECT `c_customer_id` AS `customer_id`, `c_first_name` AS `customer_first_name`, `c_last_name` AS `customer_last_name`, `c_preferred_cust_flag` AS `customer_preferred_cust_flag`, `c_birth_country` AS `customer_birth_country`, `c_login` AS `customer_login`, `c_email_address` AS `customer_email_address`, `d_year` AS `dyear`, `d_date` AS `ddate`, sum((CAST((((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, sum((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total1`, sum(`ss_net_paid`) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (`c_customer_sk` = `ss_customer_sk`)
        |      INNER JOIN date_dim ON (`ss_sold_date_sk` = `d_date_sk`)
        |    GROUP BY `c_customer_id`, `c_first_name`, `c_last_name`, `c_preferred_cust_flag`, `c_birth_country`, `c_login`, `c_email_address`, `d_year`, `d_date`) gen_subsumer_0 
        |  WHERE
        |    (gen_subsumer_0.`dyear` = 2001)
        |  GROUP BY gen_subsumer_0.`customer_id`, gen_subsumer_0.`customer_first_name`, gen_subsumer_0.`customer_last_name`, gen_subsumer_0.`dyear`, gen_subsumer_0.`customer_preferred_cust_flag`, gen_subsumer_0.`customer_birth_country`, gen_subsumer_0.`customer_login`, gen_subsumer_0.`customer_email_address`
        |  HAVING (sum(gen_subsumer_0.`year_total1`) > 0.00BD)
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, sum((CAST(web_sales.`ws_ext_list_price` AS DECIMAL(8,2)) - CAST(web_sales.`ws_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  WHERE
        |    false
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`
        |  HAVING (`year_total` > 0.00BD)) gen_subquery_0 
        |  INNER JOIN (SELECT gen_subsumer_1.`customer_id` AS `customer_id`, gen_subsumer_1.`customer_preferred_cust_flag` AS `customer_preferred_cust_flag`, sum(gen_subsumer_1.`year_total1`) AS `year_total` 
        |  FROM
        |    (SELECT `c_customer_id` AS `customer_id`, `c_first_name` AS `customer_first_name`, `c_last_name` AS `customer_last_name`, `c_preferred_cust_flag` AS `customer_preferred_cust_flag`, `c_birth_country` AS `customer_birth_country`, `c_login` AS `customer_login`, `c_email_address` AS `customer_email_address`, `d_year` AS `dyear`, `d_date` AS `ddate`, sum((CAST((((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, sum((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total1`, sum(`ss_net_paid`) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (`c_customer_sk` = `ss_customer_sk`)
        |      INNER JOIN date_dim ON (`ss_sold_date_sk` = `d_date_sk`)
        |    GROUP BY `c_customer_id`, `c_first_name`, `c_last_name`, `c_preferred_cust_flag`, `c_birth_country`, `c_login`, `c_email_address`, `d_year`, `d_date`) gen_subsumer_1 
        |  WHERE
        |    (gen_subsumer_1.`dyear` = 2002)
        |  GROUP BY gen_subsumer_1.`customer_id`, gen_subsumer_1.`customer_first_name`, gen_subsumer_1.`customer_last_name`, gen_subsumer_1.`dyear`, gen_subsumer_1.`customer_preferred_cust_flag`, gen_subsumer_1.`customer_birth_country`, gen_subsumer_1.`customer_login`, gen_subsumer_1.`customer_email_address`
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, customer.`c_preferred_cust_flag` AS `customer_preferred_cust_flag`, sum((CAST(web_sales.`ws_ext_list_price` AS DECIMAL(8,2)) - CAST(web_sales.`ws_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  WHERE
        |    false
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`) gen_subquery_1  ON (gen_subquery_1.`customer_id` = gen_subquery_0.`customer_id`)
        |  INNER JOIN (SELECT gen_subsumer_2.`customer_id` AS `customer_id`, sum(gen_subsumer_2.`year_total1`) AS `year_total` 
        |  FROM
        |    (SELECT `c_customer_id` AS `customer_id`, `c_first_name` AS `customer_first_name`, `c_last_name` AS `customer_last_name`, `c_preferred_cust_flag` AS `customer_preferred_cust_flag`, `c_birth_country` AS `customer_birth_country`, `c_login` AS `customer_login`, `c_email_address` AS `customer_email_address`, `d_year` AS `dyear`, `d_date` AS `ddate`, sum((CAST((((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, sum((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total1`, sum(`ss_net_paid`) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (`c_customer_sk` = `ss_customer_sk`)
        |      INNER JOIN date_dim ON (`ss_sold_date_sk` = `d_date_sk`)
        |    GROUP BY `c_customer_id`, `c_first_name`, `c_last_name`, `c_preferred_cust_flag`, `c_birth_country`, `c_login`, `c_email_address`, `d_year`, `d_date`) gen_subsumer_2 
        |  WHERE
        |    false
        |  GROUP BY gen_subsumer_2.`customer_id`, gen_subsumer_2.`customer_first_name`, gen_subsumer_2.`customer_last_name`, gen_subsumer_2.`dyear`, gen_subsumer_2.`customer_preferred_cust_flag`, gen_subsumer_2.`customer_birth_country`, gen_subsumer_2.`customer_login`, gen_subsumer_2.`customer_email_address`
        |  HAVING (sum(gen_subsumer_2.`year_total1`) > 0.00BD)
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, sum((CAST(web_sales.`ws_ext_list_price` AS DECIMAL(8,2)) - CAST(web_sales.`ws_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2001) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`
        |  HAVING (`year_total` > 0.00BD)) gen_subquery_2  ON (gen_subquery_0.`customer_id` = gen_subquery_2.`customer_id`)
        |  INNER JOIN (SELECT gen_subsumer_3.`customer_id` AS `customer_id`, sum(gen_subsumer_3.`year_total1`) AS `year_total` 
        |  FROM
        |    (SELECT `c_customer_id` AS `customer_id`, `c_first_name` AS `customer_first_name`, `c_last_name` AS `customer_last_name`, `c_preferred_cust_flag` AS `customer_preferred_cust_flag`, `c_birth_country` AS `customer_birth_country`, `c_login` AS `customer_login`, `c_email_address` AS `customer_email_address`, `d_year` AS `dyear`, `d_date` AS `ddate`, sum((CAST((((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, sum((CAST(`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(`ss_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total1`, sum(`ss_net_paid`) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (`c_customer_sk` = `ss_customer_sk`)
        |      INNER JOIN date_dim ON (`ss_sold_date_sk` = `d_date_sk`)
        |    GROUP BY `c_customer_id`, `c_first_name`, `c_last_name`, `c_preferred_cust_flag`, `c_birth_country`, `c_login`, `c_email_address`, `d_year`, `d_date`) gen_subsumer_3 
        |  WHERE
        |    false
        |  GROUP BY gen_subsumer_3.`customer_id`, gen_subsumer_3.`customer_first_name`, gen_subsumer_3.`customer_last_name`, gen_subsumer_3.`dyear`, gen_subsumer_3.`customer_preferred_cust_flag`, gen_subsumer_3.`customer_birth_country`, gen_subsumer_3.`customer_login`, gen_subsumer_3.`customer_email_address`
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, sum((CAST(web_sales.`ws_ext_list_price` AS DECIMAL(8,2)) - CAST(web_sales.`ws_ext_discount_amt` AS DECIMAL(8,2)))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2002) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`) gen_subquery_3 
        |WHERE
        |  (gen_subquery_0.`customer_id` = gen_subquery_3.`customer_id`) AND (CASE WHEN (gen_subquery_2.`year_total` > 0.00BD) THEN (gen_subquery_3.`year_total` / gen_subquery_2.`year_total`) ELSE CAST(NULL AS DECIMAL(38,20)) END > CASE WHEN (gen_subquery_0.`year_total` > 0.00BD) THEN (gen_subquery_1.`year_total` / gen_subquery_0.`year_total`) ELSE CAST(NULL AS DECIMAL(38,20)) END)
        |ORDER BY gen_subquery_1.`customer_preferred_cust_flag` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      //q38
      ("case_19",
       """
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        d_date ddate,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        sum(ss_ext_list_price-ss_ext_discount_amt) year_total1,
        |        sum(ss_net_paid) year_total_74,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year,
        |          d_date
       """.stripMargin.trim,
       """
        | select count(*) from (
        |    select distinct c_last_name, c_first_name, d_date
        |    from store_sales, date_dim, customer
        |          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |      and store_sales.ss_customer_sk = customer.c_customer_sk
        |      and d_month_seq between 1200 and  1200 + 11
        |  intersect
        |    select distinct c_last_name, c_first_name, d_date
        |    from catalog_sales, date_dim, customer
        |          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        |      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
        |      and d_month_seq between  1200 and  1200 + 11
        |  intersect
        |    select distinct c_last_name, c_first_name, d_date
        |    from web_sales, date_dim, customer
        |          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
        |      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
        |      and d_month_seq between  1200 and  1200 + 11
        | ) hot_cust
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      //q74
      ("case_20",
       """
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        d_date ddate,
        |        d_month_seq,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        sum(ss_net_paid) year_total_74,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE ss_customer_sk = c_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year,
        |          d_date,
        |          d_month_seq
       """.stripMargin.trim,
       """
        | with year_total as (
        | select
        |    c_customer_id customer_id, c_first_name customer_first_name,
        |    c_last_name customer_last_name, d_year as year,
        |    sum(ss_net_paid) year_total, 's' sale_type
        | from
        |    customer, store_sales, date_dim
        | where c_customer_sk = ss_customer_sk
        |    and ss_sold_date_sk = d_date_sk
        |    and d_year in (2001,2001+1)
        | group by
        |    c_customer_id, c_first_name, c_last_name, d_year
        | union all
        | select
        |    c_customer_id customer_id, c_first_name customer_first_name,
        |    c_last_name customer_last_name, d_year as year,
        |    sum(ws_net_paid) year_total, 'w' sale_type
        | from
        |    customer, web_sales, date_dim
        | where c_customer_sk = ws_bill_customer_sk
        |    and ws_sold_date_sk = d_date_sk
        |    and d_year in (2001,2001+1)
        | group by
        |    c_customer_id, c_first_name, c_last_name, d_year)
        | select
        |    t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
        | from
        |    year_total t_s_firstyear, year_total t_s_secyear,
        |    year_total t_w_firstyear, year_total t_w_secyear
        | where t_s_secyear.customer_id = t_s_firstyear.customer_id
        |    and t_s_firstyear.customer_id = t_w_secyear.customer_id
        |    and t_s_firstyear.customer_id = t_w_firstyear.customer_id
        |    and t_s_firstyear.sale_type = 's'
        |    and t_w_firstyear.sale_type = 'w'
        |    and t_s_secyear.sale_type = 's'
        |    and t_w_secyear.sale_type = 'w'
        |    and t_s_firstyear.year = 2001
        |    and t_s_secyear.year = 2001+1
        |    and t_w_firstyear.year = 2001
        |    and t_w_secyear.year = 2001+1
        |    and t_s_firstyear.year_total > 0
        |    and t_w_firstyear.year_total > 0
        |    and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
        |      > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
        | order by 1, 1, 1
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_1.`customer_id`, gen_subquery_1.`customer_first_name`, gen_subquery_1.`customer_last_name` 
        |FROM
        |  (SELECT gen_subsumer_0.`customer_id` AS `customer_id`, gen_subsumer_0.`year_total_74` AS `year_total` 
        |  FROM
        |    (SELECT customer.`c_customer_id` AS `customer_id`, customer.`c_first_name` AS `customer_first_name`, customer.`c_last_name` AS `customer_last_name`, customer.`c_preferred_cust_flag` AS `customer_preferred_cust_flag`, customer.`c_birth_country` AS `customer_birth_country`, customer.`c_login` AS `customer_login`, customer.`c_email_address` AS `customer_email_address`, date_dim.`d_year` AS `dyear`, date_dim.`d_date` AS `ddate`, date_dim.`d_month_seq`, sum((CAST((((CAST(store_sales.`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(store_sales.`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(store_sales.`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(store_sales.`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_paid`))) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |    GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`, date_dim.`d_date`, date_dim.`d_month_seq`) gen_subsumer_0 
        |  WHERE
        |    (gen_subsumer_0.`dyear` IN (2001, 2002)) AND (gen_subsumer_0.`dyear` = 2001)
        |  GROUP BY gen_subsumer_0.`customer_id`, gen_subsumer_0.`customer_first_name`, gen_subsumer_0.`customer_last_name`, gen_subsumer_0.`dyear`
        |  HAVING (gen_subsumer_0.`year_total_74` > 0.00BD)
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, makedecimal(sum(unscaledvalue(web_sales.`ws_net_paid`))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` IN (2001, 2002)) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  WHERE
        |    false
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`
        |  HAVING (`year_total` > 0.00BD)) gen_subquery_0 
        |  INNER JOIN (SELECT gen_subsumer_1.`customer_id` AS `customer_id`, gen_subsumer_1.`customer_first_name` AS `customer_first_name`, gen_subsumer_1.`customer_last_name` AS `customer_last_name`, gen_subsumer_1.`year_total_74` AS `year_total` 
        |  FROM
        |    (SELECT customer.`customer_id` AS `customer_id`, customer.`customer_first_name` AS `customer_first_name`, customer.`customer_last_name` AS `customer_last_name`, customer.`customer_preferred_cust_flag` AS `customer_preferred_cust_flag`, customer.`customer_birth_country` AS `customer_birth_country`, customer.`customer_login` AS `customer_login`, customer.`customer_email_address` AS `customer_email_address`, date_dim.`dyear` AS `dyear`, date_dim.`ddate` AS `ddate`, date_dim.`d_month_seq`, sum((CAST((((CAST(store_sales.`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(store_sales.`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(store_sales.`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(store_sales.`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_paid`))) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |    GROUP BY customer.`customer_id`, customer.`customer_first_name`, customer.`customer_last_name`, customer.`customer_preferred_cust_flag`, customer.`customer_birth_country`, customer.`customer_login`, customer.`customer_email_address`, date_dim.`dyear`, date_dim.`ddate`, date_dim.`d_month_seq`) gen_subsumer_1 
        |  WHERE
        |    (gen_subsumer_1.`dyear` IN (2001, 2002)) AND (gen_subsumer_1.`dyear` = 2002)
        |  GROUP BY gen_subsumer_1.`customer_id`, gen_subsumer_1.`customer_first_name`, gen_subsumer_1.`customer_last_name`, gen_subsumer_1.`dyear`
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, customer.`c_first_name` AS `customer_first_name`, customer.`c_last_name` AS `customer_last_name`, makedecimal(sum(unscaledvalue(web_sales.`ws_net_paid`))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` IN (2001, 2002)) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  WHERE
        |    false
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`) gen_subquery_1  ON (gen_subquery_1.`customer_id` = gen_subquery_0.`customer_id`)
        |  INNER JOIN (SELECT gen_subsumer_2.`customer_id` AS `customer_id`, gen_subsumer_2.`year_total_74` AS `year_total` 
        |  FROM
        |    (SELECT customer.`c_customer_id` AS `customer_id`, customer.`c_first_name` AS `customer_first_name`, customer.`c_last_name` AS `customer_last_name`, customer.`c_preferred_cust_flag` AS `customer_preferred_cust_flag`, customer.`c_birth_country` AS `customer_birth_country`, customer.`c_login` AS `customer_login`, customer.`c_email_address` AS `customer_email_address`, date_dim.`d_year` AS `dyear`, date_dim.`d_date` AS `ddate`, date_dim.`d_month_seq`, sum((CAST((((CAST(store_sales.`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(store_sales.`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(store_sales.`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(store_sales.`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_paid`))) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |    GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`, date_dim.`d_date`, date_dim.`d_month_seq`) gen_subsumer_2 
        |  WHERE
        |    false AND (gen_subsumer_2.`dyear` IN (2001, 2002))
        |  GROUP BY gen_subsumer_2.`customer_id`, gen_subsumer_2.`customer_first_name`, gen_subsumer_2.`customer_last_name`, gen_subsumer_2.`dyear`
        |  HAVING (gen_subsumer_2.`year_total_74` > 0.00BD)
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, makedecimal(sum(unscaledvalue(web_sales.`ws_net_paid`))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` IN (2001, 2002)) AND (date_dim.`d_year` = 2001) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`
        |  HAVING (`year_total` > 0.00BD)) gen_subquery_2  ON (gen_subquery_0.`customer_id` = gen_subquery_2.`customer_id`)
        |  INNER JOIN (SELECT gen_subsumer_3.`customer_id` AS `customer_id`, gen_subsumer_3.`year_total_74` AS `year_total` 
        |  FROM
        |    (SELECT customer.`customer_id` AS `customer_id`, customer.`customer_first_name` AS `customer_first_name`, customer.`customer_last_name` AS `customer_last_name`, customer.`customer_preferred_cust_flag` AS `customer_preferred_cust_flag`, customer.`customer_birth_country` AS `customer_birth_country`, customer.`customer_login` AS `customer_login`, customer.`customer_email_address` AS `customer_email_address`, date_dim.`dyear` AS `dyear`, date_dim.`ddate` AS `ddate`, date_dim.`d_month_seq`, sum((CAST((((CAST(store_sales.`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(store_sales.`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(store_sales.`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(store_sales.`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_paid`))) AS `year_total_74`, 's' AS `sale_type` 
        |    FROM
        |      customer
        |      INNER JOIN store_sales ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |      INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |    GROUP BY customer.`customer_id`, customer.`customer_first_name`, customer.`customer_last_name`, customer.`customer_preferred_cust_flag`, customer.`customer_birth_country`, customer.`customer_login`, customer.`customer_email_address`, date_dim.`dyear`, date_dim.`ddate`, date_dim.`d_month_seq`) gen_subsumer_3 
        |  WHERE
        |    false AND (gen_subsumer_3.`dyear` IN (2001, 2002))
        |  GROUP BY gen_subsumer_3.`customer_id`, gen_subsumer_3.`customer_first_name`, gen_subsumer_3.`customer_last_name`, gen_subsumer_3.`dyear`
        |  UNION ALL
        |  SELECT customer.`c_customer_id` AS `customer_id`, makedecimal(sum(unscaledvalue(web_sales.`ws_net_paid`))) AS `year_total` 
        |  FROM
        |    customer
        |    INNER JOIN web_sales ON (customer.`c_customer_sk` = web_sales.`ws_bill_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` IN (2001, 2002)) AND (date_dim.`d_year` = 2002) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |  GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`) gen_subquery_3 
        |WHERE
        |  (gen_subquery_0.`customer_id` = gen_subquery_3.`customer_id`) AND (CASE WHEN (gen_subquery_2.`year_total` > 0.00BD) THEN (gen_subquery_3.`year_total` / gen_subquery_2.`year_total`) ELSE CAST(NULL AS DECIMAL(37,20)) END > CASE WHEN (gen_subquery_0.`year_total` > 0.00BD) THEN (gen_subquery_1.`year_total` / gen_subquery_0.`year_total`) ELSE CAST(NULL AS DECIMAL(37,20)) END)
        |ORDER BY gen_subquery_1.`customer_id` ASC NULLS FIRST, gen_subquery_1.`customer_id` ASC NULLS FIRST, gen_subquery_1.`customer_id` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      //q87
      ("case_21",
       """
        | SELECT c_customer_id customer_id,
        |        c_first_name customer_first_name,
        |        c_last_name customer_last_name,
        |        c_preferred_cust_flag customer_preferred_cust_flag,
        |        c_birth_country customer_birth_country,
        |        c_login customer_login,
        |        c_email_address customer_email_address,
        |        d_year dyear,
        |        d_date ddate,
        |        d_month_seq,
        |        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
        |        sum(ss_net_paid) year_total_74,
        |        's' sale_type
        | FROM customer, store_sales, date_dim
        | WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
        | GROUP BY c_customer_id,
        |          c_first_name,
        |          c_last_name,
        |          c_preferred_cust_flag,
        |          c_birth_country,
        |          c_login,
        |          c_email_address,
        |          d_year,
        |          d_date,
        |          d_month_seq
       """.stripMargin.trim,
       """
        | select count(*)
        | from ((select distinct c_last_name, c_first_name, d_date
        |       from store_sales, date_dim, customer
        |       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |         and store_sales.ss_customer_sk = customer.c_customer_sk
        |         and d_month_seq between 1200 and 1200+11)
        |       except
        |      (select distinct c_last_name, c_first_name, d_date
        |       from catalog_sales, date_dim, customer
        |       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        |         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
        |         and d_month_seq between 1200 and 1200+11)
        |       except
        |      (select distinct c_last_name, c_first_name, d_date
        |       from web_sales, date_dim, customer
        |       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
        |         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
        |         and d_month_seq between 1200 and 1200+11)
        |) cool_cust
       """.stripMargin.trim,
       """
        |SELECT count(1) AS `count(1)` 
        |FROM
        |  (SELECT gen_subquery_5.`c_last_name`, gen_subquery_5.`c_first_name`, gen_subquery_5.`d_date` 
        |  FROM
        |    (SELECT gen_subquery_3.`c_last_name`, gen_subquery_3.`c_first_name`, gen_subquery_3.`d_date` 
        |    FROM
        |      (SELECT gen_subquery_2.`c_last_name`, gen_subquery_2.`c_first_name`, gen_subquery_2.`d_date` 
        |      FROM
        |        (SELECT gen_subquery_0.`c_last_name`, gen_subquery_0.`c_first_name`, gen_subquery_0.`d_date` 
        |        FROM
        |          (SELECT gen_subsumer_0.`customer_last_name` AS `c_last_name`, gen_subsumer_0.`customer_first_name` AS `c_first_name`, gen_subsumer_0.`ddate` AS `d_date` 
        |          FROM
        |            (SELECT customer.`c_customer_id` AS `customer_id`, customer.`c_first_name` AS `customer_first_name`, customer.`c_last_name` AS `customer_last_name`, customer.`c_preferred_cust_flag` AS `customer_preferred_cust_flag`, customer.`c_birth_country` AS `customer_birth_country`, customer.`c_login` AS `customer_login`, customer.`c_email_address` AS `customer_email_address`, date_dim.`d_year` AS `dyear`, date_dim.`d_date` AS `ddate`, date_dim.`d_month_seq`, sum((CAST((((CAST(store_sales.`ss_ext_list_price` AS DECIMAL(8,2)) - CAST(store_sales.`ss_ext_wholesale_cost` AS DECIMAL(8,2))) - CAST(store_sales.`ss_ext_discount_amt` AS DECIMAL(8,2))) + CAST(store_sales.`ss_ext_sales_price` AS DECIMAL(8,2))) AS DECIMAL(12,2)) / 2.00BD)) AS `year_total`, sum(store_sales.`ss_net_paid`) AS `year_total_74`, 's' AS `sale_type` 
        |            FROM
        |              customer
        |              INNER JOIN store_sales ON (customer.`c_customer_sk` = store_sales.`ss_customer_sk`)
        |              INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |            GROUP BY customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, customer.`c_preferred_cust_flag`, customer.`c_birth_country`, customer.`c_login`, customer.`c_email_address`, date_dim.`d_year`, date_dim.`d_date`, date_dim.`d_month_seq`) gen_subsumer_0 
        |          WHERE
        |            (gen_subsumer_0.`d_month_seq` >= 1200) AND (gen_subsumer_0.`d_month_seq` <= 1211)
        |          GROUP BY gen_subsumer_0.`customer_last_name`, gen_subsumer_0.`customer_first_name`, gen_subsumer_0.`ddate`) gen_subquery_0 
        |          LEFT ANTI JOIN (SELECT customer.`c_last_name`, customer.`c_first_name`, date_dim.`d_date` 
        |          FROM
        |            catalog_sales
        |            INNER JOIN date_dim ON (date_dim.`d_month_seq` >= 1200) AND (date_dim.`d_month_seq` <= 1211) AND (catalog_sales.`cs_sold_date_sk` = date_dim.`d_date_sk`)
        |            INNER JOIN customer ON (catalog_sales.`cs_bill_customer_sk` = customer.`c_customer_sk`)
        |          GROUP BY customer.`c_last_name`, customer.`c_first_name`, date_dim.`d_date`) gen_subquery_1  ON (gen_subquery_0.`c_last_name` <=> gen_subquery_1.`c_last_name`) AND (gen_subquery_0.`c_first_name` <=> gen_subquery_1.`c_first_name`) AND (gen_subquery_0.`d_date` <=> gen_subquery_1.`d_date`)) gen_subquery_2 
        |      GROUP BY gen_subquery_2.`c_last_name`, gen_subquery_2.`c_first_name`, gen_subquery_2.`d_date`) gen_subquery_3 
        |      LEFT ANTI JOIN (SELECT customer.`c_last_name`, customer.`c_first_name`, date_dim.`d_date` 
        |      FROM
        |        web_sales
        |        INNER JOIN date_dim ON (date_dim.`d_month_seq` >= 1200) AND (date_dim.`d_month_seq` <= 1211) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |        INNER JOIN customer ON (web_sales.`ws_bill_customer_sk` = customer.`c_customer_sk`)
        |      GROUP BY customer.`c_last_name`, customer.`c_first_name`, date_dim.`d_date`) gen_subquery_4  ON (gen_subquery_3.`c_last_name` <=> gen_subquery_4.`c_last_name`) AND (gen_subquery_3.`c_first_name` <=> gen_subquery_4.`c_first_name`) AND (gen_subquery_3.`d_date` <=> gen_subquery_4.`d_date`)) gen_subquery_5 
        |  GROUP BY gen_subquery_5.`c_last_name`, gen_subquery_5.`c_first_name`, gen_subquery_5.`d_date`) gen_subquery_6 
        """.stripMargin.trim),
      // the following two queries involve an MV of store_sales, date_dim, store
      //q43
      ("case_22",
       """
        | select s_store_name, s_store_id, s_gmt_offset, d_year, s_state, s_county, d_month_seq,
        |        sum(ss_net_profit),
        |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
        | from date_dim, store_sales, store
        | where d_date_sk = ss_sold_date_sk and
        |       s_store_sk = ss_store_sk 
        | group by s_store_name, s_store_id, s_gmt_offset, d_year, s_state, s_county, d_month_seq
       """.stripMargin.trim,
       """
        | select s_store_name, s_store_id,
        |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
        | from date_dim, store_sales, store
        | where d_date_sk = ss_sold_date_sk and
        |       s_store_sk = ss_store_sk and
        |       s_gmt_offset = -5 and
        |       d_year = 2000
        | group by s_store_name, s_store_id
        | order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,
        |          thu_sales,fri_sales,sat_sales
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subsumer_0.`s_store_name`, gen_subsumer_0.`s_store_id`, sum(gen_subsumer_0.`sun_sales`) AS `sun_sales`, sum(gen_subsumer_0.`mon_sales`) AS `mon_sales`, sum(gen_subsumer_0.`tue_sales`) AS `tue_sales`, sum(gen_subsumer_0.`wed_sales`) AS `wed_sales`, sum(gen_subsumer_0.`thu_sales`) AS `thu_sales`, sum(gen_subsumer_0.`fri_sales`) AS `fri_sales`, sum(gen_subsumer_0.`sat_sales`) AS `sat_sales` 
        |FROM
        |  (SELECT store.`s_store_name`, store.`s_store_id`, store.`s_gmt_offset`, date_dim.`d_year`, store.`s_state`, store.`s_county`, date_dim.`d_month_seq`, sum(store_sales.`ss_net_profit`) AS `sum(ss_net_profit)`, sum(CASE WHEN (date_dim.`d_day_name` = 'Sunday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `sun_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Monday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `mon_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Tuesday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `tue_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Wednesday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `wed_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Thursday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `thu_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Friday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `fri_sales`, sum(CASE WHEN (date_dim.`d_day_name` = 'Saturday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END) AS `sat_sales` 
        |  FROM
        |    date_dim
        |    INNER JOIN store_sales ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |    INNER JOIN store ON (store.`s_store_sk` = store_sales.`ss_store_sk`)
        |  GROUP BY store.`s_store_name`, store.`s_store_id`, store.`s_gmt_offset`, date_dim.`d_year`, store.`s_state`, store.`s_county`, date_dim.`d_month_seq`) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`d_year` = 2000) AND (CAST(gen_subsumer_0.`s_gmt_offset` AS DECIMAL(12,2)) = -5.00BD)
        |GROUP BY gen_subsumer_0.`s_store_name`, gen_subsumer_0.`s_store_id`
        |ORDER BY gen_subsumer_0.`s_store_name` ASC NULLS FIRST, gen_subsumer_0.`s_store_id` ASC NULLS FIRST, `sun_sales` ASC NULLS FIRST, `mon_sales` ASC NULLS FIRST, `tue_sales` ASC NULLS FIRST, `wed_sales` ASC NULLS FIRST, `thu_sales` ASC NULLS FIRST, `fri_sales` ASC NULLS FIRST, `sat_sales` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      // q70
      ("case_23",
       """
        | select s_store_name, s_store_id, s_gmt_offset, d_year, s_state, s_county, d_month_seq,
        |        sum(ss_net_profit),
        |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
        | from date_dim, store_sales, store
        | where d_date_sk = ss_sold_date_sk and
        |       s_store_sk = ss_store_sk 
        | group by s_store_name, s_store_id, s_gmt_offset, d_year, s_state, s_county, d_month_seq
       """.stripMargin.trim,
       """
        | select
        |    sum(ss_net_profit) as total_sum, s_state, s_county
        |   ,grouping(s_state)+grouping(s_county) as lochierarchy
        |   ,rank() over (
        |       partition by grouping(s_state)+grouping(s_county),
        |       case when grouping(s_county) = 0 then s_state end
        |       order by sum(ss_net_profit) desc) as rank_within_parent
        | from
        |    store_sales, date_dim d1, store
        | where
        |    d1.d_month_seq between 1200 and 1200+11
        | and d1.d_date_sk = ss_sold_date_sk
        | and s_store_sk  = ss_store_sk
        | and s_state in
        |    (select s_state from
        |        (select s_state as s_state,
        |                         rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
        |         from store_sales, store, date_dim
        |         where  d_month_seq between 1200 and 1200+11
        |                      and d_date_sk = ss_sold_date_sk
        |                      and s_store_sk  = ss_store_sk
        |         group by s_state) tmp1
        |     where ranking <= 5)
        | group by rollup(s_state,s_county)
        | order by
        |   lochierarchy desc
        |  ,case when lochierarchy = 0 then s_state end
        |  ,rank_within_parent
        | limit 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_5.`total_sum`, gen_subquery_5.`s_state`, gen_subquery_5.`s_county`, gen_subquery_5.`lochierarchy`, gen_subquery_5.`rank_within_parent` 
        |FROM
        |  (SELECT gen_subquery_4.`total_sum`, gen_subquery_4.`s_state`, gen_subquery_4.`s_county`, gen_subquery_4.`lochierarchy`, gen_subquery_4.`_w1`, gen_subquery_4.`_w2`, gen_subquery_4.`_w3`, RANK() OVER (PARTITION BY gen_subquery_4.`_w1`, gen_subquery_4.`_w2` ORDER BY gen_subquery_4.`_w3` DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `rank_within_parent` 
        |  FROM
        |    (SELECT makedecimal(sum(unscaledvalue(store_sales.`ss_net_profit`))) AS gen_subquery_3.`total_sum`, gen_subquery_3.`s_state`, gen_subquery_3.`s_county`, (CAST((shiftright(`spark_grouping_id`, 1) & 1) AS TINYINT) + CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT)) AS gen_subquery_3.`lochierarchy`, (CAST((shiftright(`spark_grouping_id`, 1) & 1) AS TINYINT) + CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT)) AS gen_subquery_3.`_w1`, CASE WHEN (CAST(CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT) AS INT) = 0) THEN gen_subquery_3.`s_state` END AS gen_subquery_3.`_w2`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_profit`))) AS gen_subquery_3.`_w3` 
        |    FROM
        |      (SELECT makedecimal(sum(unscaledvalue(gen_subquery_2.`ss_net_profit`))) AS `total_sum`, `s_state`, `s_county`, (CAST((shiftright(`spark_grouping_id`, 1) & 1) AS TINYINT) + CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT)) AS `lochierarchy`, (CAST((shiftright(`spark_grouping_id`, 1) & 1) AS TINYINT) + CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT)) AS `_w1`, CASE WHEN (CAST(CAST((shiftright(`spark_grouping_id`, 0) & 1) AS TINYINT) AS INT) = 0) THEN `s_state` END AS `_w2`, makedecimal(sum(unscaledvalue(gen_subquery_2.`ss_net_profit`))) AS `_w3` 
        |      FROM
        |        (SELECT store_sales.`ss_net_profit`, store.`s_state` AS `s_state`, store.`s_county` AS `s_county` 
        |        FROM
        |          store_sales
        |          INNER JOIN date_dim d1  ON (d1.`d_month_seq` >= 1200) AND (d1.`d_month_seq` <= 1211) AND (d1.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |          INNER JOIN store ON (store.`s_store_sk` = store_sales.`ss_store_sk`)
        |          LEFT SEMI JOIN (SELECT gen_subquery_0.`s_state`, gen_subquery_0.`s_state`, gen_subquery_0.`_w1`, RANK() OVER (PARTITION BY gen_subquery_0.`s_state` ORDER BY gen_subquery_0.`_w1` DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `ranking` 
        |          FROM
        |            (SELECT gen_subsumer_0.`s_state` AS `s_state`, gen_subsumer_0.`s_state`, gen_subsumer_0.`sum(ss_net_profit)` AS `_w1` 
        |            FROM
        |              (SELECT store.`s_store_name`, store.`s_store_id`, store.`s_gmt_offset`, date_dim.`d_year`, store.`s_state`, store.`s_county`, date_dim.`d_month_seq`, makedecimal(sum(unscaledvalue(store_sales.`ss_net_profit`))) AS `sum(ss_net_profit)`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Sunday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `sun_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Monday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `mon_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Tuesday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `tue_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Wednesday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `wed_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Thursday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `thu_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Friday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `fri_sales`, makedecimal(sum(unscaledvalue(CASE WHEN (date_dim.`d_day_name` = 'Saturday') THEN store_sales.`ss_sales_price` ELSE CAST(NULL AS DECIMAL(7,2)) END))) AS `sat_sales` 
        |              FROM
        |                date_dim
        |                INNER JOIN store_sales ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |                INNER JOIN store ON (store.`s_store_sk` = store_sales.`ss_store_sk`)
        |              GROUP BY store.`s_store_name`, store.`s_store_id`, store.`s_gmt_offset`, date_dim.`d_year`, store.`s_state`, store.`s_county`, date_dim.`d_month_seq`) gen_subsumer_0 
        |            WHERE
        |              (gen_subsumer_0.`d_month_seq` >= 1200) AND (gen_subsumer_0.`d_month_seq` <= 1211)
        |            GROUP BY gen_subsumer_0.`s_state`) gen_subquery_0 ) gen_subquery_1  ON (store.`s_state` = gen_subquery_1.`s_state`)
        |        WHERE
        |          (gen_subquery_1.`ranking` <= 5)) gen_subquery_2 
        |      GROUP BY `s_state`, `s_county`, `spark_grouping_id`) gen_subquery_3 ) gen_subquery_4 ) gen_subquery_5 
        |ORDER BY gen_subquery_5.`lochierarchy` DESC NULLS LAST, CASE WHEN (CAST(gen_subquery_5.`lochierarchy` AS INT) = 0) THEN gen_subquery_5.`s_state` END ASC NULLS FIRST, gen_subquery_5.`rank_within_parent` ASC NULLS FIRST
        |LIMIT 100
       """.stripMargin.trim),
      // the following five queries involve an MV of web_sales, date_dim, item
      //q12
      ("case_24",
       """
        | select 
        |  i_item_desc, i_item_id, i_category, i_class, i_current_price, i_brand_id, i_class_id, i_category_id,
        |  ws_ship_customer_sk, d_year, d_qoy, d_date, 
        |  sum(ws_quantity*ws_list_price) sales, count(*) number_sales,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_net_paid) as total_sum
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |   i_item_id, i_item_desc, i_category, i_category_id, i_class, i_current_price, i_brand_id,i_class_id,ws_ship_customer_sk,d_year,d_qoy,d_date
       """.stripMargin.trim,
       """
        | select 
        |  i_item_desc, i_category, i_class, i_current_price,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
        |          (partition by i_class) as revenueratio
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and i_category in ('Sports', 'Books', 'Home')
        |   and ws_sold_date_sk = d_date_sk
        |   and d_date between cast('1999-02-22' as date)
        |                           and (cast('1999-02-22' as date) + interval 30 days)
        | group by
        |   i_item_id, i_item_desc, i_category, i_class, i_current_price
        | order by
        |   i_category, i_class, i_item_id, i_item_desc, revenueratio
        | LIMIT 100
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_1.`i_item_desc`, gen_subquery_1.`i_category`, gen_subquery_1.`i_class`, gen_subquery_1.`i_current_price`, gen_subquery_1.`itemrevenue`, ((gen_subquery_1.`_w0` * 100.00BD) / CAST(gen_subquery_1.`_we0` AS DECIMAL(28,2))) AS `revenueratio` 
        |FROM
        |  (SELECT gen_subquery_0.`i_item_desc`, gen_subquery_0.`i_category`, gen_subquery_0.`i_class`, gen_subquery_0.`i_current_price`, gen_subquery_0.`itemrevenue`, gen_subquery_0.`_w0`, gen_subquery_0.`_w1`, gen_subquery_0.`i_item_id`, sum(gen_subquery_0.`_w1`) OVER (PARTITION BY gen_subquery_0.`i_class` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `_we0` 
        |  FROM
        |    (SELECT gen_subsumer_0.`i_item_desc`, gen_subsumer_0.`i_category`, gen_subsumer_0.`i_class`, gen_subsumer_0.`i_current_price`, gen_subsumer_0.`itemrevenue` AS `itemrevenue`, gen_subsumer_0.`itemrevenue` AS `_w0`, gen_subsumer_0.`itemrevenue` AS `_w1`, gen_subsumer_0.`i_item_id` 
        |    FROM
        |      (SELECT item.`i_item_desc`, item.`i_item_id`, item.`i_category`, item.`i_class`, item.`i_current_price`, item.`i_brand_id`, item.`i_class_id`, item.`i_category_id`, web_sales.`ws_ship_customer_sk`, date_dim.`d_year`, date_dim.`d_qoy`, date_dim.`d_date`, sum((CAST(CAST(web_sales.`ws_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(web_sales.`ws_list_price` AS DECIMAL(12,2)))) AS `sales`, count(1) AS `number_sales`, makedecimal(sum(unscaledvalue(web_sales.`ws_ext_sales_price`))) AS `itemrevenue`, makedecimal(sum(unscaledvalue(web_sales.`ws_net_paid`))) AS `total_sum` 
        |      FROM
        |        web_sales
        |        INNER JOIN item ON (web_sales.`ws_item_sk` = item.`i_item_sk`)
        |        INNER JOIN date_dim ON (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)
        |      GROUP BY item.`i_item_id`, item.`i_item_desc`, item.`i_category`, item.`i_category_id`, item.`i_class`, item.`i_current_price`, item.`i_brand_id`, item.`i_class_id`, web_sales.`ws_ship_customer_sk`, date_dim.`d_year`, date_dim.`d_qoy`, date_dim.`d_date`) gen_subsumer_0 
        |    WHERE
        |      (gen_subsumer_0.`i_category` IN ('Sports', 'Books', 'Home')) AND (gen_subsumer_0.`d_date` >= DATE '1999-02-22') AND (gen_subsumer_0.`d_date` <= DATE '1999-03-24')
        |    GROUP BY gen_subsumer_0.`i_item_id`, gen_subsumer_0.`i_item_desc`, gen_subsumer_0.`i_category`, gen_subsumer_0.`i_class`, gen_subsumer_0.`i_current_price`) gen_subquery_0 ) gen_subquery_1 
        |ORDER BY gen_subquery_1.`i_category` ASC NULLS FIRST, gen_subquery_1.`i_class` ASC NULLS FIRST, gen_subquery_1.`i_item_id` ASC NULLS FIRST, gen_subquery_1.`i_item_desc` ASC NULLS FIRST, `revenueratio` ASC NULLS FIRST
        |LIMIT 100
        """.stripMargin.trim),
      //q14a (no modular plan because EXPAND-UNION, need to fix)
      ("case_25",
       """
        | select 
        |  i_item_desc, i_item_id, i_category, i_class, i_current_price, i_brand_id, i_class_id, i_category_id,
        |  ws_ship_customer_sk, d_year, d_qoy, d_date, 
        |  sum(ws_quantity*ws_list_price) sales, count(*) number_sales,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_net_paid) as total_sum
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |   i_item_id, i_item_desc, i_category, i_category_id, i_class, i_current_price, i_brand_id,i_class_id,ws_ship_customer_sk,d_year,d_qoy,d_date
       """.stripMargin.trim,
       """
        |with cross_items as
        | (select i_item_sk ss_item_sk
        | from item,
        |    (select iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
        |     from store_sales, item iss, date_dim d1
        |     where ss_item_sk = iss.i_item_sk
        |       and ss_sold_date_sk = d1.d_date_sk
        |       and d1.d_year between 1999 AND 1999 + 2
        |   intersect
        |     select ics.i_brand_id, ics.i_class_id, ics.i_category_id
        |     from catalog_sales, item ics, date_dim d2
        |     where cs_item_sk = ics.i_item_sk
        |       and cs_sold_date_sk = d2.d_date_sk
        |       and d2.d_year between 1999 AND 1999 + 2
        |   intersect
        |     select iws.i_brand_id, iws.i_class_id, iws.i_category_id
        |     from web_sales, item iws, date_dim d3
        |     where ws_item_sk = iws.i_item_sk
        |       and ws_sold_date_sk = d3.d_date_sk
        |       and d3.d_year between 1999 AND 1999 + 2) x
        | where i_brand_id = brand_id
        |   and i_class_id = class_id
        |   and i_category_id = category_id
        |),
        | avg_sales as
        | (select avg(quantity*list_price) average_sales
        |  from (
        |     select ss_quantity quantity, ss_list_price list_price
        |     from store_sales, date_dim
        |     where ss_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 2001
        |   union all
        |     select cs_quantity quantity, cs_list_price list_price
        |     from catalog_sales, date_dim
        |     where cs_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 1999 + 2
        |   union all
        |     select ws_quantity quantity, ws_list_price list_price
        |     from web_sales, date_dim
        |     where ws_sold_date_sk = d_date_sk
        |       and d_year between 1999 and 1999 + 2) x)
        | select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
        | from(
        |     select 'store' channel, i_brand_id,i_class_id
        |             ,i_category_id,sum(ss_quantity*ss_list_price) sales
        |             , count(*) number_sales
        |     from store_sales, item, date_dim
        |     where ss_item_sk in (select ss_item_sk from cross_items)
        |       and ss_item_sk = i_item_sk
        |       and ss_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
        |   union all
        |     select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
        |     from catalog_sales, item, date_dim
        |     where cs_item_sk in (select ss_item_sk from cross_items)
        |       and cs_item_sk = i_item_sk
        |       and cs_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
        |   union all
        |     select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
        |     from web_sales, item, date_dim
        |     where ws_item_sk in (select ss_item_sk from cross_items)
        |       and ws_item_sk = i_item_sk
        |       and ws_sold_date_sk = d_date_sk
        |       and d_year = 1999+2
        |       and d_moy = 11
        |     group by i_brand_id,i_class_id,i_category_id
        |     having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
        | ) y
        | group by rollup (channel, i_brand_id,i_class_id,i_category_id)
        | order by channel,i_brand_id,i_class_id,i_category_id
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      //q86
      ("case_26",
       """
        | select 
        |  i_item_desc, i_item_id, i_category, i_class, i_current_price, i_brand_id, i_class_id, i_category_id,
        |  ws_ship_customer_sk, d_year, d_qoy, d_date, d_month_seq,
        |  sum(ws_quantity*ws_list_price) sales, count(*) number_sales,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_net_paid) as total_sum
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |   i_item_id, i_item_desc, i_category, i_category_id, i_class, i_current_price, i_brand_id,i_class_id,ws_ship_customer_sk,d_year,d_qoy,d_date, d_month_seq
       """.stripMargin.trim,
       """
        | select sum(ws_net_paid) as total_sum, i_category, i_class,
        |  grouping(i_category)+grouping(i_class) as lochierarchy,
        |  rank() over (
        |       partition by grouping(i_category)+grouping(i_class),
        |       case when grouping(i_class) = 0 then i_category end
        |       order by sum(ws_net_paid) desc) as rank_within_parent
        | from
        |    web_sales, date_dim d1, item
        | where
        |    d1.d_month_seq between 1200 and 1200+11
        | and d1.d_date_sk = ws_sold_date_sk
        | and i_item_sk  = ws_item_sk
        | group by rollup(i_category,i_class)
        | order by
        |   lochierarchy desc,
        |   case when lochierarchy = 0 then i_category end,
        |   rank_within_parent
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      //q58
      ("case_27",
       """
        | select 
        |  i_item_desc, i_item_id, i_category, i_class, i_current_price, i_brand_id, i_class_id, i_category_id,
        |  ws_ship_customer_sk, d_week_seq, d_year, d_qoy, 
        |  sum(ws_quantity*ws_list_price) sales, count(*) number_sales,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_net_paid) as total_sum
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |   i_item_id, i_item_desc, i_category, i_category_id, i_class, i_current_price, i_brand_id,i_class_id,ws_ship_customer_sk,d_week_seq, d_year,d_qoy
       """.stripMargin.trim,
       """
        | with ss_items as
        | (select i_item_id item_id, sum(ss_ext_sales_price) ss_item_rev
        | from store_sales, item, date_dim
        | where ss_item_sk = i_item_sk
        |   and d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq = (select d_week_seq
        |                                      from date_dim
        |                                      where d_date = '2000-01-03'))
        |   and ss_sold_date_sk   = d_date_sk
        | group by i_item_id),
        | cs_items as
        | (select i_item_id item_id
        |        ,sum(cs_ext_sales_price) cs_item_rev
        |  from catalog_sales, item, date_dim
        | where cs_item_sk = i_item_sk
        |  and  d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq = (select d_week_seq
        |                                      from date_dim
        |                                      where d_date = '2000-01-03'))
        |  and  cs_sold_date_sk = d_date_sk
        | group by i_item_id),
        | ws_items as
        | (select i_item_id item_id, sum(ws_ext_sales_price) ws_item_rev
        |  from web_sales, item, date_dim
        | where ws_item_sk = i_item_sk
        |  and  d_date in (select d_date
        |                  from date_dim
        |                  where d_week_seq =(select d_week_seq
        |                                     from date_dim
        |                                     where d_date = '2000-01-03'))
        |  and ws_sold_date_sk   = d_date_sk
        | group by i_item_id)
        | select ss_items.item_id
        |       ,ss_item_rev
        |       ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
        |       ,cs_item_rev
        |       ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
        |       ,ws_item_rev
        |       ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
        |       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
        | from ss_items,cs_items,ws_items
        | where ss_items.item_id=cs_items.item_id
        |   and ss_items.item_id=ws_items.item_id
        |   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
        |   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
        |   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
        |   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
        |   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
        |   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
        | order by item_id, ss_item_rev
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      //q76
      ("case_28",
       """
        | select 
        |  i_item_desc, i_item_id, i_category, i_class, i_current_price, i_brand_id, i_class_id, i_category_id,
        |  ws_ship_customer_sk, d_year, d_qoy, 
        |  sum(ws_quantity*ws_list_price) sales, count(*) number_sales,
        |  sum(ws_ext_sales_price) as itemrevenue,
        |  sum(ws_net_paid) as total_sum
        | from
        |   web_sales, item, date_dim
        | where
        |   ws_item_sk = i_item_sk
        |   and ws_sold_date_sk = d_date_sk
        | group by
        |   i_item_id, i_item_desc, i_category, i_category_id, i_class, i_current_price, i_brand_id,i_class_id,ws_ship_customer_sk,d_year,d_qoy
       """.stripMargin.trim,
       """
        | SELECT
        |    channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt,
        |    SUM(ext_sales_price) sales_amt
        | FROM(
        |    SELECT
        |        'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
        |        ss_ext_sales_price ext_sales_price
        |    FROM store_sales, item, date_dim
        |    WHERE ss_store_sk IS NULL
        |      AND ss_sold_date_sk=d_date_sk
        |      AND ss_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'web' as channel, ws_ship_customer_sk col_name, d_year, d_qoy, i_category,
        |        ws_ext_sales_price ext_sales_price
        |    FROM web_sales, item, date_dim
        |    WHERE ws_ship_customer_sk IS NULL
        |      AND ws_sold_date_sk=d_date_sk
        |      AND ws_item_sk=i_item_sk
        |    UNION ALL
        |    SELECT
        |        'catalog' as channel, cs_ship_addr_sk col_name, d_year, d_qoy, i_category,
        |        cs_ext_sales_price ext_sales_price
        |    FROM catalog_sales, item, date_dim
        |    WHERE cs_ship_addr_sk IS NULL
        |      AND cs_sold_date_sk=d_date_sk
        |      AND cs_item_sk=i_item_sk) foo
        | GROUP BY channel, col_name, d_year, d_qoy, i_category
        | ORDER BY channel, col_name, d_year, d_qoy, i_category
        | limit 100
       """.stripMargin.trim,
       """
        |
        |
        |
        """.stripMargin.trim),
      // MV constructed from tool
      ("case_29",
       """
        |SELECT item.`i_brand`, date_dim.`d_date`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, sum(store_sales.`ss_ext_sales_price`) AS `ext_price`, item.`i_item_id`, date_dim.`d_moy`, item.`i_item_desc`, item.`i_manager_id`, item.`i_class`, item.`i_manufact_id`, count(1) AS `cnt`, item.`i_category`, date_dim.`d_year`, item.`i_current_price`, item.`i_item_sk`, item.`i_brand_id` 
        |FROM
        |  store_sales
        |  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |GROUP BY item.`i_brand`, date_dim.`d_date`, substring(item.`i_item_desc`, 1, 30), item.`i_item_id`, date_dim.`d_moy`, item.`i_item_desc`, item.`i_manager_id`, item.`i_class`, item.`i_manufact_id`, item.`i_category`, date_dim.`d_year`, item.`i_current_price`, item.`i_item_sk`, item.`i_brand_id`
        """.stripMargin.trim,
       """
        | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
        | FROM  date_dim dt, store_sales, item
        | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |   AND store_sales.ss_item_sk = item.i_item_sk
        |   AND item.i_manufact_id = 128
        |   AND dt.d_moy=11
        | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        | ORDER BY dt.d_year, sum_agg desc, brand_id
        | LIMIT 100
        """.stripMargin.trim,
       """
        |SELECT gen_subsumer_0.`d_year`, gen_subsumer_0.`i_brand_id` AS `brand_id`, gen_subsumer_0.`i_brand` AS `brand`, sum(gen_subsumer_0.`sum_agg`) AS `sum_agg` 
        |FROM
        |  (SELECT item.`i_current_price`, date_dim.`d_date`, date_dim.`d_year`, item.`i_item_desc`, item.`i_category`, count(1) AS `cnt`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, date_dim.`d_moy`, item.`i_manager_id`, item.`i_item_sk`, item.`i_manufact_id`, item.`i_brand_id`, item.`i_item_id`, item.`i_brand`, item.`i_class`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg` 
        |  FROM
        |    store_sales
        |    INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)
        |    INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |  GROUP BY item.`i_current_price`, date_dim.`d_date`, date_dim.`d_year`, item.`i_item_desc`, item.`i_category`, substring(item.`i_item_desc`, 1, 30), date_dim.`d_moy`, item.`i_manager_id`, item.`i_item_sk`, item.`i_manufact_id`, item.`i_brand_id`, item.`i_item_id`, item.`i_brand`, item.`i_class`) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`d_moy` = 11) AND (gen_subsumer_0.`i_manufact_id` = 128)
        |GROUP BY gen_subsumer_0.`d_year`, gen_subsumer_0.`i_brand`, gen_subsumer_0.`i_brand_id`
        |ORDER BY gen_subsumer_0.`d_year` ASC NULLS FIRST, `sum_agg` DESC NULLS LAST, `brand_id` ASC NULLS FIRST
        |LIMIT 100 
        """.stripMargin.trim),
      // scalar sub-query
      ("case_30",
       """
        |SELECT c_customer_sk,sum(ss_quantity*ss_sales_price) csales, c_customer_id, 
        |       c_first_name, c_last_name, d_year, sum(ss_net_paid) year_total
        |FROM store_sales, customer, date_dim
        |WHERE ss_customer_sk = c_customer_sk
        |      AND ss_sold_date_sk = d_date_sk
        |GROUP BY c_customer_sk, c_customer_id, c_first_name, c_last_name, d_year 
        """.stripMargin.trim,
       """
        | with frequent_ss_items as
        | (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
        |  from store_sales, date_dim, item
        |  where ss_sold_date_sk = d_date_sk
        |    and ss_item_sk = i_item_sk
        |    and d_year in (2000, 2000+1, 2000+2,2000+3)
        |  group by substr(i_item_desc,1,30),i_item_sk,d_date
        |  having count(*) >4),
        | max_store_sales as
        | (select max(csales) tpcds_cmax
        |  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        |        from store_sales, customer, date_dim
        |        where ss_customer_sk = c_customer_sk
        |         and ss_sold_date_sk = d_date_sk
        |         and d_year in (2000, 2000+1, 2000+2,2000+3)
        |        group by c_customer_sk) x),
        | best_ss_customer as
        | (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
        |  from store_sales, customer
        |  where ss_customer_sk = c_customer_sk
        |  group by c_customer_sk
        |  having sum(ss_quantity*ss_sales_price) > (50/100.0) *
        |    (select * from max_store_sales))
        | select sum(sales)
        | from ((select cs_quantity*cs_list_price sales
        |       from catalog_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and cs_sold_date_sk = d_date_sk
        |         and cs_item_sk in (select item_sk from frequent_ss_items)
        |         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer))
        |      union all
        |      (select ws_quantity*ws_list_price sales
        |       from web_sales, date_dim
        |       where d_year = 2000
        |         and d_moy = 2
        |         and ws_sold_date_sk = d_date_sk
        |         and ws_item_sk in (select item_sk from frequent_ss_items)
        |         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))) y
        | limit 100
        """.stripMargin.trim,
       """
        |SELECT sum(gen_subquery_4.`sales`) AS `sum(sales)` 
        |FROM
        |  (SELECT (CAST(CAST(catalog_sales.`cs_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(catalog_sales.`cs_list_price` AS DECIMAL(12,2))) AS `sales` 
        |  FROM
        |    catalog_sales
        |    LEFT SEMI JOIN (SELECT item.`i_item_sk` AS `item_sk`, count(1) AS `count(1)` 
        |    FROM
        |      store_sales
        |      INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |      INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |    GROUP BY substring(item.`i_item_desc`, 1, 30), item.`i_item_sk`, date_dim.`d_date`) gen_subquery_0  ON (gen_subquery_0.`count(1)` > 4L) AND (catalog_sales.`cs_item_sk` = gen_subquery_0.`item_sk`)
        |    LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#683 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#686 as decimal(12,2)))), DecimalType(18,2)))` 
        |    FROM
        |      store_sales
        |      INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |    GROUP BY customer.`c_customer_sk`) gen_subquery_1  ON (CAST(gen_subquery_1.`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#683 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#686 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_expression_0_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum(gen_subsumer_0.`csales`) AS `csales`   FROM  (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`, customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`, sum(store_sales.`ss_net_paid`) AS `year_total`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`, customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`) gen_subsumer_0   WHERE  (gen_subsumer_0.`d_year` IN (2000, 2001, 2002, 2003))  GROUP BY gen_subsumer_0.`c_customer_sk`) gen_expression_0_0 ) AS DECIMAL(32,6)))) AND (catalog_sales.`cs_bill_customer_sk` = gen_subquery_1.`c_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (catalog_sales.`cs_sold_date_sk` = date_dim.`d_date_sk`)
        |  UNION ALL
        |  SELECT (CAST(CAST(web_sales.`ws_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(web_sales.`ws_list_price` AS DECIMAL(12,2))) AS `sales` 
        |  FROM
        |    web_sales
        |    LEFT SEMI JOIN (SELECT item.`i_item_sk` AS `item_sk`, count(1) AS `count(1)` 
        |    FROM
        |      store_sales
        |      INNER JOIN date_dim ON (date_dim.`d_year` IN (2000, 2001, 2002, 2003)) AND (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
        |      INNER JOIN item   ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
        |    GROUP BY substring(item.`i_item_desc`, 1, 30), item.`i_item_sk`, date_dim.`d_date`) gen_subquery_2  ON (gen_subquery_2.`count(1)` > 4L) AND (web_sales.`ws_item_sk` = gen_subquery_2.`item_sk`)
        |    LEFT SEMI JOIN (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#683 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#686 as decimal(12,2)))), DecimalType(18,2)))` 
        |    FROM
        |      store_sales
        |      INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)
        |    GROUP BY customer.`c_customer_sk`) gen_subquery_3  ON (CAST(gen_subquery_3.`sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#683 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#686 as decimal(12,2)))), DecimalType(18,2)))` AS DECIMAL(38,8)) > (0.500000BD * CAST((SELECT max(gen_expression_1_0.`csales`) AS `tpcds_cmax`   FROM  (SELECT sum(gen_subsumer_1.`csales`) AS `csales`   FROM  (SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `csales`, customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`, sum(store_sales.`ss_net_paid`) AS `year_total`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  GROUP BY customer.`c_customer_sk`, customer.`c_customer_id`, customer.`c_first_name`, customer.`c_last_name`, date_dim.`d_year`) gen_subsumer_1   WHERE  (gen_subsumer_1.`d_year` IN (2000, 2001, 2002, 2003))  GROUP BY gen_subsumer_1.`c_customer_sk`) gen_expression_1_0 ) AS DECIMAL(32,6)))) AND (web_sales.`ws_bill_customer_sk` = gen_subquery_3.`c_customer_sk`)
        |    INNER JOIN date_dim ON (date_dim.`d_year` = 2000) AND (date_dim.`d_moy` = 2) AND (web_sales.`ws_sold_date_sk` = date_dim.`d_date_sk`)) gen_subquery_4 
        |LIMIT 100
        """.stripMargin.trim),
      // harmonization, benchmark SEQ
      ("case_31",
       """
        |SELECT CAST(((FLOOR(((CAST(sdr_dyn_seq_custer_iot_all_hour_60min.`STARTTIME` AS DOUBLE) + 28800.0D) / 3600.0D)) * 3600L) - 28800L) AS INT) AS `a3600`, dim_apn_iot.`a12575903189`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_1`, sum(1L) AS `CUSTER_IOT_GRP_USER_NUM_STREAM_C`, sdr_dyn_seq_custer_iot_all_hour_60min.`STARTTIME`, dim_apn_iot.`a12575847251`, dim_apn_iot.`a12575817396`, dim_apn_iot.`a12575873557` 
        |FROM
        |  sdr_dyn_seq_custer_iot_all_hour_60min
        |  INNER JOIN (SELECT dim_apn_iot.`INDUSTRY` AS `a12575903189`, dim_apn_iot.`APN_NAME` AS `a12575817396`, dim_apn_iot.`CITY_ASCRIPTION` AS `a12575873557`, dim_apn_iot.`SERVICE_LEVEL` AS `a12575847251` 
        |  FROM
        |    dim_apn_iot
        |  WHERE
        |    (dim_apn_iot.`CITY_ASCRIPTION` IN ('金华', '丽水', '台州', '舟山', '嘉兴', '宁波', '温州', '绍兴', '湖州', '杭州', '衢州', '省直管', '外省地市', '测试')) AND (dim_apn_iot.`INDUSTRY` IN ('公共管理', '卫生社保', '电力供应', '金融业', '软件业', '文体娱业', '居民服务', '科研技术', '交运仓储', '建筑业', '租赁服务', '制造业', '住宿餐饮', '公共服务', '批发零售', '农林牧渔')) AND (dim_apn_iot.`SERVICE_LEVEL` IN ('金', '标准', '银', '铜'))
        |  GROUP BY dim_apn_iot.`INDUSTRY`, dim_apn_iot.`APN_NAME`, dim_apn_iot.`CITY_ASCRIPTION`, dim_apn_iot.`SERVICE_LEVEL`) dim_apn_iot  ON (sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_51` = dim_apn_iot.`a12575817396`)
        |GROUP BY dim_apn_iot.`a12575903189`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_1`, sdr_dyn_seq_custer_iot_all_hour_60min.`STARTTIME`, dim_apn_iot.`a12575847251`, dim_apn_iot.`a12575817396`, dim_apn_iot.`a12575873557`
       """.stripMargin.trim,
       """
        |SELECT AT.a3600 AS START_TIME
        |	,SUM(CUSTER_IOT_GRP_USER_NUMBER_M) AS USER_NUMBER
        |	,AT.a12575873557 AS CITY_ASCRIPTION
        |	,AT.a12575847251 AS SERVICE_LEVEL
        |	,AT.a12575903189 AS INDUSTRY
        |FROM (
        |	SELECT MT.a3600 AS a3600
        |		,MT.a12575873557 AS a12575873557
        |		,MT.a12575847251 AS a12575847251
        |		,MT.a12575903189 AS a12575903189
        |		,SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_CA
        |		,(
        |			CASE 
        |				WHEN (SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0))) > 0
        |					THEN 1
        |				ELSE 0
        |				END
        |			) AS CUSTER_IOT_GRP_USER_NUMBER_M
        |		,MT.a204010101 AS a204010101
        |	FROM (
        |		SELECT cast(floor((STARTTIME + 28800) / 3600) * 3600 - 28800 AS INT) AS a3600
        |			,SUM(COALESCE(1, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_C
        |			,D12575657700_H104.a12575903189 AS a12575903189
        |			,DIM_52 AS a204010101
        |			,D12575657700_H104.a12575873557 AS a12575873557
        |			,D12575657700_H104.a12575847251 AS a12575847251
        |		FROM SDR_DYN_SEQ_CUSTER_IOT_ALL_HOUR_60MIN
        |		LEFT JOIN (
        |			SELECT INDUSTRY AS a12575903189
        |				,APN_NAME AS a12575817396
        |				,CITY_ASCRIPTION AS a12575873557
        |				,SERVICE_LEVEL AS a12575847251
        |			FROM DIM_APN_IOT
        |			GROUP BY INDUSTRY
        |				,APN_NAME
        |				,CITY_ASCRIPTION
        |				,SERVICE_LEVEL
        |			) D12575657700_H104 ON DIM_51 = D12575657700_H104.a12575817396
        |		WHERE (
        |				D12575657700_H104.a12575873557 IN (
        |					'金华'
        |					,'丽水'
        |					,'台州'
        |					,'舟山'
        |					,'嘉兴'
        |					,'宁波'
        |					,'温州'
        |					,'绍兴'
        |					,'湖州'
        |					,'杭州'
        |					,'衢州'
        |					,'省直管'
        |					,'外省地市'
        |					,'测试'
        |					)
        |				AND D12575657700_H104.a12575903189 IN (
        |					'公共管理'
        |					,'卫生社保'
        |					,'电力供应'
        |					,'金融业'
        |					,'软件业'
        |					,'文体娱业'
        |					,'居民服务'
        |					,'科研技术'
        |					,'交运仓储'
        |					,'建筑业'
        |					,'租赁服务'
        |					,'制造业'
        |					,'住宿餐饮'
        |					,'公共服务'
        |					,'批发零售'
        |					,'农林牧渔'
        |					)
        |				AND D12575657700_H104.a12575847251 IN (
        |					'金'
        |					,'标准'
        |					,'银'
        |					,'铜'
        |					)
        |				AND DIM_1 IN (
        |					'1'
        |					,'2'
        |					,'5'
        |					)
        |				)
        |		GROUP BY STARTTIME
        |			,D12575657700_H104.a12575903189
        |			,DIM_52
        |			,D12575657700_H104.a12575873557
        |			,D12575657700_H104.a12575847251
        |		) MT
        |	GROUP BY MT.a3600
        |		,MT.a12575873557
        |		,MT.a12575847251
        |		,MT.a12575903189
        |		,MT.a204010101
        |	) AT
        |GROUP BY AT.a3600
        |	,AT.a12575873557
        |	,AT.a12575847251
        |	,AT.a12575903189
        |ORDER BY START_TIME ASC
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_2.`START_TIME`, gen_subquery_2.`USER_NUMBER`, gen_subquery_2.`CITY_ASCRIPTION`, gen_subquery_2.`SERVICE_LEVEL`, gen_subquery_2.`INDUSTRY` 
        |FROM
        |  (SELECT gen_subquery_1.`a3600` AS `START_TIME`, sum(CAST(gen_subquery_1.`CUSTER_IOT_GRP_USER_NUMBER_M` AS BIGINT)) AS `USER_NUMBER`, gen_subquery_1.`a12575873557` AS `CITY_ASCRIPTION`, gen_subquery_1.`a12575847251` AS `SERVICE_LEVEL`, gen_subquery_1.`a12575903189` AS `INDUSTRY` 
        |  FROM
        |    (SELECT gen_subquery_0.`a3600`, gen_subquery_0.`a12575873557` AS `a12575873557`, gen_subquery_0.`a12575847251` AS `a12575847251`, gen_subquery_0.`a12575903189` AS `a12575903189`, CASE WHEN (sum(coalesce(gen_subquery_0.`CUSTER_IOT_GRP_USER_NUM_STREAM_C`, 0L)) > 0L) THEN 1 ELSE 0 END AS `CUSTER_IOT_GRP_USER_NUMBER_M` 
        |    FROM
        |      (SELECT gen_subsumer_0.`a3600` AS `a3600`, sum(gen_subsumer_0.`CUSTER_IOT_GRP_USER_NUM_STREAM_C`) AS `CUSTER_IOT_GRP_USER_NUM_STREAM_C`, gen_subsumer_0.`a12575903189` AS `a12575903189`, gen_subsumer_0.`DIM_52` AS `a204010101`, gen_subsumer_0.`a12575873557` AS `a12575873557`, gen_subsumer_0.`a12575847251` AS `a12575847251` 
        |      FROM
        |        (SELECT dim_apn_iot.`a12575873557`, sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sum(1L) AS `CUSTER_IOT_GRP_USER_NUM_STREAM_C`, CAST(((FLOOR(((CAST(sdr_dyn_seq_custer_iot_all_hour_60min.`starttime` AS DOUBLE) + 28800.0D) / 3600.0D)) * 3600L) - 28800L) AS INT) AS `a3600`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`, dim_apn_iot.`a12575847251`, dim_apn_iot.`a12575903189` 
        |        FROM
        |          sdr_dyn_seq_custer_iot_all_hour_60min
        |          INNER JOIN (SELECT dim_apn_iot.`INDUSTRY` AS `a12575903189`, dim_apn_iot.`APN_NAME` AS `a12575817396`, dim_apn_iot.`CITY_ASCRIPTION` AS `a12575873557`, dim_apn_iot.`SERVICE_LEVEL` AS `a12575847251` 
        |          FROM
        |            dim_apn_iot
        |          WHERE
        |            (dim_apn_iot.`CITY_ASCRIPTION` IN ('金华', '丽水', '台州', '舟山', '嘉兴', '宁波', '温州', '绍兴', '湖州', '杭州', '衢州', '省直管', '外省地市', '测试')) AND (dim_apn_iot.`INDUSTRY` IN ('公共管理', '卫生社保', '电力供应', '金融业', '软件业', '文体娱业', '居民服务', '科研技术', '交运仓储', '建筑业', '租赁服务', '制造业', '住宿餐饮', '公共服务', '批发零售', '农林牧渔')) AND (dim_apn_iot.`SERVICE_LEVEL` IN ('金', '标准', '银', '铜'))
        |          GROUP BY dim_apn_iot.`INDUSTRY`, dim_apn_iot.`APN_NAME`, dim_apn_iot.`CITY_ASCRIPTION`, dim_apn_iot.`SERVICE_LEVEL`) dim_apn_iot  ON (sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51` = dim_apn_iot.`a12575817396`)
        |        GROUP BY dim_apn_iot.`a12575873557`, sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, dim_apn_iot.`a12575817396`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`, dim_apn_iot.`a12575847251`, dim_apn_iot.`a12575903189`) gen_subsumer_0 
        |      WHERE
        |        (gen_subsumer_0.`DIM_1` IN ('1', '2', '5'))
        |      GROUP BY gen_subsumer_0.`STARTTIME`, gen_subsumer_0.`a12575903189`, gen_subsumer_0.`DIM_52`, gen_subsumer_0.`a12575873557`, gen_subsumer_0.`a12575847251`) gen_subquery_0 
        |    GROUP BY gen_subquery_0.`a3600`, gen_subquery_0.`a12575873557`, gen_subquery_0.`a12575847251`, gen_subquery_0.`a12575903189`, gen_subquery_0.`a204010101`) gen_subquery_1 
        |  GROUP BY gen_subquery_1.`a3600`, gen_subquery_1.`a12575873557`, gen_subquery_1.`a12575847251`, gen_subquery_1.`a12575903189`) gen_subquery_2 
        |ORDER BY gen_subquery_2.`START_TIME` ASC NULLS FIRST 
       """.stripMargin.trim),
      // single table MV, latest benchmark
      ("case_32",
       """
        |SELECT tradeflow_all.`b_country`, tradeflow_all.`y_year`, substring(tradeflow_all.`hs_code`, 1, 2) AS `hs1`, sum(CASE WHEN (tradeflow_all.`y_year` = 2016) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2016`, sum(CASE WHEN (tradeflow_all.`y_year` = 2014) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2014`, sum(CASE WHEN (tradeflow_all.`y_year` = 2015) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2015`, tradeflow_all.`country`, tradeflow_all.`imex` 
        |FROM
        |  tradeflow_all
        |GROUP BY tradeflow_all.`b_country`, tradeflow_all.`y_year`, substring(tradeflow_all.`hs_code`, 1, 2), tradeflow_all.`country`, tradeflow_all.`imex`
       """.stripMargin.trim,
       """
        |SELECT *
        |FROM (
        |	SELECT DISTINCT country_show_cn
        |		,country
        |		,(
        |			CASE WHEN up.startdate <= '201401'
        |					AND up.newdate >= '201412' THEN CASE WHEN isnan(colunm_2014) THEN 0 ELSE colunm_2014 END ELSE NULL END
        |			) AS colunm_2014
        |		,(
        |			CASE WHEN up.startdate <= '201501'
        |					AND up.newdate >= '201512' THEN CASE WHEN isnan(colunm_2015) THEN 0 ELSE colunm_2015 END ELSE NULL END
        |			) AS colunm_2015
        |		,(
        |			CASE WHEN up.startdate <= '201601'
        |					AND up.newdate >= '201612' THEN CASE WHEN isnan(colunm_2016) THEN 0 ELSE colunm_2016 END ELSE NULL END
        |			) AS colunm_2016
        |		,tb
        |		,concat_ws('-', up.startdate, up.newdate) AS dbupdate
        |	FROM (
        |		SELECT a.country AS countryid
        |			,c.country_cn AS country_show_cn
        |			,c.country_en AS country
        |			,sum(v2014) AS colunm_2014
        |			,sum(v2015) AS colunm_2015
        |			,sum(v2016) AS colunm_2016
        |			,(sum(v2016) - sum(v2015)) / sum(v2015) AS tb
        |		FROM (
        |			SELECT b_country AS Country
        |				,sum(CASE WHEN y_year = 2014 THEN dollar_value ELSE 0 END) AS v2014
        |				,sum(CASE WHEN y_year = 2015 THEN dollar_value ELSE 0 END) AS v2015
        |				,sum(CASE WHEN y_year = 2016 THEN dollar_value ELSE 0 END) AS v2016
        |			FROM tradeflow_all
        |			WHERE imex = 0
        |				AND (
        |					y_year = 2014
        |					OR y_year = 2015
        |					OR y_year = 2016
        |					)
        |			GROUP BY b_country
        |				,y_year
        |			) a
        |		LEFT JOIN country c ON (a.country = c.countryid)
        |		GROUP BY country_show_cn
        |			,country
        |			,countryid
        |      ,country_en
        |		) w
        |	LEFT JOIN updatetime up ON (
        |			w.countryid = up.countryid
        |			AND imex = 0
        |			)
        |	WHERE !(isnan(colunm_2014)
        |			AND isnan(colunm_2015)
        |			AND isnan(colunm_2016))
        |		AND (
        |			colunm_2014 <> 0
        |			OR colunm_2015 <> 0
        |			OR colunm_2016 <> 0
        |			)
        |	) f
        |WHERE colunm_2014 IS NOT NULL
        |	OR colunm_2015 IS NOT NULL
        |	OR colunm_2016 IS NOT NULL
       """.stripMargin.trim,
       """
         |SELECT gen_subquery_1.`country_show_cn`, gen_subquery_1.`country`, gen_subquery_1.`colunm_2014`, gen_subquery_1.`colunm_2015`, gen_subquery_1.`colunm_2016`, gen_subquery_1.`tb`, gen_subquery_1.`dbupdate` 
         |FROM
         |  (SELECT w.`country_show_cn`, w.`country`, CASE WHEN ((up.`startdate` <= '201401') AND (up.`newdate` >= '201412')) THEN CASE WHEN isnan(w.`colunm_2014`) THEN 0.0D ELSE w.`colunm_2014` END ELSE CAST(NULL AS DOUBLE) END AS `colunm_2014`, CASE WHEN ((up.`startdate` <= '201501') AND (up.`newdate` >= '201512')) THEN CASE WHEN isnan(w.`colunm_2015`) THEN 0.0D ELSE w.`colunm_2015` END ELSE CAST(NULL AS DOUBLE) END AS `colunm_2015`, CASE WHEN ((up.`startdate` <= '201601') AND (up.`newdate` >= '201612')) THEN CASE WHEN isnan(w.`colunm_2016`) THEN 0.0D ELSE w.`colunm_2016` END ELSE CAST(NULL AS DOUBLE) END AS `colunm_2016`, w.`tb`, concat_ws('-', up.`startdate`, up.`newdate`) AS `dbupdate` 
         |  FROM
         |    (SELECT gen_subquery_0.`country` AS `countryid`, gen_subquery_0.`country_cn` AS `country_show_cn`, gen_subquery_0.`country_en` AS `country`, sum(gen_subquery_0.`v2014`) AS `colunm_2014`, sum(gen_subquery_0.`v2015`) AS `colunm_2015`, sum(gen_subquery_0.`v2016`) AS `colunm_2016`, ((sum(gen_subquery_0.`v2016`) - sum(gen_subquery_0.`v2015`)) / sum(gen_subquery_0.`v2015`)) AS `tb` 
         |    FROM
         |      (SELECT `Country`, `v2014`, `v2015`, `v2016`, `countryid`, `country_en`, `country_cn` 
         |      FROM
         |        (SELECT gen_subsumer_0.`b_country` AS `Country`, sum(gen_subsumer_0.`v2014`) AS `v2014`, sum(gen_subsumer_0.`v2015`) AS `v2015`, sum(gen_subsumer_0.`v2016`) AS `v2016` 
         |        FROM
         |          (SELECT tradeflow_all.`b_country`, tradeflow_all.`y_year`, substring(tradeflow_all.`hs_code`, 1, 2) AS `hs1`, sum(CASE WHEN (CAST(tradeflow_all.`y_year` AS INT) = 2016) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2016`, sum(CASE WHEN (CAST(tradeflow_all.`y_year` AS INT) = 2014) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2014`, sum(CASE WHEN (CAST(tradeflow_all.`y_year` AS INT) = 2015) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2015`, tradeflow_all.`country`, tradeflow_all.`imex` 
         |          FROM
         |            tradeflow_all
         |          GROUP BY tradeflow_all.`b_country`, tradeflow_all.`y_year`, substring(tradeflow_all.`hs_code`, 1, 2), tradeflow_all.`country`, tradeflow_all.`imex`) gen_subsumer_0 
         |        WHERE
         |          (CAST(gen_subsumer_0.`imex` AS INT) = 0) AND (((CAST(gen_subsumer_0.`y_year` AS INT) = 2014) OR (CAST(gen_subsumer_0.`y_year` AS INT) = 2015)) OR (CAST(gen_subsumer_0.`y_year` AS INT) = 2016))
         |        GROUP BY gen_subsumer_0.`b_country`, gen_subsumer_0.`y_year`) a 
         |        LEFT OUTER JOIN country c  ON (a.`country` = c.`countryid`)) gen_subquery_0 
         |    GROUP BY gen_subquery_0.`country_cn`, gen_subquery_0.`country`, gen_subquery_0.`countryid`, gen_subquery_0.`country_en`) w 
         |    LEFT OUTER JOIN updatetime up  ON (CAST(up.`imex` AS INT) = 0) AND (w.`countryid` = up.`countryid`) AND (((CASE WHEN ((up.`startdate` <= '201401') AND (up.`newdate` >= '201412')) THEN CASE WHEN isnan(w.`colunm_2014`) THEN 0.0D ELSE w.`colunm_2014` END ELSE CAST(NULL AS DOUBLE) END IS NOT NULL) OR (CASE WHEN ((up.`startdate` <= '201501') AND (up.`newdate` >= '201512')) THEN CASE WHEN isnan(w.`colunm_2015`) THEN 0.0D ELSE w.`colunm_2015` END ELSE CAST(NULL AS DOUBLE) END IS NOT NULL)) OR (CASE WHEN ((up.`startdate` <= '201601') AND (up.`newdate` >= '201612')) THEN CASE WHEN isnan(w.`colunm_2016`) THEN 0.0D ELSE w.`colunm_2016` END ELSE CAST(NULL AS DOUBLE) END IS NOT NULL))
         |  WHERE
         |    (((NOT isnan(w.`colunm_2014`)) OR (NOT isnan(w.`colunm_2015`))) OR (NOT isnan(w.`colunm_2016`))) AND (((NOT (w.`colunm_2014` = 0.0D)) OR (NOT (w.`colunm_2015` = 0.0D))) OR (NOT (w.`colunm_2016` = 0.0D)))) gen_subquery_1 
         |GROUP BY gen_subquery_1.`country_show_cn`, gen_subquery_1.`country`, gen_subquery_1.`colunm_2014`, gen_subquery_1.`colunm_2015`, gen_subquery_1.`colunm_2016`, gen_subquery_1.`tb`, gen_subquery_1.`dbupdate`
        """.stripMargin.trim),
      // single table MV, benchmark SEQ
      ("case_33",
       """
        |SELECT sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`, sum(1L) AS `sum(1)` 
        |FROM
        |  sdr_dyn_seq_custer_iot_all_hour_60min
        |GROUP BY sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`
       """.stripMargin.trim,
       """
        |SELECT AT.a3600 AS START_TIME
        |	,SUM(CUSTER_IOT_GRP_USER_NUMBER_M) AS USER_NUMBER
        |	,AT.a12575873557 AS CITY_ASCRIPTION
        |	,AT.a12575847251 AS SERVICE_LEVEL
        |	,AT.a12575903189 AS INDUSTRY
        |FROM (
        |	SELECT MT.a3600 AS a3600
        |		,MT.a12575873557 AS a12575873557
        |		,MT.a12575847251 AS a12575847251
        |		,MT.a12575903189 AS a12575903189
        |		,SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_CA
        |		,(
        |			CASE 
        |				WHEN (SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0))) > 0
        |					THEN 1
        |				ELSE 0
        |				END
        |			) AS CUSTER_IOT_GRP_USER_NUMBER_M
        |		,MT.a204010101 AS a204010101
        |	FROM (
        |		SELECT cast(floor((STARTTIME + 28800) / 3600) * 3600 - 28800 AS INT) AS a3600
        |			,SUM(COALESCE(1, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_C
        |			,D12575657700_H104.a12575903189 AS a12575903189
        |			,DIM_52 AS a204010101
        |			,D12575657700_H104.a12575873557 AS a12575873557
        |			,D12575657700_H104.a12575847251 AS a12575847251
        |		FROM SDR_DYN_SEQ_CUSTER_IOT_ALL_HOUR_60MIN
        |		LEFT JOIN (
        |			SELECT INDUSTRY AS a12575903189
        |				,APN_NAME AS a12575817396
        |				,CITY_ASCRIPTION AS a12575873557
        |				,SERVICE_LEVEL AS a12575847251
        |			FROM DIM_APN_IOT
        |			GROUP BY INDUSTRY
        |				,APN_NAME
        |				,CITY_ASCRIPTION
        |				,SERVICE_LEVEL
        |			) D12575657700_H104 ON DIM_51 = D12575657700_H104.a12575817396
        |		WHERE (
        |				D12575657700_H104.a12575873557 IN (
        |					'金华'
        |					,'丽水'
        |					,'台州'
        |					,'舟山'
        |					,'嘉兴'
        |					,'宁波'
        |					,'温州'
        |					,'绍兴'
        |					,'湖州'
        |					,'杭州'
        |					,'衢州'
        |					,'省直管'
        |					,'外省地市'
        |					,'测试'
        |					)
        |				AND D12575657700_H104.a12575903189 IN (
        |					'公共管理'
        |					,'卫生社保'
        |					,'电力供应'
        |					,'金融业'
        |					,'软件业'
        |					,'文体娱业'
        |					,'居民服务'
        |					,'科研技术'
        |					,'交运仓储'
        |					,'建筑业'
        |					,'租赁服务'
        |					,'制造业'
        |					,'住宿餐饮'
        |					,'公共服务'
        |					,'批发零售'
        |					,'农林牧渔'
        |					)
        |				AND D12575657700_H104.a12575847251 IN (
        |					'金'
        |					,'标准'
        |					,'银'
        |					,'铜'
        |					)
        |				AND DIM_1 IN (
        |					'1'
        |					,'2'
        |					,'5'
        |					)
        |				)
        |		GROUP BY STARTTIME
        |			,D12575657700_H104.a12575903189
        |			,DIM_52
        |			,D12575657700_H104.a12575873557
        |			,D12575657700_H104.a12575847251
        |		) MT
        |	GROUP BY MT.a3600
        |		,MT.a12575873557
        |		,MT.a12575847251
        |		,MT.a12575903189
        |		,MT.a204010101
        |	) AT
        |GROUP BY AT.a3600
        |	,AT.a12575873557
        |	,AT.a12575847251
        |	,AT.a12575903189
        |ORDER BY START_TIME ASC
       """.stripMargin.trim,
       """
        |SELECT gen_subquery_3.`START_TIME`, gen_subquery_3.`USER_NUMBER`, gen_subquery_3.`CITY_ASCRIPTION`, gen_subquery_3.`SERVICE_LEVEL`, gen_subquery_3.`INDUSTRY` 
        |FROM
        |  (SELECT gen_subquery_2.`a3600` AS `START_TIME`, sum(CAST(gen_subquery_2.`CUSTER_IOT_GRP_USER_NUMBER_M` AS BIGINT)) AS `USER_NUMBER`, gen_subquery_2.`a12575873557` AS `CITY_ASCRIPTION`, gen_subquery_2.`a12575847251` AS `SERVICE_LEVEL`, gen_subquery_2.`a12575903189` AS `INDUSTRY` 
        |  FROM
        |    (SELECT gen_subquery_1.`a3600`, gen_subquery_1.`a12575873557` AS `a12575873557`, gen_subquery_1.`a12575847251` AS `a12575847251`, gen_subquery_1.`a12575903189` AS `a12575903189`, CASE WHEN (sum(coalesce(gen_subquery_1.`CUSTER_IOT_GRP_USER_NUM_STREAM_C`, 0L)) > 0L) THEN 1 ELSE 0 END AS `CUSTER_IOT_GRP_USER_NUMBER_M` 
        |    FROM
        |      (SELECT CAST(((FLOOR(((CAST(gen_subquery_0.`STARTTIME` AS DOUBLE) + 28800.0D) / 3600.0D)) * 3600L) - 28800L) AS INT) AS `a3600`, sum(gen_subquery_0.`sum(1)`) AS `CUSTER_IOT_GRP_USER_NUM_STREAM_C`, gen_subquery_0.`a12575903189` AS `a12575903189`, gen_subquery_0.`DIM_52` AS `a204010101`, gen_subquery_0.`a12575873557` AS `a12575873557`, gen_subquery_0.`a12575847251` AS `a12575847251` 
        |      FROM
        |        (SELECT `starttime`, `dim_52`, `a12575903189`, `a12575873557`, `a12575847251`, gen_harmonized_default_sdr_dyn_seq_custer_iot_all_hour_60min.`sum(1)` 
        |        FROM
        |          (SELECT gen_subsumer_0.`DIM_51`, gen_subsumer_0.`DIM_1`, gen_subsumer_0.`STARTTIME`, gen_subsumer_0.`DIM_52`, sum(gen_subsumer_0.`sum(1)`) AS `sum(1)` 
        |          FROM
        |            (SELECT sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`, sum(1L) AS `sum(1)` 
        |            FROM
        |              sdr_dyn_seq_custer_iot_all_hour_60min
        |            GROUP BY sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`) gen_subsumer_0 
        |          GROUP BY gen_subsumer_0.`DIM_51`, gen_subsumer_0.`DIM_1`, gen_subsumer_0.`STARTTIME`, gen_subsumer_0.`DIM_52`) gen_harmonized_default_sdr_dyn_seq_custer_iot_all_hour_60min 
        |          INNER JOIN (SELECT dim_apn_iot.`INDUSTRY` AS `a12575903189`, dim_apn_iot.`APN_NAME` AS `a12575817396`, dim_apn_iot.`CITY_ASCRIPTION` AS `a12575873557`, dim_apn_iot.`SERVICE_LEVEL` AS `a12575847251` 
        |          FROM
        |            dim_apn_iot
        |          GROUP BY dim_apn_iot.`INDUSTRY`, dim_apn_iot.`APN_NAME`, dim_apn_iot.`CITY_ASCRIPTION`, dim_apn_iot.`SERVICE_LEVEL`) D12575657700_H104  ON (sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_51` = D12575657700_H104.`a12575817396`) AND (`a12575873557` IN ('金华', '丽水', '台州', '舟山', '嘉兴', '宁波', '温州', '绍兴', '湖州', '杭州', '衢州', '省直管', '外省地市', '测试')) AND (`a12575903189` IN ('公共管理', '卫生社保', '电力供应', '金融业', '软件业', '文体娱业', '居民服务', '科研技术', '交运仓储', '建筑业', '租赁服务', '制造业', '住宿餐饮', '公共服务', '批发零售', '农林牧渔')) AND (`a12575847251` IN ('金', '标准', '银', '铜'))
        |        WHERE
        |          (sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_1` IN ('1', '2', '5'))) gen_subquery_0 
        |      GROUP BY gen_subquery_0.`STARTTIME`, gen_subquery_0.`a12575903189`, gen_subquery_0.`DIM_52`, gen_subquery_0.`a12575873557`, gen_subquery_0.`a12575847251`) gen_subquery_1 
        |    GROUP BY gen_subquery_1.`a3600`, gen_subquery_1.`a12575873557`, gen_subquery_1.`a12575847251`, gen_subquery_1.`a12575903189`, gen_subquery_1.`a204010101`) gen_subquery_2 
        |  GROUP BY gen_subquery_2.`a3600`, gen_subquery_2.`a12575873557`, gen_subquery_2.`a12575847251`, gen_subquery_2.`a12575903189`) gen_subquery_3 
        |ORDER BY gen_subquery_3.`START_TIME` ASC NULLS FIRST
       """.stripMargin.trim
        )
  )
  
}