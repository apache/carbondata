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

package org.apache.carbondata.mv.tool.constructing

object TestTPCDS_1_4_MVBatch {
  val tpcds_1_4_testCases = Seq(
         ("case_1",
          Seq(       
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
               |  select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
               |  from store_sales, date_dim, item
               |  where ss_sold_date_sk = d_date_sk
               |    and ss_item_sk = i_item_sk
               |    and d_year in (2000, 2000+1, 2000+2,2000+3)
               |  group by substr(i_item_desc,1,30),i_item_sk,d_date
               |  having count(*) >4
              """.stripMargin.trim,
//              """
//               |     select 'store' channel, i_brand_id,i_class_id
//               |             ,i_category_id,sum(ss_quantity*ss_list_price) sales
//               |             , count(*) number_sales
//               |     from store_sales, item, date_dim
//               |     where ss_item_sk in (select ss_item_sk from cross_items)
//               |       and ss_item_sk = i_item_sk
//               |       and ss_sold_date_sk = d_date_sk
//               |       and d_year = 1999+2
//               |       and d_moy = 11
//               |     group by i_brand_id,i_class_id,i_category_id
//               |     having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
//              """.stripMargin.trim,
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
//              """
//               |SELECT first(gen_subquery_1.channel), gen_subquery_1.col_name, gen_subquery_1.d_year, gen_subquery_1.d_qoy, gen_subquery_1.i_category, SUM(gen_subquery_1.ss_ext_sales_price) sales_amt
//               |FROM
//               | (SELECT
//               |      'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
//               |      ss_ext_sales_price
//               |  FROM store_sales, item, date_dim
//               |  WHERE ss_store_sk IS NULL
//               |    AND ss_sold_date_sk=d_date_sk
//               |    AND ss_item_sk=i_item_sk) gen_subquery_1
//               | GROUP BY col_name, d_year, d_qoy, i_category
//               | ORDER BY col_name, d_year, d_qoy, i_category
//               | limit 100
//              """.stripMargin.trim,
              """
               |    SELECT
               |        'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
               |        SUM(ss_ext_sales_price) ext_sales_price
               |    FROM store_sales, item, date_dim
               |    WHERE ss_store_sk IS NULL
               |      AND ss_sold_date_sk=d_date_sk
               |      AND ss_item_sk=i_item_sk   
               |    GROUP BY ss_store_sk, d_year, d_qoy, i_category
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
              """.stripMargin.trim
          ),
          Seq(
              """
               |SELECT item.`i_brand`, store_sales.`ss_store_sk`, date_dim.`d_year`, item.`i_category`, date_dim.`d_qoy`, date_dim.`d_moy`, date_dim.`d_date`, item.`i_class`, item.`i_item_id`, item.`i_manufact_id`, item.`i_manager_id`, item.`i_item_desc`, item.`i_item_sk`, item.`i_current_price`, sum(store_sales.`ss_ext_sales_price`) AS `ext_sales_price`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, item.`i_brand_id`, count(1) AS `cnt` 
               |FROM
               |  store_sales
               |  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
               |  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
               |GROUP BY item.`i_brand`, store_sales.`ss_store_sk`, date_dim.`d_year`, item.`i_category`, date_dim.`d_qoy`, date_dim.`d_moy`, date_dim.`d_date`, item.`i_class`, item.`i_item_id`, item.`i_manufact_id`, item.`i_manager_id`, item.`i_item_desc`, item.`i_item_sk`, item.`i_current_price`, substring(item.`i_item_desc`, 1, 30), item.`i_brand_id`
              """.stripMargin.trim
          )
         ),
      // for each case, the first entry is MV, which is constructed from the rest SQLs in the case.
      // q3
         ("case_2",
          Seq(       
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
              """.stripMargin.trim
          ),
          Seq(
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
              """.stripMargin.trim
          )
         ),
         ("case_3",
          Seq(       
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
               |  select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
               |  from store_sales, date_dim, item
               |  where ss_sold_date_sk = d_date_sk
               |    and ss_item_sk = i_item_sk
               |    and d_year in (2000, 2000+1, 2000+2,2000+3)
               |  group by substr(i_item_desc,1,30),i_item_sk,d_date
               |  having count(*) >4
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
               |    SELECT
               |        'store' as channel, ss_store_sk col_name, d_year, d_qoy, i_category,
               |        SUM(ss_ext_sales_price) ext_sales_price
               |    FROM store_sales, item, date_dim
               |    WHERE ss_store_sk IS NULL
               |      AND ss_sold_date_sk=d_date_sk
               |      AND ss_item_sk=i_item_sk   
               |    GROUP BY ss_store_sk, d_year, d_qoy, i_category
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
              """.stripMargin.trim,
              """
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
              """.stripMargin.trim
          ),
          Seq(
              """
               |SELECT item.`i_brand`, store_sales.`ss_store_sk`, date_dim.`d_year`, item.`i_category`, date_dim.`d_qoy`, date_dim.`d_moy`, date_dim.`d_date`, item.`i_class`, item.`i_item_id`, item.`i_manufact_id`, item.`i_manager_id`, item.`i_item_desc`, item.`i_item_sk`, item.`i_current_price`, sum(store_sales.`ss_ext_sales_price`) AS `ext_sales_price`, substring(item.`i_item_desc`, 1, 30) AS `itemdesc`, item.`i_brand_id`, count(1) AS `cnt` 
               |FROM
               |  store_sales
               |  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)
               |  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)
               |GROUP BY item.`i_brand`, store_sales.`ss_store_sk`, date_dim.`d_year`, item.`i_category`, date_dim.`d_qoy`, date_dim.`d_moy`, date_dim.`d_date`, item.`i_class`, item.`i_item_id`, item.`i_manufact_id`, item.`i_manager_id`, item.`i_item_desc`, item.`i_item_sk`, item.`i_current_price`, substring(item.`i_item_desc`, 1, 30), item.`i_brand_id`
              """.stripMargin.trim
          )
         ),
         ("case_4",
          Seq(       
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
               |		,(CASE WHEN (SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0))) > 0 THEN 1 ELSE 0 END) AS CUSTER_IOT_GRP_USER_NUMBER_M
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
               |				STARTTIME >= 1482854400
               |				AND STARTTIME < 1482940800
               |				AND STARTTIME >= 1482854400
               |				AND STARTTIME < 1482940800
               |        AND DIM_10 IS NULL
               |				AND D12575657700_H104.a12575873557 IN (
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
               |					1
               |					,2
               |					,5
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
              """.stripMargin.trim
          ),
          Seq(
              """
               |SELECT sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`, sum(1L) AS `sum(1)` 
               |FROM
               |  sdr_dyn_seq_custer_iot_all_hour_60min
               |GROUP BY sdr_dyn_seq_custer_iot_all_hour_60min.`starttime`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_51`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_52`, sdr_dyn_seq_custer_iot_all_hour_60min.`DIM_10`, sdr_dyn_seq_custer_iot_all_hour_60min.`dim_1`
              """.stripMargin.trim
          )
        ),
        ("case_5",
          Seq(       
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
               |SELECT *
               |FROM (
               |	SELECT DISTINCT Partner_cn
               |		,Partner
               |		,(CASE WHEN isnan(colunm_2014) THEN 0 ELSE colunm_2014 END) AS colunm_2014
               |		,(CASE WHEN isnan(colunm_2015) THEN 0 ELSE colunm_2015 END) AS colunm_2015
               |		,(CASE WHEN isnan(colunm_2016) THEN 0 ELSE colunm_2016 END) AS colunm_2016
               |		,tb
               |	FROM (
               |		SELECT cp.country_cn AS Partner_cn
               |			,cp.country_en AS Partner
               |			,sum(v2014) AS colunm_2014
               |			,sum(v2015) AS colunm_2015
               |			,sum(v2016) AS colunm_2016
               |			,(sum(v2016) - sum(v2015)) / sum(v2015) AS tb
               |		FROM (
               |			SELECT country AS Partner
               |				,sum(CASE WHEN y_year = 2014 THEN dollar_value ELSE 0 END) AS v2014
               |				,sum(CASE WHEN y_year = 2015 THEN dollar_value ELSE 0 END) AS v2015
               |				,sum(CASE WHEN y_year = 2016 THEN dollar_value ELSE 0 END) AS v2016
               |			FROM tradeflow_all
               |			WHERE imex = 0
               |				AND b_country = 110
               |				AND (
               |					y_year = 2014
               |					OR y_year = 2015
               |					OR y_year = 2016
               |					)
               |			GROUP BY country
               |				,y_year
               |			) a
               |		LEFT JOIN country_general cp ON (a.Partner = cp.countryid)
               |		GROUP BY partner
               |			,partner_cn
               |      ,country_en
               |		) w
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
               |SELECT *
               |FROM (
               |	SELECT DISTINCT hs1 as hs
               |		,hs_cn
               |		,hs_en
               |		,(CASE WHEN isnan(colunm_2014) THEN 0 ELSE colunm_2014 END) AS colunm_2014
               |		,(CASE WHEN isnan(colunm_2015) THEN 0 ELSE colunm_2015 END) AS colunm_2015
               |		,(CASE WHEN isnan(colunm_2016) THEN 0 ELSE colunm_2016 END) AS colunm_2016
               |		,tb
               |	FROM (
               |		SELECT a.hs1
               |			,h.hs_cn
               |			,h.hs_en
               |			,sum(v2014) AS colunm_2014
               |			,sum(v2015) AS colunm_2015
               |			,sum(v2016) AS colunm_2016
               |			,(sum(v2016) - sum(v2015)) / sum(v2015) AS tb
               |		FROM (
               |			SELECT substring(hs_code, 1, 2) AS hs1
               |				,sum(CASE WHEN y_year = 2014 THEN dollar_value ELSE 0 END) AS v2014
               |				,sum(CASE WHEN y_year = 2015 THEN dollar_value ELSE 0 END) AS v2015
               |				,sum(CASE WHEN y_year = 2016 THEN dollar_value ELSE 0 END) AS v2016
               |			FROM tradeflow_all
               |			WHERE imex = 0
               |				AND country = 194
               |				AND b_country = 110
               |				AND (
               |					y_year = 2014
               |					OR y_year = 2015
               |					OR y_year = 2016
               |					)
               |			GROUP BY substring(hs_code, 1, 2)
               |				,y_year
               |			) a
               |		LEFT JOIN hs246 h ON (a.hs1 = h.hs)
               |		GROUP BY hs1
               |			,hs_cn
               |			,hs_en
               |		) w
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
              """.stripMargin.trim
          ),
          Seq(
              """
               |SELECT sum(CASE WHEN (tradeflow_all.`y_year` = 2014) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2014`, sum(CASE WHEN (tradeflow_all.`y_year` = 2015) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2015`, substring(tradeflow_all.`hs_code`, 1, 2) AS `hs1`, tradeflow_all.`imex`, tradeflow_all.`country`, tradeflow_all.`b_country`, tradeflow_all.`y_year`, sum(CASE WHEN (tradeflow_all.`y_year` = 2016) THEN tradeflow_all.`dollar_value` ELSE 0.0D END) AS `v2016` 
               |FROM
               |  tradeflow_all
               |GROUP BY substring(tradeflow_all.`hs_code`, 1, 2), tradeflow_all.`imex`, tradeflow_all.`country`, tradeflow_all.`b_country`, tradeflow_all.`y_year`
              """.stripMargin.trim
          )
        ),
        // there is no data for case_6 (SmartCare)
         ("case_6",
          Seq(       
              """
               |SELECT *
               |FROM (
               |	SELECT 10 totalCount
               |		,rSDR.*
               |		,RANK() OVER (
               |			ORDER BY EXPERDROPRATIO DESC
               |				,CONACKCOUNT_SUM DESC
               |				,UTCSTTIME
               |				,areaId
               |				,accessType
               |			) ROWIDX
               |	FROM (
               |		SELECT sdrALL.*
               |			,CASE WHEN (
               |						CASE WHEN (sdrALL.accessTypeId = 1)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END
               |						) = '-99999'
               |					OR (
               |						CASE WHEN (sdrALL.accessTypeId = 1)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END
               |						) = '-99999.00' THEN '--' ELSE CASE WHEN (sdrALL.accessTypeId = 1)
               |							OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END END RABDROPRATIO
               |		FROM (
               |			SELECT sdrALL.*
               |				,CASE WHEN (
               |							CASE WHEN (sdrALL.accessTypeId = 0)
               |									OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END
               |							) = '-99999'
               |						OR (
               |							CASE WHEN (sdrALL.accessTypeId = 0)
               |									OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END
               |							) = '-99999.00' THEN '--' ELSE CASE WHEN (sdrALL.accessTypeId = 0)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END END TCHDROPRATIO
               |			FROM (
               |				SELECT SDR.STARTTIME UTCSTTIME
               |					,DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |						,'yyyy-mm-dd HH:mm'
               |						) || ' ~ ' || DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |						,'HH:mm'
               |						) STTIME
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |					,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |					,5 areaType
               |					,SDR.ACCESS_TYPE accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,SUM(CONACKRADIODROPCOUNT) CONACKRADIODROPCOUNT_SUM
               |					,SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) CONACKCOUNT_SUM
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST((CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2))) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END E2ECONDROPRATIO
               |					,CASE WHEN SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) = 0 THEN - 99999 ELSE CAST((CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2))) / SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) * 100 AS DECIMAL(20, 2)) END EXPERDROPRATIO
               |					,CASE WHEN SUM(ASSCMPCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPNOHOCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT) * 100 AS DECIMAL(20, 2)) END TCHDROPNOHORATIO
               |					,CASE WHEN SUM(HOCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOCOUNT) * 100 AS DECIMAL(20, 2)) END HOSUCRATE
               |					,CASE WHEN SUM(HOINCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOINSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOINCOUNT) * 100 AS DECIMAL(20, 2)) END HOINSUCRATE
               |					,CASE WHEN SUM(HOOUTCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOOUTSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOOUTCOUNT) * 100 AS DECIMAL(20, 2)) END HOOUTSUCRATE
               |					,CASE WHEN SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) * 100 AS DECIMAL(20, 2)) END CONNEACKRADIODROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKCNDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKCNDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKDSCDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKDSCDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKCLRCMDDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKCLRCMDDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKRELDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKRELDROPRATIO
               |				FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |				LEFT JOIN (
               |					SELECT BSCRNC_ID
               |						,BSCRNC_NAME
               |					FROM CS.DIM_LOC_BSCRNC
               |					GROUP BY BSCRNC_ID
               |						,BSCRNC_NAME
               |					) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |				LEFT JOIN (
               |					SELECT HRegionID
               |						,HRegionType
               |					FROM nethouse.CFG_REGION DIM
               |					GROUP BY HRegionID
               |						,HRegionType
               |					) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |					AND ss.HRegionType = areaType
               |				WHERE STARTTIME >= 1512403200
               |					AND STARTTIME < 1512404100
               |					AND SDR.ACCESS_TYPE <> 9
               |				GROUP BY UTCSTTIME
               |					,STTIME
               |					,areaName
               |					,areaId
               |					,accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,accessType
               |				) sdrALL
               |			LEFT JOIN (
               |				SELECT SDR.STARTTIME UTCSTTIME
               |					,DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |						,'yyyy-mm-dd HH:mm'
               |						) || ' ~ ' || DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |						,'HH:mm'
               |						) STTIME
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |					,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |					,5 areaType
               |					,SDR.ACCESS_TYPE accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,CASE WHEN SUM(ASSCMPCOUNT + HOSUCCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT + HOSUCCOUNT) * 100 AS DECIMAL(20, 2)) END TCHDROPRATIO
               |				FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |				LEFT JOIN (
               |					SELECT BSCRNC_ID
               |						,BSCRNC_NAME
               |					FROM CS.DIM_LOC_BSCRNC
               |					GROUP BY BSCRNC_ID
               |						,BSCRNC_NAME
               |					) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |				LEFT JOIN (
               |					SELECT HRegionID
               |						,HRegionType
               |					FROM nethouse.CFG_REGION DIM
               |					GROUP BY HRegionID
               |						,HRegionType
               |					) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |					AND ss.HRegionType = areaType
               |				WHERE STARTTIME >= 1512403200
               |					AND STARTTIME < 1512404100
               |					AND SDR.ACCESS_TYPE = 0
               |				GROUP BY UTCSTTIME
               |					,STTIME
               |					,areaName
               |					,areaId
               |					,accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,accessType
               |				) sdr2G ON sdrALL.UTCSTTIME = sdr2G.UTCSTTIME
               |				AND sdrALL.areaId = sdr2G.areaId
               |			) sdrALL
               |		LEFT JOIN (
               |			SELECT SDR.STARTTIME UTCSTTIME
               |				,DATEFORMAT (
               |					dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |					,'yyyy-mm-dd HH:mm'
               |					) || ' ~ ' || DATEFORMAT (
               |					dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |					,'HH:mm'
               |					) STTIME
               |				,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |				,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |				,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |				,5 areaType
               |				,SDR.ACCESS_TYPE accessTypeId
               |				,ss.HRegionID
               |				,ss.HRegionType
               |				,CASE WHEN SUM(ASSCMPCOUNT + HOSUCCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT + HOSUCCOUNT) * 100 AS DECIMAL(20, 2)) END RABDROPRATIO
               |			FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |			LEFT JOIN (
               |				SELECT BSCRNC_ID
               |					,BSCRNC_NAME
               |				FROM CS.DIM_LOC_BSCRNC
               |				GROUP BY BSCRNC_ID
               |					,BSCRNC_NAME
               |				) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |			LEFT JOIN (
               |				SELECT HRegionID
               |					,HRegionType
               |				FROM nethouse.CFG_REGION DIM
               |				GROUP BY HRegionID
               |					,HRegionType
               |				) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |				AND ss.HRegionType = areaType
               |			WHERE STARTTIME >= 1512403200
               |				AND STARTTIME < 1512404100
               |				AND SDR.ACCESS_TYPE = 1
               |			GROUP BY UTCSTTIME
               |				,STTIME
               |				,areaName
               |				,areaId
               |				,accessTypeId
               |				,ss.HRegionID
               |				,ss.HRegionType
               |				,accessType
               |			) sdr3G ON sdrALL.UTCSTTIME = sdr3G.UTCSTTIME
               |			AND sdrALL.areaId = sdr3G.areaId
               |		) rSDR
               |	WHERE AREAID IS NOT NULL
               |	
               |	UNION ALL
               |	
               |	SELECT 10 totalCount
               |		,rSDR.*
               |		,10 ROWIDX
               |	FROM (
               |		SELECT sdrALL.*
               |			,CASE WHEN (
               |						CASE WHEN (sdrALL.accessTypeId = 1)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END
               |						) = '-99999'
               |					OR (
               |						CASE WHEN (sdrALL.accessTypeId = 1)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END
               |						) = '-99999.00' THEN '--' ELSE CASE WHEN (sdrALL.accessTypeId = 1)
               |							OR (sdrALL.accessTypeId = 255) THEN '' || isnull(RABDROPRATIO, - 99999) || '' ELSE '--' END END RABDROPRATIO
               |		FROM (
               |			SELECT sdrALL.*
               |				,CASE WHEN (
               |							CASE WHEN (sdrALL.accessTypeId = 0)
               |									OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END
               |							) = '-99999'
               |						OR (
               |							CASE WHEN (sdrALL.accessTypeId = 0)
               |									OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END
               |							) = '-99999.00' THEN '--' ELSE CASE WHEN (sdrALL.accessTypeId = 0)
               |								OR (sdrALL.accessTypeId = 255) THEN '' || isnull(TCHDROPRATIO, - 99999) || '' ELSE '--' END END TCHDROPRATIO
               |			FROM (
               |				SELECT SDR.STARTTIME UTCSTTIME
               |					,DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |						,'yyyy-mm-dd HH:mm'
               |						) || ' ~ ' || DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |						,'HH:mm'
               |						) STTIME
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |					,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |					,5 areaType
               |					,SDR.ACCESS_TYPE accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,SUM(CONACKRADIODROPCOUNT) CONACKRADIODROPCOUNT_SUM
               |					,SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) CONACKCOUNT_SUM
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST((CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2))) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END E2ECONDROPRATIO
               |					,CASE WHEN SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) = 0 THEN - 99999 ELSE CAST((CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2))) / SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) * 100 AS DECIMAL(20, 2)) END EXPERDROPRATIO
               |					,CASE WHEN SUM(ASSCMPCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPNOHOCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT) * 100 AS DECIMAL(20, 2)) END TCHDROPNOHORATIO
               |					,CASE WHEN SUM(HOCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOCOUNT) * 100 AS DECIMAL(20, 2)) END HOSUCRATE
               |					,CASE WHEN SUM(HOINCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOINSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOINCOUNT) * 100 AS DECIMAL(20, 2)) END HOINSUCRATE
               |					,CASE WHEN SUM(HOOUTCOUNT) = 0 THEN 99999 ELSE CAST(CAST(SUM(HOOUTSUCCOUNT) AS DECIMAL(20, 2)) / SUM(HOOUTCOUNT) * 100 AS DECIMAL(20, 2)) END HOOUTSUCRATE
               |					,CASE WHEN SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKRADIODROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKRADIODROPCOUNT + MOTCSIRSPDELAYBEYONDCOUNT) * 100 AS DECIMAL(20, 2)) END CONNEACKRADIODROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKCNDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKCNDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKDSCDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKDSCDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKCLRCMDDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKCLRCMDDROPRATIO
               |					,CASE WHEN SUM(CONACKCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(CONACKRELDROPCOUNT) AS DECIMAL(20, 2)) / SUM(CONACKCOUNT) * 100 AS DECIMAL(20, 2)) END CONACKRELDROPRATIO
               |				FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |				LEFT JOIN (
               |					SELECT BSCRNC_ID
               |						,BSCRNC_NAME
               |					FROM CS.DIM_LOC_BSCRNC
               |					GROUP BY BSCRNC_ID
               |						,BSCRNC_NAME
               |					) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |				LEFT JOIN (
               |					SELECT HRegionID
               |						,HRegionType
               |					FROM nethouse.CFG_REGION DIM
               |					GROUP BY HRegionID
               |						,HRegionType
               |					) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |					AND ss.HRegionType = areaType
               |				WHERE STARTTIME >= 1512403200
               |					AND STARTTIME < 1512404100
               |					AND SDR.ACCESS_TYPE <> 9
               |				GROUP BY UTCSTTIME
               |					,STTIME
               |					,areaName
               |					,areaId
               |					,accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,accessType
               |				) sdrALL
               |			LEFT JOIN (
               |				SELECT SDR.STARTTIME UTCSTTIME
               |					,DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |						,'yyyy-mm-dd HH:mm'
               |						) || ' ~ ' || DATEFORMAT (
               |						dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |						,'HH:mm'
               |						) STTIME
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |					,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |					,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |					,5 areaType
               |					,SDR.ACCESS_TYPE accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,CASE WHEN SUM(ASSCMPCOUNT + HOSUCCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT + HOSUCCOUNT) * 100 AS DECIMAL(20, 2)) END TCHDROPRATIO
               |				FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |				LEFT JOIN (
               |					SELECT BSCRNC_ID
               |						,BSCRNC_NAME
               |					FROM CS.DIM_LOC_BSCRNC
               |					GROUP BY BSCRNC_ID
               |						,BSCRNC_NAME
               |					) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |				LEFT JOIN (
               |					SELECT HRegionID
               |						,HRegionType
               |					FROM nethouse.CFG_REGION DIM
               |					GROUP BY HRegionID
               |						,HRegionType
               |					) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |					AND ss.HRegionType = areaType
               |				WHERE STARTTIME >= 1512403200
               |					AND STARTTIME < 1512404100
               |					AND SDR.ACCESS_TYPE = 0
               |				GROUP BY UTCSTTIME
               |					,STTIME
               |					,areaName
               |					,areaId
               |					,accessTypeId
               |					,ss.HRegionID
               |					,ss.HRegionType
               |					,accessType
               |				) sdr2G ON sdrALL.UTCSTTIME = sdr2G.UTCSTTIME
               |				AND sdrALL.areaId = sdr2G.areaId
               |			) sdrALL
               |		LEFT JOIN (
               |			SELECT SDR.STARTTIME UTCSTTIME
               |				,DATEFORMAT (
               |					dateadd(ss, UTCSTTIME + - 18000, '19700101')
               |					,'yyyy-mm-dd HH:mm'
               |					) || ' ~ ' || DATEFORMAT (
               |					dateadd(ss, UTCSTTIME + - 17100, '19700101')
               |					,'HH:mm'
               |					) STTIME
               |				,CASE WHEN SDR.BSCRNC_ID = NULL THEN 'UnKnown' WHEN DIM.BSCRNC_ID = NULL THEN '' || SDR.BSCRNC_ID || '' ELSE DIM.BSCRNC_NAME END areaName
               |				,CASE WHEN SDR.BSCRNC_ID = NULL THEN NULL WHEN DIM.BSCRNC_ID = NULL THEN SDR.BSCRNC_ID ELSE DIM.BSCRNC_ID END areaId
               |				,CASE WHEN SDR.ACCESS_TYPE = 0 THEN '2G' ELSE '3G' END accessType
               |				,5 areaType
               |				,SDR.ACCESS_TYPE accessTypeId
               |				,ss.HRegionID
               |				,ss.HRegionType
               |				,CASE WHEN SUM(ASSCMPCOUNT + HOSUCCOUNT) = 0 THEN - 99999 ELSE CAST(CAST(SUM(TCHDROPCOUNT) AS DECIMAL(20, 2)) / SUM(ASSCMPCOUNT + HOSUCCOUNT) * 100 AS DECIMAL(20, 2)) END RABDROPRATIO
               |			FROM CS.SDR_VOICE_BSC_15MIN_575 SDR
               |			LEFT JOIN (
               |				SELECT BSCRNC_ID
               |					,BSCRNC_NAME
               |				FROM CS.DIM_LOC_BSCRNC
               |				GROUP BY BSCRNC_ID
               |					,BSCRNC_NAME
               |				) DIM ON DIM.BSCRNC_ID = SDR.BSCRNC_ID
               |			LEFT JOIN (
               |				SELECT HRegionID
               |					,HRegionType
               |				FROM nethouse.CFG_REGION DIM
               |				GROUP BY HRegionID
               |					,HRegionType
               |				) ss ON '' || ss.HRegionID || '' = '' || areaId || ''
               |				AND ss.HRegionType = areaType
               |			WHERE STARTTIME >= 1512403200
               |				AND STARTTIME < 1512404100
               |				AND SDR.ACCESS_TYPE = 1
               |			GROUP BY UTCSTTIME
               |				,STTIME
               |				,areaName
               |				,areaId
               |				,accessTypeId
               |				,ss.HRegionID
               |				,ss.HRegionType
               |				,accessType
               |			) sdr3G ON sdrALL.UTCSTTIME = sdr3G.UTCSTTIME
               |			AND sdrALL.areaId = sdr3G.areaId
               |		) rSDR
               |	WHERE AREAID IS NULL
               |	) ordSDR
               |WHERE ROWIDX >= 1
               |	AND ROWIDX <= 20
              """.stripMargin.trim
          ),
          Seq(
              """
               |
              """.stripMargin.trim
          )
         )
   )
}

  
