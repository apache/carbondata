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

package org.apache.carbondata.mv

object TestSQLBatch2 {

  val testSQLBatch2 = Seq[String](
      s"""
         |SELECT f1.A,COUNT(*) AS B 
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |WHERE f1.E IS NULL AND (f1.C > d1.E OR d1.E = 3)
         |GROUP BY f1.A
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,COUNT(*) AS B 
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |  JOIN dim1 d2 ON (f1.K = d2.K AND d2.G > 0)
         |WHERE f1.E IS NULL AND f1.C > d1.E
         |GROUP BY f1.A
      """.stripMargin.trim,
      s"""
         |SELECT substr(item.i_item_desc,1,30) itemdesc, item.i_item_sk item_sk, date_dim.d_date solddate, count(*) cnt
         |FROM date_dim, store_sales, item
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND date_dim.d_year in (2000, 2000+1, 2000+2, 2000+3)
         |GROUP BY substr(item.i_item_desc,1,30), item.i_item_sk,date_dim.d_date 
      """.stripMargin.trim,
      s"""
         |SELECT item.i_item_desc, item.i_category, item.i_class, item.i_current_price, 
         |       SUM(store_sales.ss_ext_sales_price) as itemrevenue,
         |       SUM(store_sales.ss_ext_sales_price)*100/sum(sum(store_sales.ss_ext_sales_price)) over (partition by item.i_class) as revenueratio
         |FROM date_dim, store_sales, item
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND item.i_category in ('Sport', 'Books', 'Home')
         |      AND date_dim.d_date between cast('1999-02-22' as date) AND (cast('1999-02-22' as date) + interval 30 days)
         |GROUP BY item.i_item_id, item.i_item_desc, item.i_category, item.i_class, item.i_current_price 
      """.stripMargin.trim,
      s"""
         |SELECT 'store' channel, store_sales.ss_store_sk col_name, date_dim.d_year, date_dim.d_qoy, 
         |       item.i_category, SUM(store_sales.ss_ext_sales_price) ext_sales_price 
         |FROM date_dim, store_sales, item
         |WHERE store_sales.ss_store_sk IS NULL
         |      AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |GROUP BY channel, store_sales.ss_store_sk, date_dim.d_year, date_dim.d_qoy, item.i_category 
      """.stripMargin.trim,
      s"""
         |SELECT 'store' channel, store_sales.ss_store_sk col_name, date_dim.d_year, date_dim.d_qoy, 
         |       item.i_category, SUM(store_sales.ss_ext_sales_price) ext_sales_price 
         |FROM date_dim, store_sales, item
         |WHERE store_sales.ss_store_sk IS NULL
         |      AND store_sales.ss_sold_date_sk = date_dim.d_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |GROUP BY store_sales.ss_store_sk, date_dim.d_year, date_dim.d_qoy, item.i_category 
      """.stripMargin.trim,
      s"""
         |SELECT item.i_brand_id brand_id, item.i_brand brand, SUM(ss_ext_sales_price) ext_price 
         |FROM date_dim, store_sales, item
         |WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND item.i_manager_id = 28
         |      AND date_dim.d_year = 1999
         |      AND date_dim.d_moy = 11
         |GROUP BY item.i_brand_id, item.i_brand 
      """.stripMargin.trim,
      s"""
         |SELECT item.i_brand_id brand_id, item.i_brand_id brand, SUM(ss_ext_sales_price) ext_price 
         |FROM date_dim, store_sales, item
         |WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND item.i_manager_id = 28
         |      AND date_dim.d_year = 1999
         |      AND date_dim.d_moy = 11
         |GROUP BY item.i_brand_id, item.i_class_id,item.i_category_id 
      """.stripMargin.trim,
      s"""
         |SELECT 'store' channel, item.i_brand_id, item.i_class_id, item.i_category_id, 
         |       SUM(store_sales.ss_quantity*store_sales.ss_list_price) sales, count(*) number_sales
         |FROM date_dim, store_sales, item
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND date_dim.d_year = 1999 + 2
         |      AND date_dim.d_moy = 11
         |GROUP BY item.i_brand_id, item.i_class_id,item.i_category_id 
      """.stripMargin.trim,
      s"""
         |SELECT substr(item.i_item_desc,1,30) itemdesc, item.i_item_sk item_sk, dt.d_date solddate, count(*) cnt
         |FROM date_dim dt, store_sales, item
         |WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
         |      AND store_sales.ss_item_sk = item.i_item_sk
         |      AND dt.d_year in (2000, 2000+1, 2000+2, 2000+3)
         |GROUP BY substr(item.i_item_desc,1,30), item.i_item_sk,dt.d_date
      """.stripMargin.trim,

      s"""
         |SELECT store_sales.ss_store_sk,date_dim.d_year,
         |       COUNT(*) numsales
         |FROM date_dim, store_sales
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |GROUP BY store_sales.ss_store_sk,date_dim.d_year GROUPING SETS (store_sales.ss_store_sk,date_dim.d_year)
      """.stripMargin.trim,
      s"""
         |SELECT store_sales.ss_store_sk,date_dim.d_year,
         |       SUM(store_sales.ss_ext_sales_price) as itemrevenue
         |FROM date_dim, store_sales
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |GROUP BY CUBE(store_sales.ss_store_sk,date_dim.d_year)
      """.stripMargin.trim,
      s"""
         |SELECT date_dim.d_moy,date_dim.d_qoy, date_dim.d_year,
         |       SUM(store_sales.ss_ext_sales_price) as itemrevenue
         |FROM date_dim, store_sales
         |WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
         |GROUP BY ROLLUP(date_dim.d_moy,date_dim.d_qoy, date_dim.d_year)
      """.stripMargin.trim
      )
}