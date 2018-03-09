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

object TestSQLBatch {

  val testSQLBatch = Seq[String](
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
         |SELECT fact.B
         |FROM
         |  fact
         |UNION ALL
         |SELECT fact.B
         |FROM
         |  fact
         |UNION ALL
         |SELECT fact.B
         |FROM
         |  fact
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
         |UNION ALL
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
         |UNION ALL
         |SELECT fact.B
         |FROM
         |  fact
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
         |UNION ALL
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
         |UNION ALL
         |SELECT fact.B
         |FROM
         |  fact
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
         |UNION ALL
         |SELECT fact.A
         |FROM
         |  fact
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*) AS A
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*) AS A
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |WHERE f1.E IS NULL AND f1.C > d1.E AND f1.B = 2
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*) AS A
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |WHERE f1.E IS NULL AND f1.C > d1.E AND d1.E = 3
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*) AS A
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |WHERE f1.E IS NULL AND f1.C > d1.E
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*) AS A
         |FROM
         |  fact f1
         |  JOIN dim d1 ON (f1.K = d1.K)
         |  JOIN dim d2 ON (f1.K = d2.K AND d2.E > 0)
         |WHERE f1.E IS NULL AND f1.C > d1.E
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim d1 ON (fact.K = d1.K)
         |  JOIN dim d2 ON (fact.K = d2.K AND d2.E > 0)
         |WHERE fact.E IS NULL AND fact.C > d1.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > dim.E AND (dim.E IS NULL OR dim1.G IS NULL)
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > dim.E OR dim1.G IS NULL
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E OR dim.E IS NULL
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E AND dim.E IS NULL
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > dim.E
      """.stripMargin.trim,
      s"""
         |SELECT fact.A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K AND fact.K IS NOT NULL)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0 AND dim1.K IS NOT NULL)
         |WHERE fact.E IS NULL AND fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K AND fact.K IS NOT NULL)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.E IS NULL AND fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.E IS NULL AND fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K AND dim1.G > 0)
         |WHERE fact.C > fact.E AND fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > fact.E AND (fact.C > dim.E OR dim1.G > 0)
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > fact.E AND fact.C > dim.E OR dim1.G > 0
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > fact.E AND fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > fact.E OR fact.C > dim.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |WHERE fact.C > fact.E
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,COUNT(*) AS A
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,COUNT(*) AS S1
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |GROUP BY fact.A
         |--HAVING COUNT(*) > 5
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,COUNT(*)--, my_fun(3) AS S1
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |GROUP BY fact.A
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,COUNT(*) AS S1
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |GROUP BY fact.A
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,sum(cast(dim.D as bigint)) AS S1
         |FROM
         |  fact
         |  JOIN dim ON (fact.K = dim.K)
         |  JOIN dim1 ON (fact.K = dim1.K)
         |GROUP BY fact.A
      """.stripMargin.trim,
      s"""
         |SELECT FOO.A, sum(cast(FOO.B as bigint)) AS S
         |FROM (SELECT fact.A, fact.B
         |      FROM
         |        fact
         |        JOIN dim ON (fact.K = dim.K)) FOO
         |GROUP BY FOO.A
      """.stripMargin.trim,
      s"""
         |SELECT FOO.A, sum(cast(FOO.B as bigint)) AS S
         |FROM (SELECT fact.A, fact.B
         |      FROM
         |        fact
         |        JOIN dim ON (fact.K = dim.K)) FOO
         |GROUP BY FOO.A
      """.stripMargin.trim,
      s"""
         |SELECT f1.A,f1.B,COUNT(*)
         |FROM
         |  fact f1
         |  JOIN fact f2 ON (f1.K = f2.K)
         |  JOIN fact f3 ON (f1.K = f3.K)
         |GROUP BY f1.A,f1.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,sum(cast(dim.D as bigint)) AS S1
         |FROM
         |  fact
         |  LEFT OUTER JOIN dim ON (fact.K = dim.K)
         |GROUP BY fact.A,fact.B
      """.stripMargin.trim,
      s"""
         |SELECT fact.A,fact.B,fact.C,sum(cast(dim.D as bigint)) AS S1
         |FROM
         |  fact
         |  LEFT OUTER JOIN dim ON (fact.K = dim.K)
         |GROUP BY fact.A,fact.B,fact.C
      """.stripMargin.trim,
//      s"""
//         |SELECT *
//         |FROM fact, dim
//      """.stripMargin.trim,
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