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

package org.apache.carbondata.mv.tool

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.tpcds.{TestCommonSubexpressionManager, TestHiveSingleton, TestQueryBatchPreprocessor}
import org.apache.spark.sql.internal.SQLConf

import org.apache.carbondata.mv.dsl._
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.tool.constructing.TestTPCDS_1_4_MVBatch._

class CostBasedCSEManagerSuite extends TestHiveSingleton {

  test("typical mqo test") {    
    
//    CommonSubexpressionRuleEngine.tableCluster.set(QueryBatchRuleEngine.getTableCluster())
    
    val table2columnset = Map("date_dim" -> Set("d_year", "d_moy", "d_date_sk", "d_date", "d_qoy"),
                              "store_sales" -> Set("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_quantity", "ss_sales_price", "ss_store_sk", "ss_ext_sales_price"),
                              "item" -> Set("i_brand", "i_brand_id", "i_manufact_id", "i_item_sk", "i_item_desc", "i_manager_id", "i_category", "i_item_id", "i_class", "i_current_price"),
                              "customer" -> Set("c_customer_sk"),
                              "catalog_sales" -> Set("cs_quantity", "cs_list_price", "cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_ship_addr_sk", "cs_ext_sales_price", "cs_item_sk"),
                              "web_sales" -> Set("ws_quantity", "ws_list_price", "ws_sold_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_ship_customer_sk", "ws_ext_sales_price"),
                              "dim_apn_iot" -> Set("industry", "apn_name", "city_ascription", "service_level"),
                              "sdr_dyn_seq_custer_iot_all_hour_60min" -> Set("starttime", "dim_52", "dim_51", "dim_10"),
                              "tradeflow_all" -> Set("y_year", "dollar_value", "country", "b_country", "imex", "hs_code"),
                              "country" -> Set("countryid", "country_en", "country_cn"),
                              "updatetime" -> Set("countryid", "imex", "newdate", "startdate")
                             )

    spark.conf.set(SQLConf.CBO_ENABLED.key, "true")
    spark.conf.set(SQLConf.CASE_SENSITIVE.key, "false")
    
    for (tableName <- table2columnset.keySet) {
      val sqlDF = spark.sql(s"SELECT * FROM $tableName LIMIT 3")
//      sqlDF.show()
      spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      val cols = table2columnset.get(tableName).map(_.mkString(", ")).getOrElse("")
      spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS $cols")
    }

//    val x = spark.sql(s"""
//                        |                SELECT a.country AS countryid
//                        |                        ,c.country_cn AS country_show_cn
//                        |                        ,c.country_en AS country
//                        |                        ,sum(v2014) AS colunm_2014
//                        |                        ,sum(v2015) AS colunm_2015
//                        |                        ,sum(v2016) AS colunm_2016
//                        |                        ,(sum(v2016) - sum(v2015)) / sum(v2015) AS tb
//                        |                FROM (
//                        |                        SELECT b_country AS Country
//                        |                                ,sum(CASE 
//                        |                                                WHEN y_year = 2014
//                        |                                                        THEN dollar_value
//                        |                                                ELSE 0
//                        |                                                END) AS v2014
//                        |                                ,sum(CASE 
//                        |                                                WHEN y_year = 2015
//                        |                                                        THEN dollar_value
//                        |                                                ELSE 0
//                        |                                                END) AS v2015
//                        |                                ,sum(CASE 
//                        |                                                WHEN y_year = 2016
//                        |                                                        THEN dollar_value
//                        |                                                ELSE 0
//                        |                                                END) AS v2016
//                        |                        FROM tradeflow_all
//                        |                        WHERE imex = 0
//                        |                                AND (
//                        |                                        y_year = 2014
//                        |                                        OR y_year = 2015
//                        |                                        OR y_year = 2016
//                        |                                        )
//                        |                        GROUP BY b_country
//                        |                                ,y_year
//                        |                        ) a
//                        |                LEFT JOIN country c ON (a.country = c.countryid)
//                        |                GROUP BY country_show_cn
//                        |                        ,country
//                        |                        ,countryid
//                        |                        ,country_en
//                       """.stripMargin.trim)
//    val x = spark.sql(
//             s"""
//               |SELECT *
//               |FROM (
//               |	SELECT DISTINCT country_show_cn
//               |		,country
//               |		,(
//               |			CASE WHEN up.startdate <= '201401'
//               |					AND up.newdate >= '201412' THEN CASE WHEN isnan(colunm_2014) THEN 0 ELSE colunm_2014 END ELSE NULL END
//               |			) AS colunm_2014
//               |		,(
//               |			CASE WHEN up.startdate <= '201501'
//               |					AND up.newdate >= '201512' THEN CASE WHEN isnan(colunm_2015) THEN 0 ELSE colunm_2015 END ELSE NULL END
//               |			) AS colunm_2015
//               |		,(
//               |			CASE WHEN up.startdate <= '201601'
//               |					AND up.newdate >= '201612' THEN CASE WHEN isnan(colunm_2016) THEN 0 ELSE colunm_2016 END ELSE NULL END
//               |			) AS colunm_2016
//               |		,tb
//               |		,concat_ws('-', up.startdate, up.newdate) AS dbupdate
//               |	FROM (
//               |		SELECT a.country AS countryid
//               |			,c.country_cn AS country_show_cn
//               |			,c.country_en AS country
//               |			,sum(v2014) AS colunm_2014
//               |			,sum(v2015) AS colunm_2015
//               |			,sum(v2016) AS colunm_2016
//               |			,(sum(v2016) - sum(v2015)) / sum(v2015) AS tb
//               |		FROM (
//               |			SELECT b_country AS Country
//               |				,sum(CASE WHEN y_year = 2014 THEN dollar_value ELSE 0 END) AS v2014
//               |				,sum(CASE WHEN y_year = 2015 THEN dollar_value ELSE 0 END) AS v2015
//               |				,sum(CASE WHEN y_year = 2016 THEN dollar_value ELSE 0 END) AS v2016
//               |			FROM tradeflow_all
//               |			WHERE imex = 0
//               |				AND (
//               |					y_year = 2014
//               |					OR y_year = 2015
//               |					OR y_year = 2016
//               |					)
//               |			GROUP BY b_country
//               |				,y_year
//               |			) a
//               |		LEFT JOIN country c ON (a.country = c.countryid)
//               |		GROUP BY country_show_cn
//               |			,country
//               |			,countryid
//               |      ,country_en
//               |		) w
//               |	LEFT JOIN updatetime up ON (
//               |			w.countryid = up.countryid
//               |			AND imex = 0
//               |			)
//               |	WHERE !(isnan(colunm_2014)
//               |			AND isnan(colunm_2015)
//               |			AND isnan(colunm_2016))
//               |		AND (
//               |			colunm_2014 <> 0
//               |			OR colunm_2015 <> 0
//               |			OR colunm_2016 <> 0
//               |			)
//               |	) f
//               |WHERE colunm_2014 IS NOT NULL
//               |	OR colunm_2015 IS NOT NULL
//               |	OR colunm_2016 IS NOT NULL
//              """.stripMargin.trim)       
    
//    val x = spark.sql(s"""SELECT * FROM tradeflow_all LIMIT 1""")
//    val y = spark.sql(s"""SELECT * FROM country LIMIT 1""")
//    val z = spark.sql(s"""SELECT * FROM updatetime LIMIT 1""")
//    val stats = spark.sessionState.catalog.getTableMetadata(TableIdentifier("store_sales")).stats
//    val c = stats.get.rowCount
//    val z = spark.table("store_sales").queryExecution.optimizedPlan.stats(conf)
//    val x = spark.sql(s"""SELECT customer.`c_customer_sk`, sum((CAST(CAST(store_sales.`ss_quantity` AS DECIMAL(10,0)) AS DECIMAL(12,2)) * CAST(store_sales.`ss_sales_price` AS DECIMAL(12,2)))) AS `sum(CheckOverflow((promote_precision(cast(cast(ss_quantity#54986 as decimal(10,0)) as decimal(12,2))) * promote_precision(cast(ss_sales_price#54989 as decimal(12,2)))), DecimalType(18,2)))`   FROM  store_sales  INNER JOIN customer   ON (store_sales.`ss_customer_sk` = customer.`c_customer_sk`)  GROUP BY customer.`c_customer_sk`""")
//    val x = spark.sql(s"""SELECT item.`i_brand`, count(1) AS `count(1)`, date_dim.`d_year`, item.`i_brand_id`, sum(store_sales.`ss_ext_sales_price`) AS `ext_price`, item.`i_item_sk`   FROM  store_sales  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)  GROUP BY item.`i_brand`, date_dim.`d_date`, substr(item.`i_item_desc`, 1, 30), date_dim.`d_year`, item.`i_brand_id`, item.`i_item_sk`""")
//    val x = spark.sql(s"""SELECT item.`i_manufact_id`, item.`i_brand`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`, item.`i_manager_id`   FROM  store_sales  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)  GROUP BY item.`i_manufact_id`, item.`i_brand`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id`, item.`i_manager_id`""")
//    val x = spark.sql(s"""SELECT item.`i_brand`, date_dim.`d_year`, item.`i_brand_id`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`   FROM  store_sales  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`)  GROUP BY item.`i_brand`, date_dim.`d_year`, item.`i_brand_id`""")
//    val p = x.queryExecution.optimizedPlan
//    val y = p.stats(conf)
//    val z = x.count()
//    val s = spark.sql(s"""SELECT item.`i_brand` FROM item""")
//    val t = s.count()
//    val s1 = spark.sql(s"""SELECT DISTINCT item.`i_brand` FROM item""")
//    val t1 = s1.count()
//    val s2 = spark.sql(s"""SELECT DISTINCT item.`i_brand_id` FROM item""")
//    val t2 = s2.count()
//    val s3 = spark.sql(s"""SELECT DISTINCT date_dim.`d_year` FROM date_dim""")
//    val t3 = s3.count()
//    val s4 = spark.sql(s"""SELECT DISTINCT item.`i_brand`, item.`i_brand_id` FROM item""")
//    val t4 = s4.count()
//      val s5 = spark.sql(s"""SELECT item.`i_item_id`, item.`i_brand`, item.`i_brand_id`, item.`i_current_price`, sum(store_sales.`ss_ext_sales_price`) AS `ext_price`, date_dim.`d_moy`, item.`i_manager_id`, date_dim.`d_year`, item.`i_item_desc`, date_dim.`d_date`, item.`i_category`, item.`i_class`, item.`i_manufact_id` FROM  store_sales  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`) GROUP BY item.`i_item_id`, item.`i_brand`, item.`i_brand_id`, item.`i_current_price`, date_dim.`d_moy`, item.`i_manager_id`, date_dim.`d_year`, item.`i_item_desc`, date_dim.`d_date`, item.`i_category`, item.`i_class`, item.`i_manufact_id`""")
//      val t5 = s5.count()
//      val s6 = spark.sql(s"""SELECT item.`i_category`, item.`i_manufact_id`, sum(store_sales.`ss_ext_sales_price`) AS `itemrevenue`, item.`i_class`, item.`i_current_price`, item.`i_item_id`, item.`i_item_desc`, item.`i_brand`, date_dim.`d_date`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id` FROM  store_sales  INNER JOIN date_dim ON (store_sales.`ss_sold_date_sk` = date_dim.`d_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`) GROUP BY item.`i_category`, item.`i_manufact_id`, item.`i_class`, item.`i_current_price`, item.`i_item_id`, item.`i_item_desc`, item.`i_brand`, date_dim.`d_date`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id`""")
//      val t6 = s6.count()
//      val s7 = spark.sql(s"""SELECT item.`i_manufact_id`, sum(store_sales.`ss_ext_sales_price`) AS `sum_agg`, item.`i_manager_id`, item.`i_brand`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id` FROM  store_sales  INNER JOIN date_dim ON (date_dim.`d_date_sk` = store_sales.`ss_sold_date_sk`)  INNER JOIN item ON (store_sales.`ss_item_sk` = item.`i_item_sk`) GROUP BY item.`i_manufact_id`, item.`i_manager_id`, item.`i_brand`, date_dim.`d_moy`, date_dim.`d_year`, item.`i_brand_id`""")
//      val t7 = s7.count()
//      val p6 = s6.queryExecution.optimizedPlan
//      val y6 = p6.stats(conf)
//      val s8 = spark.sql(s"""SELECT DISTINCT date_dim.`d_date` FROM date_dim""")
//      val t8 = s8.count()
//    x.explain()
//    x.show()
//    y.show()
//    z.show()
    val stats = spark.sessionState.catalog.getTableMetadata(TableIdentifier("country")).stats
    val stats1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tradeflow_all")).stats
    val stats2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("updatetime")).stats

    val dest = "case_4"
//    val dest = "case_5"

    tpcds_1_4_testCases.foreach { testcase =>
      val batch = mutable.ArrayBuffer[ModularPlan]()
      if (testcase._1 == dest) {
        testcase._2.foreach { query =>
          val analyzed = spark.sql(query).queryExecution.analyzed
          batch += analyzed.optimize.modularize.harmonize
        }
        val b = collection.immutable.Seq(batch:_*)
        val qbPreprocessor = TestQueryBatchPreprocessor
        val fb = qbPreprocessor.preprocess(b.map(p => (p, 1)))
        val iterator = fb.groupBy(_._1.signature).toIterator
        for ((signature, batchBySignature) <- iterator) {
          val cses = TestCommonSubexpressionManager.execute(batchBySignature)
          cses.foreach { case (plan, freq) => 
            Try(plan.asCompactSQL) match {
              case Success(s) => println(s"\n\n===== MV candidate for ${testcase._1} =====\n\n${s}\n\n")
              case Failure(e) => println(s"""\n\n===== MV candidate for ${testcase._1} failed =====\n\n${e.toString}""")
            }
        }
          
        // TODO: add regression verification
//        comparePlanCollections(cses.map(_._1.toString.stripMargin.trim), testcase._3)
        }
      }
    }
    
  }
    
}