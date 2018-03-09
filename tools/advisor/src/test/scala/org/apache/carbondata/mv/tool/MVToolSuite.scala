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

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.hive.tpcds.{TestHiveSingleton, _}
import org.apache.spark.sql.internal.SQLConf

class MVToolSuite extends TestHiveSingleton {
  class MVToolTest extends MVToolBase {
    override protected def getMVFilePath: String = "./output/mv-candidate.sql"

    override def end = {   
      val sdf = new SimpleDateFormat(YML_DEFAULT_DATE_FORMAT)
      sdf.setTimeZone(TimeZone.getTimeZone("UtC"))
      sdf.parse("2017-11-03").getTime
    }
    override def backwardDays = 1
  }
  
  test("advise MVs via query logs") {
    
//    CommonSubexpressionRuleEngine.tableCluster.set(QueryBatchRuleEngine.getTableCluster())
    
    val table2columnset = Map("date_dim" -> Set("d_year", "d_moy", "d_date_sk", "d_date", "d_qoy"),
                              "store_sales" -> Set("ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_quantity", "ss_sales_price", "ss_store_sk", "ss_ext_sales_price"),
                              "item" -> Set("i_brand", "i_brand_id", "i_manufact_id", "i_item_sk", "i_item_desc", "i_manager_id", "i_category", "i_item_id", "i_class", "i_current_price"),
                              "customer" -> Set("c_customer_sk"),
                              "catalog_sales" -> Set("cs_quantity", "cs_list_price", "cs_sold_date_sk", "cs_item_sk", "cs_bill_customer_sk", "cs_ship_addr_sk", "cs_ext_sales_price", "cs_item_sk"),
                              "web_sales" -> Set("ws_quantity", "ws_list_price", "ws_sold_date_sk", "ws_item_sk", "ws_bill_customer_sk", "ws_ship_customer_sk", "ws_ext_sales_price"),
                              "dim_apn_iot" -> Set("industry", "apn_name", "city_ascription", "service_level"),
                              "sdr_dyn_seq_custer_iot_all_hour_60min" -> Set("starttime", "dim_52", "dim_51")
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

    val queryLogEntryPattern = """^[{]\"sql\":\s*\"(.*?)\"[}]$"""
    val mvtool = new MVToolTest
    
    mvtool.adviseMVs(spark, TestQueryBatchPreprocessor, TestCommonSubexpressionManager, "./input/queries-%d.json.log.gz", queryLogEntryPattern)
  }
}