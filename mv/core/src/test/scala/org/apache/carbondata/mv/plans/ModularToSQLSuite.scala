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

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.dsl.Plans._
import org.apache.carbondata.mv.testutil.ModularPlanTest

class ModularToSQLSuite extends ModularPlanTest with BeforeAndAfter {

  import org.apache.carbondata.mv.testutil.TestSQLBatch._

  val spark = sqlContext
  val testHive = sqlContext.sparkSession
  
  ignore("convert modular plans to sqls") {
    
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Fact (
           |  `A` int,
           |  `B` int,
           |  `C` int,
           |  `E` int,
           |  `K` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE        
        """.stripMargin.trim
        )
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Dim (
           |  `D` int,
           |  `E` int,
           |  `K` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE        
        """.stripMargin.trim
        )
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Dim1 (
           |  `F` int,
           |  `G` int,
           |  `K` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE        
        """.stripMargin.trim
        )
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists store_sales (
           |  `ss_sold_date_sk` int,
           |  `ss_item_sk` int,
           |  `ss_quantity` int,
           |  `ss_list_price` decimal(7,2),
           |  `ss_ext_sales_price` decimal(7,2),
           |  `ss_store_sk` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
        """.stripMargin.trim
    )
    
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists date_dim (
           |  `d_date_sk` int,
           |  `d_date` date,
           |  `d_year` int,
           |  `d_moy` int,
           |  `d_qoy` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
        """.stripMargin.trim
    )
    
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists item (
           |  `i_item_sk` int,
           |  `i_item_id` string,
           |  `i_brand` string,
           |  `i_brand_id` int,
           |  `i_item_desc` string,
           |  `i_class_id` int,
           |  `i_class` string,
           |  `i_category` string,
           |  `i_category_id` int,
           |  `i_manager_id` int,
           |  `i_current_price` decimal(7,2)
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
        """.stripMargin.trim
    )
        
    testHive.udf.register("my_fun", (s: Integer) => s)
    
    testSQLBatch.foreach { query =>
      val analyzed = testHive.sql(query).queryExecution.optimizedPlan
      val modularPlan = analyzed.optimize.modularize

      LOGGER.info(s"\n\n===== MODULAR PLAN =====\n\n${modularPlan.treeString} \n")
      
      val compactSql = modularPlan.asCompactSQL
      val convertedSql = modularPlan.asOneLineSQL

      LOGGER.info(s"\n\n===== CONVERTED SQL =====\n\n$compactSql \n")
      
      val analyzed1 = testHive.sql(convertedSql).queryExecution.optimizedPlan
      val modularPlan1 = analyzed1.optimize.modularize

      LOGGER.info(s"\n\n===== CONVERTED SQL =====\n\n$compactSql \n")

      LOGGER.info(s"\n\n===== MODULAR PLAN1 =====\n\n${modularPlan1.treeString} \n")
      
      comparePlans(modularPlan, modularPlan1)
    }

  }
  
}