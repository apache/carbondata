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

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.scalatest.BeforeAndAfter
import org.apache.carbondata.mv.testutil.ModularPlanTest
import org.apache.spark.sql.util.SparkSQLUtil

class TestSQLSuite extends ModularPlanTest with BeforeAndAfter { 
  import org.apache.carbondata.mv.rewrite.matching.TestSQLBatch._

  val spark = sqlContext
  val testHive = sqlContext.sparkSession
  val hiveClient = CarbonSessionCatalogUtil.getClient(spark.sparkSession)
  
  ignore("protypical mqo rewrite test") {
    
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Fact (
           |  `tid`     int,
           |  `fpgid`   int,
           |  `flid`    int,
           |  `date`    timestamp,
           |  `faid`    int,
           |  `price`   double,
           |  `qty`     int,
           |  `disc`    string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE       
        """.stripMargin.trim
        )
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Dim (
           |  `lid`     int,
           |  `city`    string,
           |  `state`   string,
           |  `country` string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE   
        """.stripMargin.trim
        )    
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE if not exists Item (
           |  `i_item_id`     int,
           |  `i_item_sk`     int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE   
        """.stripMargin.trim
        )

    val dest = "case_11"
        
    sampleTestCases.foreach { testcase =>
      if (testcase._1 == dest) {
        val mvSession = new SummaryDatasetCatalog(testHive)
        val summary = testHive.sql(testcase._2)
        mvSession.registerSummaryDataset(summary)
        val rewrittenSQL =
          mvSession.mvSession.rewrite(mvSession.mvSession.sparkSession.sql(
            testcase._3).queryExecution.analyzed).toCompactSQL.trim

        if (!rewrittenSQL.trim.equals(testcase._4)) {
          fail(
              s"""
              |=== FAIL: SQLs do not match ===
              |${sideBySide(rewrittenSQL, testcase._4).mkString("\n")}
              """.stripMargin)
              }
        }
    
    }
  }

}