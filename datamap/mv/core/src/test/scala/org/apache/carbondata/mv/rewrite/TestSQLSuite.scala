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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.util.PlanTest
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.MQOSession

class TestSQLSuite extends PlanTest with BeforeAndAfter {

  import org.apache.carbondata.mv.rewrite.matching.TestSQLBatch._

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
  val hiveClient = testHive.sparkSession.metadataHive

  test("protypical mqo rewrite test") {

    hiveClient.runSqlHive(
      s"""
         |CREATE TABLE Fact (
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
         |CREATE TABLE Dim (
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
         |CREATE TABLE Item (
         |  `i_item_id`     int,
         |  `i_item_sk`     int
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
        """.stripMargin.trim
    )

    val dest = "case_10"

    sampleTestCases.foreach { testcase =>
      if (testcase._1 == dest) {
        val mqoSession = new MQOSession(testHive.sparkSession)
        val summary = testHive.sparkSession.sql(testcase._2)
        mqoSession.sharedState.registerSummaryDataset(summary)
        val rewrittenSQL = mqoSession.rewrite(testcase._3).toCompactSQL.trim

        if (!rewrittenSQL.equals(testcase._4)) {
          println(
            s"""
               |=== FAIL: SQLs do not match ===
               |${sideBySide(rewrittenSQL, testcase._4).mkString("\n")}
              """.stripMargin)
        }
      }

    }
  }
  testHive.sparkSession.cloneSession()
}