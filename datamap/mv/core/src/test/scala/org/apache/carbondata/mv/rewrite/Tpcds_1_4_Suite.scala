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
import org.apache.spark.sql.test.util.PlanTest
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.testutil.Tpcds_1_4_Tables._

class Tpcds_1_4_Suite extends PlanTest with BeforeAndAfter {

//  test("test using tpc-ds queries") {
//
//    tpcds1_4Tables.foreach { create_table =>
//      hiveClient.runSqlHive(create_table)
//    }
    
//    val dest = "case_30"
//    val dest = "case_32"
    val dest = "case_3"
    
//    tpcds_1_4_testCases.foreach { testcase =>
//      if (testcase._1 == dest) {
//        val mqoSession = new MQOSession(testHive.sparkSession)
//        val summaryDF = testHive.sparkSession.sql(testcase._2)
//        mqoSession.sharedState.registerSummaryDataset(summaryDF)
//
//        Try(mqoSession.rewrite(testcase._3).withSummaryData) match {
//          case Success(rewrittenPlan) =>
//            println(s"""\n\n===== REWRITTEN MODULAR PLAN for ${testcase._1} =====\n\n$rewrittenPlan \n""")
//
//            Try(rewrittenPlan.asCompactSQL) match {
//              case Success(s) =>
//                println(s"\n\n===== CONVERTED SQL for ${testcase._1} =====\n\n${s}\n")
//                if (!s.trim.equals(testcase._4)) {
//                  println(
//                      s"""
//                      |=== FAIL: SQLs do not match ===
//                      |${sideBySide(s, testcase._4).mkString("\n")}
//                      """.stripMargin)
//                      }
//
//              case Failure(e) => println(s"""\n\n===== CONVERTED SQL for ${testcase._1} failed =====\n\n${e.toString}""")
//            }
//
//          case Failure(e) => println(s"""\n\n==== MODULARIZE the logical query plan for ${testcase._1} failed =====\n\n${e.toString}""")
//        }
        
//        val rewrittenSQL = rewrittenPlan.asCompactSQL
//        val rewrittenSQL = mqoSession.rewrite(testcase._3).toCompactSQL

//        if (!rewrittenSQL.equals(testcase._4)) {
//          fail(
//              s"""
//              |=== FAIL: SQLs do not match ===
//              |${sideBySide(rewrittenSQL, testcase._4).mkString("\n")}
//              """.stripMargin)
//              }
//        }
//
//    }
//
//  }
}