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

import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.testutil.ModularPlanTest

// scalastyle:off println
class Tpcds_1_4_BenchmarkSuite extends ModularPlanTest with BeforeAndAfter {

//  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
//  // spark.conf.set("spark.sql.crossJoin.enabled", true)
//  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
//  val hiveClient = testHive.sparkSession.metadataHive

//  test("test SQLBuilder using tpc-ds queries") {
//
//    tpcds1_4Tables.foreach { create_table =>
//      hiveClient.runSqlHive(create_table)
//    }
//
////    val dest = "qTradeflow"  // this line is for development, comment it out once done
//    val dest = "qSEQ"
////    val dest = "qAggPushDown"    // this line is for development, comment it out once done
////    val dest = "q10"
//
//    tpcds1_4Queries.foreach { query =>
//      if (query._1 == dest) {  // this line is for development, comment it out once done
//        val analyzed = testHive.sql(query._2).queryExecution.analyzed
//        println(s"""\n\n===== Analyzed Logical Plan for ${query._1} =====\n\n$analyzed \n""")
//
////        val cnonicalizedPlan = new SQLBuilder(analyzed).Canonicalizer.execute(analyzed)
////
////        Try(new SQLBuilder(analyzed).toSQL) match {
////          case Success(s) => logInfo(s"""\n\n===== CONVERTED back ${query._1} USING SQLBuilder =====\n\n$s \n""")
////          case Failure(e) => logInfo(s"""Cannot convert the logical query plan of ${query._1} back to SQL""")
////        }
//
//        // this Try is for development, comment it out once done
//        Try(analyzed.optimize) match {
//          case Success(o) => {
//            println(s"""\n\n===== Optimized Logical Plan for ${query._1} =====\n\n$o \n""")
//          }
//          case Failure(e) =>
//        }
//
//        val o = analyzed.optimize
//        val o1 = o.modularize
//
//        Try(o.modularize.harmonize) match {
//          case Success(m) => {
//            println(s"""\n\n===== MODULAR PLAN for ${query._1} =====\n\n$m \n""")
//
//            Try(m.asCompactSQL) match {
//              case Success(s) => println(s"\n\n===== CONVERTED SQL for ${query._1} =====\n\n${s}\n")
//              case Failure(e) => println(s"""\n\n===== CONVERTED SQL for ${query._1} failed =====\n\n${e.toString}""")
//            }
//          }
//          case Failure(e) => println(s"""\n\n==== MODULARIZE the logical query plan for ${query._1} failed =====\n\n${e.toString}""")
//        }
//      }
//    }
//
//  }
}
// scalastyle:on println