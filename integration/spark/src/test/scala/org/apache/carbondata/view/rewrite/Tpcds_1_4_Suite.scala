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

package org.apache.carbondata.view.rewrite

import java.io.{File, PrintWriter}

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.optimizer.MVRewrite
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.view.MVCatalogInSpark
import org.apache.carbondata.view.testutil.ModularPlanTest

class Tpcds_1_4_Suite extends ModularPlanTest with BeforeAndAfter {
  import org.apache.carbondata.view.rewrite.matching.TestTPCDS_1_4_Batch._
  import org.apache.carbondata.view.testutil.Tpcds_1_4_Tables._

  val spark = sqlContext
  val testHive = sqlContext.sparkSession

  test("test using tpc-ds queries") {

    tpcds1_4Tables.foreach { create_table =>
      hiveClient.runSqlHive(create_table)
    }

    val writer = new PrintWriter(new File("batch.txt"))
//    val dest = "case_30"
//    val dest = "case_32"
//    val dest = "case_33"
// case_15 and case_16 need revisit

    val dest = "case_39"   /* to run single case, uncomment out this */

    tpcds_1_4_testCases.foreach { testcase =>
      if (testcase._1 == dest) { /* to run single case, uncomment out this */
        val mvSession = new MVCatalogInSpark(testHive)
        val summaryDF = testHive.sql(testcase._2)
        mvSession.registerSchema(summaryDF)

        writer.print(s"\n\n==== ${testcase._1} ====\n\n==== mv ====" +
                     s"\n\n${testcase._2}\n\n==== original query ====\n\n${testcase._3}\n")

        val rewriteSQL = new MVRewrite(mvSession,
          mvSession.session.sql(testcase._3).queryExecution.optimizedPlan,
          mvSession.session).toCompactSQL.trim
        LOGGER.info(s"\n\n\n\n===== Rewritten query for ${testcase._1} =====\n\n${rewriteSQL}\n")

        if (!rewriteSQL.trim.equals(testcase._4)) {
          LOGGER.error(s"===== Rewrite not matched for ${testcase._1}\n")
          LOGGER.error(s"\n\n===== Rewrite failed for ${testcase._1}, Expected: =====" +
                       s"\n\n${testcase._4}\n")
          LOGGER.error(
              s"""
              |=== FAIL: SQLs do not match ===
              |${sideBySide(rewriteSQL, testcase._4).mkString("\n")}
              """.stripMargin)
          writer.print(s"\n\n==== result ====\n\nfailed\n")
          writer.print(s"\n\n==== rewritten query ====\n\n${rewriteSQL}\n")
        }
        else {
          LOGGER.info(s"===== Rewrite successful for ${testcase._1}, as expected\n")
          writer.print(s"\n\n==== result ====\n\nsuccessful\n")
          writer.print(s"\n\n==== rewritten query ====\n\n${rewriteSQL}\n")
        }

        }  /* to run single case, uncomment out this */

    }

    writer.close()
  }
}
