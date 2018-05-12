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

import org.apache.spark.sql.catalyst.util._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.mv.dsl._
import org.apache.carbondata.mv.plans.modular.ModularPlanSignatureGenerator
import org.apache.carbondata.mv.testutil.ModularPlanTest
import org.apache.carbondata.mv.testutil.Tpcds_1_4_Tables.tpcds1_4Tables

class SignatureSuite extends ModularPlanTest with BeforeAndAfterAll {
  import org.apache.carbondata.mv.TestSQLBatch._

  override protected def beforeAll(): Unit = {
    sql("drop database if exists tpcds1 cascade")
    sql("create database tpcds1")
    sql("use tpcds1")
    tpcds1_4Tables.foreach { create_table =>
      sql(create_table)
    }

    sql(
      s"""
         |CREATE TABLE Fact (
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

    sql(
      s"""
         |CREATE TABLE Dim (
         |  `D` int,
         |  `E` int,
         |  `K` int
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
        """.stripMargin.trim
    )

    sql(
      s"""
         |CREATE TABLE Dim1 (
         |  `F` int,
         |  `G` int,
         |  `K` int
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
        """.stripMargin.trim
    )

    sqlContext.udf.register("my_fun", (s: Integer) => s)
  }


  test("test signature computing") {

    testSQLBatch.foreach { query =>
      val analyzed = sql(query).queryExecution.analyzed
      val modularPlan = analyzed.optimize.modularize
      val sig = ModularPlanSignatureGenerator.generate(modularPlan)
      sig match {
        case Some(s) if (s.groupby != true || s.datasets != Set("default.fact","default.dim")) =>
          println(
              s"""
              |=== FAIL: signature do not match ===
              |${sideBySide(s.groupby.toString, true.toString).mkString("\n")}
              |${sideBySide(s.datasets.toString, Set("Fact","Dim").toString).mkString("\n")}
            """.stripMargin)
        case _ =>
      }
    }
  }

  override protected def afterAll(): Unit = {
    sql("use default")
    sql("drop database if exists tpcds1 cascade")
  }
}