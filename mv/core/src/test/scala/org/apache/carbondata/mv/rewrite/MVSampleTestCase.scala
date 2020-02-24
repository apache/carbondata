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

import java.io.File

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.rewrite.matching.TestSQLBatch._

class MVSampleTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark/src/test/resources"
    sql("drop database if exists sample cascade")
    sql("create database sample")
    sql("use sample")

    createTables.map(sql)

  }

  def createTables: Seq[String] = {
    Seq[String](
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
         |STORED AS carbondata
        """.stripMargin.trim,
      s"""
         |CREATE TABLE Dim (
         |  `lid`     int,
         |  `city`    string,
         |  `state`   string,
         |  `country` string
         |)
         |STORED AS carbondata
        """.stripMargin.trim,
      s"""
         |CREATE TABLE Item (
         |  `i_item_id`     int,
         |  `i_item_sk`     int
         |)
         |STORED AS carbondata
        """.stripMargin.trim
    )
  }


  test("test create materialized view with sampleTestCases case_1") {
    sql(s"drop materialized view if exists datamap_sm1")
    sql(s"create materialized view datamap_sm1  as ${sampleTestCases(0)._2}")
    val df = sql(sampleTestCases(0)._3)
    assert(!TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm1"))
    sql(s"drop materialized view datamap_sm1")
  }

  test("test create materialized view with sampleTestCases case_3") {
    sql(s"drop materialized view if exists datamap_sm2")
    sql(s"create materialized view datamap_sm2  as ${sampleTestCases(2)._2}")
    val df = sql(sampleTestCases(2)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm2"))
    sql(s"drop materialized view datamap_sm2")
  }

  test("test create materialized view with sampleTestCases case_4") {
    sql(s"drop materialized view if exists datamap_sm3")
    sql(s"create materialized view datamap_sm3  as ${sampleTestCases(3)._2}")
    val df = sql(sampleTestCases(3)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm3"))
    sql(s"drop materialized view datamap_sm3")
  }

  test("test create materialized view with sampleTestCases case_5") {
    sql(s"drop materialized view if exists datamap_sm4")
    sql(s"create materialized view datamap_sm4  as ${sampleTestCases(4)._2}")
    val df = sql(sampleTestCases(4)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm4"))
    sql(s"drop materialized view datamap_sm4")
  }

  test("test create materialized view with sampleTestCases case_6") {
    sql(s"drop materialized view if exists datamap_sm5")
    sql(s"create materialized view datamap_sm5  as ${sampleTestCases(5)._2}")
    val df = sql(sampleTestCases(5)._3)
    assert(!TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm5"))
    sql(s"drop materialized view datamap_sm5")
  }

  test("test create materialized view with sampleTestCases case_7") {
    sql(s"drop materialized view if exists datamap_sm6")
    sql(s"create materialized view datamap_sm6  as ${sampleTestCases(6)._2}")
    val df = sql(sampleTestCases(6)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm6"))
    sql(s"drop materialized view datamap_sm6")
  }

  test("test create materialized view with sampleTestCases case_8") {
    sql(s"drop materialized view if exists datamap_sm7")
    sql(s"create materialized view datamap_sm7  as ${sampleTestCases(7)._2}")
    val df = sql(sampleTestCases(7)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm7"))
    sql(s"drop materialized view datamap_sm7")
  }

  test("test create materialized view with sampleTestCases case_9") {
    sql(s"drop materialized view if exists datamap_sm8")
    sql(s"create materialized view datamap_sm8  as ${sampleTestCases(8)._2}")
    val df = sql(sampleTestCases(8)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_sm8"))
    sql(s"drop materialized view datamap_sm8")
  }

  def drop(): Unit = {
    sql("use default")
    sql("drop database if exists sample cascade")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}
