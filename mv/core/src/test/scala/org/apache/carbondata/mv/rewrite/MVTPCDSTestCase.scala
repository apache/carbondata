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
import org.apache.carbondata.mv.rewrite.matching.TestTPCDS_1_4_Batch._
import org.apache.carbondata.mv.testutil.Tpcds_1_4_Tables.tpcds1_4Tables

class MVTPCDSTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark/src/test/resources"
    sql("drop database if exists tpcds cascade")
    sql("create database tpcds")
    sql("use tpcds")

    tpcds1_4Tables.foreach { create_table =>
      sql(create_table)
    }

  }

  ignore("test create materialized view with tpcds_1_4_testCases case_1") {
    sql(s"drop materialized view if exists datamap_tpcds1")
    sql(s"create materialized view datamap_tpcds1  as ${tpcds_1_4_testCases(0)._2}")
    val df = sql(tpcds_1_4_testCases(0)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds1"))
    sql(s"drop materialized view datamap_tpcds1")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_3") {
    sql(s"drop materialized view if exists datamap_tpcds3")
    sql(s"create materialized view datamap_tpcds3  as ${tpcds_1_4_testCases(2)._2}")
    val df = sql(tpcds_1_4_testCases(2)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds3"))
    sql(s"drop materialized view datamap_tpcds3")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_4") {
    sql(s"drop materialized view if exists datamap_tpcds4")
    sql(s"create materialized view datamap_tpcds4  as ${tpcds_1_4_testCases(3)._2}")
    val df = sql(tpcds_1_4_testCases(3)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds4"))
    sql(s"drop materialized view datamap_tpcds4")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_5") {
    sql(s"drop materialized view if exists datamap_tpcds5")
    sql(s"create materialized view datamap_tpcds5  as ${tpcds_1_4_testCases(4)._2}")
    val df = sql(tpcds_1_4_testCases(4)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds5"))
    sql(s"drop materialized view datamap_tpcds5")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_6") {
    sql(s"drop materialized view if exists datamap_tpcds6")
    sql(s"create materialized view datamap_tpcds6  as ${tpcds_1_4_testCases(5)._2}")
    val df = sql(tpcds_1_4_testCases(5)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds6"))
    sql(s"drop materialized view datamap_tpcds6")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_8") {
    sql(s"drop materialized view if exists datamap_tpcds8")
    sql(s"create materialized view datamap_tpcds8  as ${tpcds_1_4_testCases(7)._2}")
    val df = sql(tpcds_1_4_testCases(7)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds8"))
    sql(s"drop materialized view datamap_tpcds8")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_11") {
    sql(s"drop materialized view if exists datamap_tpcds11")
    sql(s"create materialized view datamap_tpcds11  as ${tpcds_1_4_testCases(10)._2}")
    val df = sql(tpcds_1_4_testCases(10)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds11"))
    sql(s"drop materialized view datamap_tpcds11")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_15") {
    sql(s"drop materialized view if exists datamap_tpcds15")
    sql(s"create materialized view datamap_tpcds15  as ${tpcds_1_4_testCases(14)._2}")
    val df = sql(tpcds_1_4_testCases(14)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds15"))
    sql(s"drop materialized view datamap_tpcds15")
  }

  ignore("test create materialized view with tpcds_1_4_testCases case_16") {
    sql(s"drop materialized view if exists datamap_tpcds16")
    sql(s"create materialized view datamap_tpcds16  as ${tpcds_1_4_testCases(15)._2}")
    val df = sql(tpcds_1_4_testCases(15)._3)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_tpcds16"))
    sql(s"drop materialized view datamap_tpcds16")
  }

  def drop(): Unit = {
    sql("use default")
    sql("drop database if exists tpcds cascade")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}
