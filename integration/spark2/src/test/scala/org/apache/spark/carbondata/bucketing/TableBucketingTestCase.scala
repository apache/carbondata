/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.carbondata.bucketing

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.command.LoadTable
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TableBucketingTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t4")
    spark.sql("DROP TABLE IF EXISTS t5")
    spark.sql("DROP TABLE IF EXISTS t6")
    spark.sql("DROP TABLE IF EXISTS t7")
    spark.sql("DROP TABLE IF EXISTS t8")
  }

  test("test create table with buckets") {
    spark.sql(
      """
           CREATE TABLE t4
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t4")
      """)
    LoadTable(Some("default"), "t4", "./src/test/resources/dataDiff.csv", Nil,
      Map(("use_kettle", "false"))).run(spark)
    val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default_t4")
    if (table != null && table.getBucketingInfo("t4") != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test create table with no bucket join of carbon tables") {
    spark.sql(
      """
           CREATE TABLE t5
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("tableName"="t5")
      """)
    LoadTable(Some("default"), "t5", "./src/test/resources/dataDiff.csv", Nil,
      Map(("use_kettle", "false"))).run(spark)

    val plan = spark.sql(
      """
        |select t1.*, t2.*
        |from t5 t1, t5 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchange => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
  }

  test("test create table with bucket join of carbon tables") {
    spark.sql(
      """
           CREATE TABLE t6
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t6")
      """)
    LoadTable(Some("default"), "t6", "./src/test/resources/dataDiff.csv", Nil,
      Map(("use_kettle", "false"))).run(spark)

    val plan = spark.sql(
      """
        |select t1.*, t2.*
        |from t6 t1, t6 t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchange => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test create table with bucket join of carbon table and parquet table") {
    spark.sql(
      """
           CREATE TABLE t7
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t7")
      """)
    LoadTable(Some("default"), "t7", "./src/test/resources/dataDiff.csv", Nil,
      Map(("use_kettle", "false"))).run(spark)

    spark.sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    spark.sql("select * from t7").write
      .format("parquet")
      .bucketBy(4, "name")
      .saveAsTable("bucketed_parquet_table")

    val plan = spark.sql(
      """
        |select t1.*, t2.*
        |from t7 t1, bucketed_parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchange => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("test create table with bucket join of carbon table and non bucket parquet table") {
    spark.sql(
      """
           CREATE TABLE t8
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t8")
      """)
    LoadTable(Some("default"), "t8", "./src/test/resources/dataDiff.csv", Nil,
      Map(("use_kettle", "false"))).run(spark)

    spark.sql("DROP TABLE IF EXISTS parquet_table")
    spark.sql("select * from t8").write
      .format("parquet")
      .saveAsTable("parquet_table")

    val plan = spark.sql(
      """
        |select t1.*, t2.*
        |from t8 t1, parquet_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchange => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non bucket tables")
  }

  override def afterAll {
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t4")
    spark.sql("DROP TABLE IF EXISTS t5")
    spark.sql("DROP TABLE IF EXISTS t6")
    spark.sql("DROP TABLE IF EXISTS t7")
    spark.sql("DROP TABLE IF EXISTS t8")
  }
}
