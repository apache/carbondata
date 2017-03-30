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

package org.apache.spark.carbondata.bucketing

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.command.LoadTable
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TableBucketingTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
    sql("DROP TABLE IF EXISTS t3")
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
    sql("DROP TABLE IF EXISTS t9")
  }

  test("test create table with buckets") {
    sql(
      """
           CREATE TABLE t4
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t4")
      """)
    LoadTable(Some("default"), "t4", s"$resourcesPath/source.csv", Nil,
      Map()).run(sqlContext.sparkSession)
    val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default_t4")
    if (table != null && table.getBucketingInfo("t4") != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("must be unable to create if number of buckets is in negative number") {
    try {
      sql(
        """
           CREATE TABLE t9
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="-1", "bucketcolumns"="name", "tableName"="t9")
        """)
      assert(false)
    }
    catch {
      case malformedCarbonCommandException: MalformedCarbonCommandException => assert(true)
    }
  }

  test("test create table with no bucket join of carbon tables") {
    sql(
      """
           CREATE TABLE t5
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("tableName"="t5")
      """)
    LoadTable(Some("default"), "t5", s"$resourcesPath/source.csv", Nil,
      Map()).run(sqlContext.sparkSession)

    val plan = sql(
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
    sql(
      """
           CREATE TABLE t6
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t6")
      """)
    LoadTable(Some("default"), "t6", s"$resourcesPath/source.csv", Nil,
      Map()).run(sqlContext.sparkSession)

    val plan = sql(
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
    sql(
      """
           CREATE TABLE t7
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t7")
      """)
    LoadTable(Some("default"), "t7", s"$resourcesPath/source.csv", Nil,
      Map()).run(sqlContext.sparkSession)

    sql("DROP TABLE IF EXISTS bucketed_parquet_table")
    sql("select * from t7").write
      .format("parquet")
      .bucketBy(4, "name")
      .saveAsTable("bucketed_parquet_table")

    val plan = sql(
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
    sql(
      """
           CREATE TABLE t8
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           USING org.apache.spark.sql.CarbonSource
           OPTIONS("bucketnumber"="4", "bucketcolumns"="name", "tableName"="t8")
      """)
    LoadTable(Some("default"), "t8", s"$resourcesPath/source.csv", Nil,
      Map()).run(sqlContext.sparkSession)

    sql("DROP TABLE IF EXISTS parquet_table")
    sql("select * from t8").write
      .format("parquet")
      .saveAsTable("parquet_table")

    val plan = sql(
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

  test("test scalar subquery with equal") {
    sql(
      """select sum(salary) from t4 t1
        |where ID = (select sum(ID) from t4 t2 where t1.name = t2.name)""".stripMargin)
      .count()
  }

  test("test scalar subquery with lessthan") {
    sql(
      """select sum(salary) from t4 t1
        |where ID < (select sum(ID) from t4 t2 where t1.name = t2.name)""".stripMargin)
      .count()
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("DROP TABLE IF EXISTS t4")
    sql("DROP TABLE IF EXISTS t5")
    sql("DROP TABLE IF EXISTS t6")
    sql("DROP TABLE IF EXISTS t7")
    sql("DROP TABLE IF EXISTS t8")
  }
}
