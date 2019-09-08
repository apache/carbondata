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
package org.apache.carbondata.integration.spark.testsuite.timeseries

import java.sql.Timestamp

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeSeriesMatchStrategySuite extends CarbonQueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  var timestampFormat: String = _

  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    SparkUtil4Test.createTaskMockUp(sqlContext)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  }

  override protected def beforeEach(): Unit = {
    sql("drop table if exists mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |     mytime TIMESTAMP,
        |     name STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
  }

  val timeSeries = "TIMESERIES"

  test("test timeseries match 1: select small one when create big_agg and then create small_agg") {

    dropDataMaps("maintable", "big_agg", "small_agg")
    sql(
      s"""
         | CREATE DATAMAP big_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age), max(age), min(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df1 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")

    sql(
      s"""
         | CREATE DATAMAP small_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df2 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df2.queryExecution.analyzed, "maintable_small_agg")
  }

  test("test timeseries match 2: select small one when create small_agg and then create big_agg") {
    dropDataMaps("maintable", "big_agg", "small_agg")

    sql(
      s"""
         | CREATE DATAMAP small_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df2 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))
    preAggTableValidator(df2.queryExecution.analyzed, "maintable_small_agg")

    sql(
      s"""
         | CREATE DATAMAP big_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age), max(age), min(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df1 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    intercept[Exception]{
      preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")
    }
    preAggTableValidator(df1.queryExecution.analyzed, "maintable_small_agg")
  }

  test("test timeseries match 3: select small one when create big_agg and then create small_agg") {

    dropDataMaps("maintable", "big_agg", "small_agg")
    sql(
      s"""
         | CREATE DATAMAP big_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age), max(age), min(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df1 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")

    sql(
      s"""
         | CREATE DATAMAP small_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df2 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df2.queryExecution.analyzed, "maintable_small_agg")
    intercept[Exception] {
      preAggTableValidator(df1.queryExecution.analyzed, "maintable_small_agg")
    }
    preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")
  }

  test("test timeseries match 4: select small one when create big_agg, small_agg, and middle_agg") {

    dropDataMaps("maintable", "big_agg", "small_agg", "middle_agg")
    sql(
      s"""
         | CREATE DATAMAP big_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age), max(age), min(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df1 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")

    sql(
      s"""
         | CREATE DATAMAP small_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df2 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df2.queryExecution.analyzed, "maintable_small_agg")
    intercept[Exception] {
      preAggTableValidator(df1.queryExecution.analyzed, "maintable_small_agg")
    }
    preAggTableValidator(df1.queryExecution.analyzed, "maintable_big_agg")

    sql(
      s"""
         | CREATE DATAMAP middle_agg ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df3 = sql(
      """
        | SELECT
        |   timeseries(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |     timeseries(mytime,'minute')<'2016-02-23 09:02:00' and
        |     timeseries(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   timeseries(mytime,'minute')
        | ORDER BY
        |   timeseries(mytime,'minute')
      """.stripMargin)

    preAggTableValidator(df3.queryExecution.analyzed, "maintable_small_agg")
  }

  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit ={
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is FROM create preaTable1regate table class so no need to transform
      // the query plan
      case ca:CarbonRelation =>
        if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        ca
      case logicalRelation:LogicalRelation =>
        if(logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        logicalRelation
    }
    if(!isValidPlan) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override def afterAll: Unit = {
    dropDataMaps("maintable", "agg0_second", "agg0_hour", "agg0_day", "agg0_month", "agg0_year")
    sql("drop table if exists mainTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
  }
}
