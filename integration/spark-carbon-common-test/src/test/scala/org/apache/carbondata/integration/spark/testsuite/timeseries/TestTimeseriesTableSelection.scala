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
package org.apache.carbondata.integration.spark.testsuite.timeseries.timeseries

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeseriesTableSelection extends CarbonQueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString
  var timestampFormat: String = _

  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    SparkUtil4Test.createTaskMockUp(sqlContext)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists mainTable")
    sql(
      """
        | CREATE TABLE mainTable(mytime timestamp, name string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'SECOND_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_minute ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'minute_granularity'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_hour ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'HOUR_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_day ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'DAY_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'MONTH_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_year ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'YEAR_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
  }

  test("test timeseries table selection 1") {
    val df = sql("SELECT mytime FROM mainTable GROUP BY mytime")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test timeseries table selection 2") {
    val df = sql("SELECT TIMESERIES(mytime,'hour') FROM mainTable " +
                 "GROUP BY TIMESERIES(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test timeseries table selection 3: No enum constant MILLI") {
    val e = intercept[Exception] {
      val df = sql(
        """
          | SELECT TIMESERIES(mytime,'milli')
          | FROM mainTable
          | GROUP BY TIMESERIES(mytime,'milli')
        """.stripMargin)
      preAggTableValidator(df.queryExecution.analyzed, "maintable")
      df.show()
    }
    assert(e.getMessage.contains(
      "No enum constant org.apache.carbondata.core.preagg.TimeSeriesFunctionEnum.MILLI"))
  }

  test("test timeseries table selection 4") {
    val df = sql("SELECT TIMESERIES(mytime,'year') FROM mainTable GROUP BY TIMESERIES(mytime,'year')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_year")
  }

  test("test timeseries table selection 5") {
    val df = sql("SELECT TIMESERIES(mytime,'day') FROM mainTable GROUP BY TIMESERIES(mytime,'day')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_day")
  }

  test("test timeseries table selection 6") {
    val df = sql("SELECT TIMESERIES(mytime,'month') FROM mainTable GROUP BY TIMESERIES(mytime,'month')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_month")
  }

  test("test timeseries table selection 7") {
    val df = sql("SELECT TIMESERIES(mytime,'minute') FROM mainTable GROUP BY TIMESERIES(mytime,'minute')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_minute")
  }

  test("test timeseries table selection 8") {
    val df = sql("SELECT TIMESERIES(mytime,'second') FROM mainTable GROUP BY TIMESERIES(mytime,'second')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_second")
  }

  test("test timeseries table selection 9") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour')
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='x'
        | GROUP BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test timeseries table selection 10") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour')
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='x'
        | GROUP BY TIMESERIES(mytime,'hour')
        | ORDER BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test timeseries table selection 11") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour'),SUM(age)
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='x'
        | GROUP BY TIMESERIES(mytime,'hour')
        | ORDER BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test timeseries table selection 12") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour') AS hourlevel,SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='x'
        | GROUP BY TIMESERIES(mytime,'hour')
        | ORDER BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test timeseries table selection 13") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour')as hourlevel,SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='x' AND name='vishal'
        | GROUP BY TIMESERIES(mytime,'hour')
        | ORDER BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test timeseries table selection 14: TIMESERIES(mytime,'hour') match") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour')
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='2016-02-23 09:00:00'
        | GROUP BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
    checkAnswer(df, Row(Timestamp.valueOf("2016-02-23 09:00:00.0")))
  }

  test("test timeseries table selection 15: TIMESERIES(mytime,'hour') not match") {
    val df = sql(
      """
        | SELECT TIMESERIES(mytime,'hour')
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'hour')='2016-02-23 09:01:00'
        | GROUP BY TIMESERIES(mytime,'hour')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
    checkExistence(df, false, "2016-02-23 09:00:00", "2016-02-23 09:01:00")
  }

  test("test timeseries table selection 16: TIMESERIES(mytime,'minute') match") {
    checkExistence(sql("SELECT * FROM mainTable"), true,
      "2016-02-23 09:01:30", "2016-02-23 09:02:40")
    checkExistence(sql("SELECT * FROM mainTable"), false,
      "2016-02-23 09:02:00", "2016-02-23 09:01:00")
    val df = sql(
      """
        |SELECT TIMESERIES(mytime,'minute') 
        |FROM mainTable 
        |GROUP BY TIMESERIES(mytime,'minute')
      """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
    checkExistence(df, true, "2016-02-23 09:02:00", "2016-02-23 09:01:00")
    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 09:02:00.0")),
        Row(Timestamp.valueOf("2016-02-23 09:01:00.0"))))

    val df2 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute')as minutelevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')='2016-02-23 09:01:00'
        | GROUP BY TIMESERIES(mytime,'minute')
        | ORDER BY TIMESERIES(mytime,'minute')
      """.stripMargin)
    preAggTableValidator(df2.queryExecution.analyzed, "maintable_agg0_minute")
    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))
  }

  test("test timeseries table selection 17: TIMESERIES(mytime,'minute') not match pre agg") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute')as minutelevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')='2016-02-23 09:01:00' AND name='vishal'
        | GROUP BY TIMESERIES(mytime,'minute')
        | ORDER BY TIMESERIES(mytime,'minute')
      """.stripMargin)
    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 10)))
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test timeseries table selection 18: select with many GROUP BY AND one filter") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'year') AS yearLevel,
        |   TIMESERIES(mytime,'month') AS monthLevel,
        |   TIMESERIES(mytime,'day') AS dayLevel,
        |   TIMESERIES(mytime,'hour') AS hourLevel,
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   TIMESERIES(mytime,'second') AS secondLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'year'),
        |   TIMESERIES(mytime,'month'),
        |   TIMESERIES(mytime,'day'),
        |   TIMESERIES(mytime,'hour'),
        |   TIMESERIES(mytime,'minute'),
        |   TIMESERIES(mytime,'second')
        | ORDER BY
        |   TIMESERIES(mytime,'year'),
        |   TIMESERIES(mytime,'month'),
        |   TIMESERIES(mytime,'day'),
        |   TIMESERIES(mytime,'hour'),
        |   TIMESERIES(mytime,'minute'),
        |   TIMESERIES(mytime,'second')
      """.stripMargin)

    checkExistence(df, true,
      "2016-01-01 00:00:00",
      "2016-02-01 00:00:00",
      "2016-02-23 09:00:00",
      "2016-02-23 09:01:00",
      "2016-02-23 09:01:50",
      "30"
    )
  }

  test("test timeseries table selection 19: select with many GROUP BY AND many filter") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'year') AS yearLevel,
        |   TIMESERIES(mytime,'month') AS monthLevel,
        |   TIMESERIES(mytime,'day') AS dayLevel,
        |   TIMESERIES(mytime,'hour') AS hourLevel,
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   TIMESERIES(mytime,'second') AS secondLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |   TIMESERIES(mytime,'second')='2016-02-23 09:01:50' and
        |   TIMESERIES(mytime,'minute')='2016-02-23 09:01:00' and
        |   TIMESERIES(mytime,'hour')='2016-02-23 09:00:00' and
        |   TIMESERIES(mytime,'month')='2016-02-01 00:00:00' and
        |   TIMESERIES(mytime,'year')='2016-01-01 00:00:00'
        | GROUP BY
        |   TIMESERIES(mytime,'year'),
        |   TIMESERIES(mytime,'month'),
        |   TIMESERIES(mytime,'day'),
        |   TIMESERIES(mytime,'hour'),
        |   TIMESERIES(mytime,'minute'),
        |   TIMESERIES(mytime,'second')
        | ORDER BY
        |   TIMESERIES(mytime,'year'),
        |   TIMESERIES(mytime,'month'),
        |   TIMESERIES(mytime,'day'),
        |   TIMESERIES(mytime,'hour'),
        |   TIMESERIES(mytime,'minute'),
        |   TIMESERIES(mytime,'second')
      """.stripMargin)

    checkExistence(df, true,
      "2016-01-01 00:00:00",
      "2016-02-01 00:00:00",
      "2016-02-23 09:00:00",
      "2016-02-23 09:01:00",
      "2016-02-23 09:01:50",
      "30"
    )
  }

  test("test timeseries table selection 21: filter < AND >") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:04:00'
        |   AND TIMESERIES(mytime,'minute')>'2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test timeseries table selection 22: filter <= AND >=") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<='2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test timeseries table selection 23: filter < AND >=") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test timeseries table selection 24: filter < AND >=") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:01:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq.empty)

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test timeseries table selection 25: filter many column") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE
        |   TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00' and
        |   TIMESERIES(mytime,'hour')>='2016-02-23 09:00:00' and
        |   name='vishal'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 10)))
  }

  test("test timeseries table selection 26: filter < AND >=, avg") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   avg(age) AS avg
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 20.0)))

  }

  test("test timeseries table selection 27: filter < AND >=, max") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'second') AS secondLevel,
        |   max(age) AS maxValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'second')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'second')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'second')
        | ORDER BY
        |   TIMESERIES(mytime,'second')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:30"), 10),
      Row(Timestamp.valueOf("2016-02-23 09:01:40"), 20),
      Row(Timestamp.valueOf("2016-02-23 09:01:50"), 30)))
  }

  test("test timeseries table selection 28: filter < AND >=, min") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'second') AS secondLevel,
        |   min(age) AS minValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'second')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'second')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'second')
        | ORDER BY
        |   TIMESERIES(mytime,'second')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:30"), 10),
      Row(Timestamp.valueOf("2016-02-23 09:01:40"), 20),
      Row(Timestamp.valueOf("2016-02-23 09:01:50"), 30)))
  }

  test("test timeseries table selection 29: count, max, min, sum") {
    dropDataMaps("maintable", "agg1_second", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age), count(age), max(age), min(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   count(age) AS count
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 3)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1_minute")

    val df1 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   max(age) AS maxValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 30)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_agg1_minute")

    val df2 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   min(age) AS minValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 10)))

    preAggTableValidator(df2.queryExecution.analyzed, "maintable_agg1_minute")

    dropDataMaps("maintable", "agg1_second", "agg1_minute")

    val df4 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUM
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df4, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))
  }

  test("test timeseries table selection 30: max, no create") {
    val df1 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   max(age) AS maxValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 30)))
  }

  test("test timeseries table selection 31: min, no create") {
    val df1 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   min(age) AS minValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 10)))
  }

  test("test timeseries table selection 32: filter < AND >=, min") {
    val df = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'second') AS secondLevel,
        |   min(age) AS minValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'second')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'second')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'second')
        | ORDER BY
        |   TIMESERIES(mytime,'second')
      """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:30"), 10),
      Row(Timestamp.valueOf("2016-02-23 09:01:40"), 20),
      Row(Timestamp.valueOf("2016-02-23 09:01:50"), 30)))
  }

  test("test timeseries table selection 33: max") {
    dropDataMaps("maintable", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
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
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   max(age) AS maxValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 30)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_agg1_minute")
  }

  test("test timeseries table selection 34: min") {
    dropDataMaps("maintable", "agg1_second", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
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
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   min(age) AS minValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 10)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_agg1_minute")
  }

  test("test timeseries table selection 35: sum") {
    dropDataMaps("maintable", "agg1_second", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
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
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   SUM(age) AS SUMValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 60)))
  }

  test("test timeseries table selection 36: count") {
    dropDataMaps("maintable", "agg1_second", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
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
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   count(age) AS countValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 3)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_agg1_minute")

  }

  test("test timeseries table selection 37: avg") {
    dropDataMaps("maintable", "agg1_second", "agg1_minute")
    sql(
      s"""
         | CREATE DATAMAP agg1_minute ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         | AS SELECT mytime, avg(age)
         | FROM mainTable
         | GROUP BY mytime
      """.stripMargin)

    val df1 = sql(
      """
        | SELECT
        |   TIMESERIES(mytime,'minute') AS minuteLevel,
        |   avg(age) AS avgValue
        | FROM mainTable
        | WHERE TIMESERIES(mytime,'minute')<'2016-02-23 09:02:00'
        |   AND TIMESERIES(mytime,'minute')>='2016-02-23 09:01:00'
        | GROUP BY
        |   TIMESERIES(mytime,'minute')
        | ORDER BY
        |   TIMESERIES(mytime,'minute')
      """.stripMargin)

    checkAnswer(df1, Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00"), 20)))

    preAggTableValidator(df1.queryExecution.analyzed, "maintable_agg1_minute")
  }

  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit ={
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is FROM create preaTable1regate table class so no need to transform the query plan
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
