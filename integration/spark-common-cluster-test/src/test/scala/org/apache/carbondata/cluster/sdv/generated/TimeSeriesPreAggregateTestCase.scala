
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

package org.apache.carbondata.cluster.sdv.generated

import java.util.TimeZone

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.util.CarbonProperties

class TimeSeriesPreAggregateTestCase extends QueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString
  val timeZonePre = TimeZone.getDefault
  val csvPath = s"$resourcesPath/Data/timeseriestest.csv"
  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    TimeZone.setDefault(TimeZone.getTimeZone(System.getProperty("user.timezone")))
    sql("drop table if exists mainTable")
    sql(
      "CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache" +
      ".carbondata.format'")
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
         | 'year_granularity'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$csvPath' into table mainTable")
  }

  test("TimeSeriesPreAggregateTestCase_001") {
    val expected = sql("select cast(date_format(mytime, 'YYYY') as timestamp) as mytime,sum(age) " +
                       "from mainTable group by date_format(mytime , 'YYYY')")
    val actual = sql("select * from maintable_agg0_year")
    checkAnswer(actual, expected)
  }

  test("TimeSeriesPreAggregateTestCase_002") {
    val expected = sql(
      "select cast(date_format(mytime, 'YYYY-MM') as timestamp) as mytime,sum(age) " +
      "from mainTable group by date_format(mytime , 'YYYY-MM')")
    val actual = sql("select * from maintable_agg0_month")
    checkAnswer(actual, expected)
  }

  test("TimeSeriesPreAggregateTestCase_003") {
    val expected = sql(
      "select cast(date_format(mytime, 'YYYY-MM-dd') as timestamp) as mytime,sum(age) " +
      "from mainTable group by date_format(mytime , 'YYYY-MM-dd')")
    val actual = sql("select * from maintable_agg0_day")
    checkAnswer(actual, expected)
  }

  test("TimeSeriesPreAggregateTestCase_004") {
    val expected = sql(
      "select cast(date_format(mytime, 'YYYY-MM-dd HH') as timestamp) as mytime,sum(age) " +
      "from mainTable group by date_format(mytime , 'YYYY-MM-dd HH')")
    val actual = sql("select * from maintable_agg0_hour")
    checkAnswer(actual, expected)
  }

  test("TimeSeriesPreAggregateTestCase_005") {
    val expected = sql(
      "select cast(date_format(mytime, 'YYYY-MM-dd HH:mm') as timestamp) as mytime,sum(age) " +
      "from mainTable group by date_format(mytime , 'YYYY-MM-dd HH:mm')")
    val actual = sql("select * from maintable_agg0_minute")
    checkAnswer(actual, expected)
  }

  test("TimeSeriesPreAggregateTestCase_006") {
    val expected = sql(
      "select cast(date_format(mytime, 'YYYY-MM-dd HH:mm:ss') as timestamp) as mytime,sum(age) " +
      "from mainTable group by date_format(mytime , 'YYYY-MM-dd HH:mm:ss')")
    val actual = sql("select * from maintable_agg0_second")
    checkAnswer(actual, expected)
  }

  //test case for compaction
  test("TimeSeriesPreAggregateTestCase_007") {
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$csvPath' into table mainTable")
    sql("alter table maintable compact 'minor'")
    val segmentNamesSecond = sql("show segments for table maintable_agg0_second").collect()
      .map(_.get(0).toString)
    segmentNamesSecond should equal(Array("3", "2", "1", "0.1", "0"))

    val segmentNamesMinute = sql("show segments for table maintable_agg0_minute").collect()
      .map(_.get(0).toString)
    segmentNamesMinute should equal(Array("3", "2", "1", "0.1", "0"))

    val segmentNamesHour = sql("show segments for table maintable_agg0_hour").collect()
      .map(_.get(0).toString)
    segmentNamesHour should equal(Array("3", "2", "1", "0.1", "0"))

    val segmentNamesday = sql("show segments for table maintable_agg0_day").collect()
      .map(_.get(0).toString)
    segmentNamesday should equal(Array("3", "2", "1", "0.1", "0"))

    val segmentNamesmonth = sql("show segments for table maintable_agg0_month").collect()
      .map(_.get(0).toString)
    segmentNamesmonth should equal(Array("3", "2", "1", "0.1", "0"))

    val segmentNamesyear = sql("show segments for table maintable_agg0_year").collect()
      .map(_.get(0).toString)
    segmentNamesyear should equal(Array("3", "2", "1", "0.1", "0"))
  }

  override def afterAll: Unit = {
    TimeZone.setDefault(timeZonePre)
    sql("drop table if exists mainTable")

  }
}
