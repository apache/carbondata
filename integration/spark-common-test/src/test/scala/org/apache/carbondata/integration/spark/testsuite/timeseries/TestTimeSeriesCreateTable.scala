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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.schema.table.DataMapClassName.{PREAGGREGATE, TIMESERIES}
import org.apache.carbondata.spark.exception.{MalformedCarbonCommandException, UnsupportedDataMapException}

class TestTimeSeriesCreateTable extends QueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.getName

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(dataTime timestamp, name string, city string, age int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""
         | create datamap agg0_second on table mainTable
         | using '$timeSeries'
         | DMPROPERTIES (
         | 'event_time'='dataTime',
         | 'second_granularity'='1')
         | as select dataTime, sum(age) from mainTable
         | group by dataTime
       """.stripMargin)
    sql(
      s"""
         | create datamap agg0_hour on table mainTable
         | using '$timeSeries'
         | DMPROPERTIES (
         | 'event_time'='dataTime',
         | 'hour_granularity'='1')
         | as select dataTime, sum(age) from mainTable
         | group by dataTime
       """.stripMargin)
    sql(
      s"""
         | create datamap agg0_day on table mainTable
         | using '$timeSeries'
         | DMPROPERTIES (
         | 'event_time'='dataTime',
         | 'day_granularity'='1')
         | as select dataTime, sum(age) from mainTable
         | group by dataTime
       """.stripMargin)
    sql(
      s"""
         | create datamap agg0_month on table mainTable
         | using '$timeSeries'
         | DMPROPERTIES (
         | 'event_time'='dataTime',
         | 'month_granularity'='1')
         | as select dataTime, sum(age) from mainTable
         | group by dataTime
       """.stripMargin)
    sql(
      s"""
         | create datamap agg0_year on table mainTable
         | using '$timeSeries'
         | DMPROPERTIES (
         | 'event_time'='dataTime',
         | 'year_granularity'='1')
         | as select dataTime, sum(age) from mainTable
         | group by dataTime
       """.stripMargin)
  }

  test("test timeseries create table Zero") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_second"), true, "maintable_agg0_second")
    sql("drop datamap agg0_second on table mainTable")
  }

  test("test timeseries create table One") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_hour"), true, "maintable_agg0_hour")
    sql("drop datamap agg0_hour on table mainTable")
  }
  test("test timeseries create table two") {
    checkExistence(sql("DESCRIBE FORMATTED maintable_agg0_day"), true, "maintable_agg0_day")
    sql("drop datamap agg0_day on table mainTable")
  }
  test("test timeseries create table three") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_month"), true, "maintable_agg0_month")
    sql("drop datamap agg0_month on table mainTable")
  }
  test("test timeseries create table four") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_year"), true, "maintable_agg0_year")
    sql("drop datamap agg0_year on table mainTable")
  }

  test("test timeseries create table five") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'sec_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
        """.stripMargin)
    }
  }

  test("test timeseries create table Six") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'second_granularity'='2')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table seven") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'second_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'second_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table Eight") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='name',
           | 'second_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table Nine") {
    intercept[Exception] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='name',
           | 'second_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table: using") {
    val e: Exception = intercept[UnsupportedDataMapException] {
      sql(
        """create datamap agg1 on table mainTable
          | using 'abc'
          | DMPROPERTIES (
          |   'event_time'='dataTime',
          |   'second_granularity'='1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains(
      s"Don't support using abc to create datamap, please use $PREAGGREGATE or $TIMESERIES"))
  }

  test("test timeseries create table: using and catch MalformedCarbonCommandException") {
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        """create datamap agg1 on table mainTable
          | using 'abc'
          | DMPROPERTIES (
          |   'event_time'='dataTime',
          |   'second_granularity'='1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains(
      s"Don't support using abc to create datamap, please use $PREAGGREGATE or $TIMESERIES"))
  }

  test("test timeseries create table: granularity only support one 1") {
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'second_granularity'='1',
           | 'hour_granularity'='1',
           | 'day_granularity'='1',
           | 'month_granularity'='1',
           | 'year_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support one"))
  }

  test("test timeseries create table: granularity only support one 2") {
    val e: Exception = intercept[UnsupportedDataMapException] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'second_granularity'='1',
           | 'hour_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support one"))
  }

  test("test timeseries create table: granularity only support one 3") {
    val e: Exception = intercept[UnsupportedDataMapException] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'day_granularity'='1',
           | 'hour_granularity'='1')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support one"))
  }

  test("test timeseries create table: Granularity only support 1") {
    val e = intercept[UnsupportedDataMapException] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'day_granularity'='2')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
  }

  test("test timeseries create table: Granularity only support 1 and throw Exception") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | create datamap agg0_second on table mainTable
           | using '$timeSeries'
           | DMPROPERTIES (
           | 'event_time'='dataTime',
           | 'hour_granularity'='2')
           | as select dataTime, sum(age) from mainTable
           | group by dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}
