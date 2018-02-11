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

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES

class TestTimeSeriesCreateTable extends QueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("CREATE TABLE mainTable(dataTime timestamp, name string, city string, age int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""
         | CREATE DATAMAP agg0_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='dataTime',
         | 'SECOND_GRANULARITY'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_hour ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='dataTime',
         | 'HOUR_GRANULARITY'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_day ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='dataTime',
         | 'day_granularity'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='dataTime',
         | 'month_granularity'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg0_year ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='dataTime',
         | 'year_granularity'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
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
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SEC_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
        """.stripMargin)
    }
  }

  test("test timeseries create table Six") {
    intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SECOND_GRANULARITY'='2')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table seven") {
    intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SECOND_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SECOND_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table Eight") {
    intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='name',
           | 'SECOND_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table Nine") {
    intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='name',
           | 'SECOND_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
  }

  test("test timeseries create table: USING") {
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        """CREATE DATAMAP agg1 ON TABLE mainTable
          | USING 'abc'
          | DMPROPERTIES (
          |   'EVENT_TIME'='dataTime',
          |   'SECOND_GRANULARITY'='1')
          | AS SELECT dataTime, SUM(age) FROM mainTable
          | GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.equals("Unknown data map type abc"))
  }

  test("test timeseries create table: USING and catch MalformedCarbonCommandException") {
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        """CREATE DATAMAP agg1 ON TABLE mainTable
          | USING 'abc'
          | DMPROPERTIES (
          |   'EVENT_TIME'='dataTime',
          |   'SECOND_GRANULARITY'='1')
          | AS SELECT dataTime, SUM(age) FROM mainTable
          | GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.equals("Unknown data map type abc"))
  }

  test("test timeseries create table: Only one granularity level can be defined 1") {
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SECOND_GRANULARITY'='1',
           | 'HOUR_GRANULARITY'='1',
           | 'DAY_GRANULARITY'='1',
           | 'MONTH_GRANULARITY'='1',
           | 'YEAR_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Only one granularity level can be defined"))
  }

  test("test timeseries create table: Only one granularity level can be defined 2") {
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'SECOND_GRANULARITY'='1',
           | 'HOUR_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Only one granularity level can be defined"))
  }

  test("test timeseries create table: Only one granularity level can be defined 3") {
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'DAY_GRANULARITY'='1',
           | 'HOUR_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Only one granularity level can be defined"))
  }

  test("test timeseries create table: Granularity only support 1") {
    val e = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'DAY_GRANULARITY'='2')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Granularity only support 1"))
  }

  test("test timeseries create table: Granularity only support 1 and throw Exception") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'HOUR_GRANULARITY'='2')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Granularity only support 1"))
  }

  test("test timeseries create table: timeSeries should define time granularity") {
    val e = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals(s"$timeSeries should define time granularity"))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
  }
}
