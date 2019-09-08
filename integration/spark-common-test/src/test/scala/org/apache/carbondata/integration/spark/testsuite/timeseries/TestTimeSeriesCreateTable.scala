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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeSeriesCreateTable extends CarbonQueryTest with BeforeAndAfterAll with BeforeAndAfterEach{

  val timeSeries = TIMESERIES.toString
  var timestampFormat: String = _

  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS mainTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
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

  override def afterEach(): Unit = {
    dropDataMaps("mainTable", "agg1_second", "agg1_minute",
      "agg1_hour", "agg1_day", "agg1_month", "agg1_year")
  }

  test("test timeseries create table 1") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_second"), true, "maintable_agg0_second")
    sql("DROP DATAMAP agg0_second ON TABLE mainTable")
  }

  test("test timeseries create table 2") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_hour"), true, "maintable_agg0_hour")
    sql("DROP DATAMAP agg0_hour ON TABLE mainTable")
  }

  test("test timeseries create table 3") {
    checkExistence(sql("DESCRIBE FORMATTED maintable_agg0_day"), true, "maintable_agg0_day")
    sql("DROP DATAMAP agg0_day ON TABLE mainTable")
  }

  test("test timeseries create table 4") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_month"), true, "maintable_agg0_month")
    sql("DROP DATAMAP agg0_month ON TABLE mainTable")
  }

  test("test timeseries create table 5") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_year"), true, "maintable_agg0_year")
    sql("DROP DATAMAP agg0_year ON TABLE mainTable")
  }

  test("test timeseries create table 6: TIMESERIES should define time granularity") {
    sql("DROP DATAMAP IF EXISTS agg0_second ON TABLE mainTable")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""CREATE DATAMAP agg0_second ON TABLE mainTable USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'SEC_GRANULARITY'='1')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains("TIMESERIES should define time granularity"))
  }

  test("test timeseries create table 7: Granularity only support 1") {
    sql("DROP DATAMAP IF EXISTS agg0_second ON TABLE mainTable")
    val e = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'DAY_GRANULARITY'='1.5')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Granularity only support 1"))
  }

  test("test timeseries create table 8: Granularity only support 1") {
    dropDataMaps("mainTable", "agg1_hour")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""CREATE DATAMAP agg1_hour ON TABLE mainTable USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'HOUR_GRANULARITY'='hour=-2')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support "))
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "maintable_agg1_hour")
  }

  test("test timeseries create table 9: SECOND_GRANULARITY is null") {
    sql("DROP DATAMAP IF EXISTS agg1 ON TABLE mainTable")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""CREATE DATAMAP agg0_hour ON TABLE mainTable
           |USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'HOUR_GRANULARITY'='')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
  }

  test("test timeseries create table 10: Table already exists in database") {
    val e = intercept[MalformedDataMapCommandException] {
      sql(
        s"""CREATE DATAMAP agg1_hour ON TABLE mainTable USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'HOUR_GRANULARITY'='1')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
        """.stripMargin)
      sql(
        s"""CREATE DATAMAP agg1_hour ON TABLE mainTable USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'HOUR_GRANULARITY'='1')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains(
      "DataMap name 'agg1_hour' already exist"))
  }

  test("test timeseries create table 11: don't support create timeseries table on non timestamp") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='name',
           | 'HOUR_GRANULARITY'='1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals("Timeseries event time is only supported on Timestamp column"))
  }

  test("test timeseries create table 12: Time series column dataTime does not exists in select") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'HOUR_GRANULARITY'='1')
           | AS SELECT name, SUM(age) FROM mainTable
           | GROUP BY name
         """.stripMargin)
    }
    assert(e.getMessage.equals("Time series column dataTime does not exists in select"))
  }

  test("test timeseries create table 13: don't support create timeseries table on non timestamp") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""CREATE DATAMAP agg0_hour ON TABLE mainTable
           |USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='name',
           |   'HOUR_GRANULARITY'='1')
           |AS SELECT name, SUM(age) FROM mainTable
           |GROUP BY name
        """.stripMargin)
    }
    assert(e.getMessage.contains("Timeseries event time is only supported on Timestamp column"))
  }

  test("test timeseries create table 14: USING") {
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        """CREATE DATAMAP agg0_hour ON TABLE mainTable
          | USING 'abc'
          | DMPROPERTIES (
          |   'EVENT_TIME'='dataTime',
          |   'HOUR_GRANULARITY'='1')
          | AS SELECT dataTime, SUM(age) FROM mainTable
          | GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.equals("DataMap 'abc' not found"))
  }

  test("test timeseries create table 15: USING and catch MalformedCarbonCommandException") {
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        """CREATE DATAMAP agg0_hour ON TABLE mainTable
          | USING 'abc'
          | DMPROPERTIES (
          |   'EVENT_TIME'='dataTime',
          |   'HOUR_GRANULARITY'='1')
          | AS SELECT dataTime, SUM(age) FROM mainTable
          | GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.equals("DataMap 'abc' not found"))
  }

  test("test timeseries create table 16: Only one granularity level can be defined 1") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
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
    e.printStackTrace()
    assert(e.getMessage.equals("Only one granularity level can be defined"))
  }

  test("test timeseries create table 17: Only one granularity level can be defined 2") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
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

  test("test timeseries create table 18: Only one granularity level can be defined 3") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
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

  test("test timeseries create table 19: timeSeries should define time granularity") {
    sql("DROP DATAMAP IF EXISTS agg0_hour ON TABLE mainTable")
    val e = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_hour ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.equals(s"$timeSeries should define time granularity"))
  }

  test("test timeseries create table 20: should support if not exists, create when same table exists") {
    sql("DROP DATAMAP IF EXISTS agg1 ON TABLE mainTable")

    sql(
      s"""
         | CREATE DATAMAP agg1 ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='dataTime',
         |   'MONTH_GRANULARITY'='1')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
        """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP IF NOT EXISTS agg1 ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='dataTime',
         |   'MONTH_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "agg1")
    checkExistence(sql("DESC FORMATTED mainTable_agg1"), true, "maintable_age_sum")
  }

  test("test timeseries create table 32: should support if not exists, create when same table not exists") {
    sql("DROP DATAMAP IF EXISTS agg1_year ON TABLE mainTable")
    sql(
      s"""
         |CREATE DATAMAP if not exists agg1_year ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'YEAR_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "agg1_year")
    checkExistence(sql("DESC FORMATTED mainTable_agg1_year"), true, "maintable_age_sum")
  }

  test("test timeseries create table 20: don't support 'create datamap if exists'") {
    val e: Exception = intercept[AnalysisException] {
      sql(
        s"""CREATE DATAMAP IF EXISTS agg2 ON TABLE mainTable
          | USING '$timeSeries'
          | DMPROPERTIES (
          |   'EVENT_TIME'='dataTime',
          |   'MONTH_GRANULARITY'='1')
          | AS SELECT dataTime, SUM(age) FROM mainTable
          | GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains("identifier matching regex"))
  }

  test("test timeseries create table 26: test different data type") {
    sql("drop table if exists dataTable")
    sql(
      s"""
         | CREATE TABLE dataTable(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | decimalField DECIMAL(18,2),
         | charField CHAR(5),
         | floatField FLOAT,
         | dataTime timestamp
         | )
         | STORED BY 'carbondata'
       """.stripMargin)


    sql(
      s"""CREATE DATAMAP agg0_hour ON TABLE dataTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'HOUR_GRANULARITY'='1')
         | AS SELECT
         |   dataTime,
         |   SUM(intField),
         |   shortField,
         |   booleanField,
         |   intField,
         |   bigintField,
         |   doubleField,
         |   stringField,
         |   decimalField,
         |   charField,
         |   floatField
         | FROM dataTable
         | GROUP BY
         |   dataTime,
         |   shortField,
         |   booleanField,
         |   intField,
         |   bigintField,
         |   doubleField,
         |   stringField,
         |   decimalField,
         |   charField,
         |   floatField
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE dataTable"), true, "datatable_agg0_hour")
    sql("DROP TABLE IF EXISTS dataTable")
  }

  test("test timeseries create table 27: test data map name") {
    sql(
      s"""CREATE DATAMAP agg1_hour ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'HOUR_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "agg1_hour")
    checkExistence(sql("DESC FORMATTED mainTable_agg1_hour"), true, "maintable_age_sum")
  }

  test("test timeseries create table 28: event_time is null") {
    sql("DROP DATAMAP IF EXISTS agg1 ON TABLE mainTable")
    intercept[NullPointerException] {
      sql(
        s"""CREATE DATAMAP agg1 ON TABLE mainTable
           |USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='',
           |   'HOUR_GRANULARITY'='1')
           |AS SELECT name, SUM(age) FROM mainTable
           |GROUP BY name
        """.stripMargin)
    }
  }

  test("test timeseries create table 29: table not exists") {
    sql("DROP DATAMAP IF EXISTS agg1 ON TABLE mainTable")
    val e = intercept[AnalysisException] {
      sql(
        s"""CREATE DATAMAP agg1 ON TABLE mainTable
           |USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='dataTime',
           |   'HOUR_GRANULARITY'='1')
           |AS SELECT dataTime, SUM(age) FROM mainTableNo
           |GROUP BY dataTime
        """.stripMargin)
    }
    assert(e.getMessage.contains("Table or view not found: mainTableNo"))
  }

  test("test timeseries create table 33: support event_time and granularity key with space") {
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
    sql(
      s"""CREATE DATAMAP agg1_month ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   ' event_time '='dataTime',
         |   ' MONTH_GRANULARITY '='1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), true, "maintable_agg1_month")
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
  }


  test("test timeseries create table 34: support event_time value with space") {
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
    sql(
      s"""CREATE DATAMAP agg1_month ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time '=' dataTime',
         |   'MONTH_GRANULARITY '='1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), true, "maintable_agg1_month")
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
  }

  test("test timeseries create table 35: support granularity value with space") {
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
    sql(
      s"""CREATE DATAMAP agg1_month ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time '='dataTime',
         |   'MONTH_GRANULARITY '=' 1')
         |AS SELECT dataTime, SUM(age) FROM mainTable
         |GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), true, "maintable_agg1_month")
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
  }

  test("test timeseries create table 36: support event_time and granularity value with space") {
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
    sql(
      s"""
         | CREATE DATAMAP agg1_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='dataTime   ',
         |   'MONTH_GRANULARITY'=' 1  ')
         | AS SELECT dataTime, SUM(age) FROM mainTable
         | GROUP BY dataTime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), true, "maintable_agg1_month")
  }

  test("test timeseries create table 37: unsupport event_time error value") {
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
    intercept[NullPointerException] {
      sql(
        s"""CREATE DATAMAP agg1_month ON TABLE mainTable USING '$timeSeries'
           |DMPROPERTIES (
           |   'event_time'='data Time',
           |   'MONTH_GRANULARITY'='1')
           |AS SELECT dataTime, SUM(age) FROM mainTable
           |GROUP BY dataTime
         """.stripMargin)
    }
    sql("DROP DATAMAP IF EXISTS agg1_month ON TABLE maintable")
  }

  override def afterAll: Unit = {
    dropTable("mainTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
  }
}
