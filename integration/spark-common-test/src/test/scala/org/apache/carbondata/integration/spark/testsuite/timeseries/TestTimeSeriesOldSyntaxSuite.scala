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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeSeriesOldSyntaxSuite extends QueryTest with BeforeAndAfterEach {

  val timeSeries = TIMESERIES.toString

  override def beforeEach: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql(
      """
        | CREATE TABLE IF NOT EXISTS mainTable(
        | imei STRING,
        | age INT,
        | mac STRING ,
        | prodate TIMESTAMP,
        | update TIMESTAMP,
        | gamepoint DOUBLE,
        | contrid DOUBLE) 
        | stored by 'carbondata'
      """.stripMargin)
  }

  override def afterEach: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
  }

  test("test timeseries old syntax 1: don't support timeseries.eventTime") {
    val e = intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg2 ON table mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           |     'timeseries.eventTime'='prodate',
           |     'timeseries.hierarchy'='hour=1,day=1,month=1,year=1')
           | AS SELECT prodate, mac
           | FROM mainTable
           | GROUP BY prodate,mac
        """.stripMargin)
    }
    assert(e.getMessage.equals(
      "Don't support old syntax: timeseries.eventtime, please use event_time"))
  }

  test("test timeseries old syntax 2: don't support timeseries.eventTime") {
    val e = intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg2 ON table mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           |     'timeseries.eventTime'='prodate',
           |     'hour_granularity'='1')
           | AS SELECT prodate, mac
           | FROM mainTable
           | GROUP BY prodate,mac
        """.stripMargin)
    }
    assert(e.getMessage.equals(
      "Don't support old syntax: timeseries.eventtime, please use event_time"))
  }

  test("test timeseries old syntax 3: don't support timeseries.hierarchy") {
    val e = intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP agg2 ON table mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           |     'event_Time'='prodate',
           |     'timeseries.hierarchy'='hour=1,day=1,month=1,year=1')
           | AS SELECT prodate, mac
           | FROM mainTable
           | GROUP BY prodate,mac
        """.stripMargin)
    }
    assert(e.getMessage.equals(
      "Don't support old syntax: timeseries.hierarchy, please use different granularity"))
  }

  test("test timeseries old syntax 4:  support new syntax as contrast ") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,"false")
    sql(
      s"""
         | CREATE DATAMAP agg2_hour ON table mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |     'event_Time'='prodate',
         |     'hour_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
        """.stripMargin)

    checkExistence(sql("SHOW TABLES"), false, "maintable_agg2_hour")
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg2_hour")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS, CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
  }

  test("test timeseries old syntax 5: new show tables strategy as contrast") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,"false")
    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE mainTable
         | USING 'preaggregate'
         | AS SELECT mac,AVG(age)
         | FROM mainTable
         | GROUP BY mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'second_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_minute ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'minute_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_hour ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'hour_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_day ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'day_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'month_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP agg2_year ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES ('event_time'='prodate', 'year_granularity'='1')
         | AS SELECT prodate, mac
         | FROM mainTable
         | GROUP BY prodate,mac
       """.stripMargin)

    checkExistence(sql("SHOW TABLES"), false,
      "maintable_preagg_sum", "maintable_agg2_second", "maintable_agg2_minute", "maintable_agg2_day",
      "maintable_agg2_hour", "maintable_agg2_month", "maintable_agg2_year")

    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true,
      "maintable_preagg_sum", "maintable_agg2_second", "maintable_agg2_minute", "maintable_agg2_day",
      "maintable_agg2_hour", "maintable_agg2_month", "maintable_agg2_year")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS, CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT)
  }

}
