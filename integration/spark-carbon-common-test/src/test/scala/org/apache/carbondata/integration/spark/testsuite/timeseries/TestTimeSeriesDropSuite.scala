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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, NoSuchDataMapException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TestTimeSeriesDropSuite extends CarbonQueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val timeSeries = "timeseries"
  var timestampFormat: String = _

  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    dropTable("mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |   dataTime TIMESTAMP,
        |   name STRING,
        |   city STRING,
        |   age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  override def afterEach(): Unit = {
    dropDataMaps("mainTable", "agg1_second", "agg1_minute",
      "agg1_hour", "agg1_day", "agg1_month", "agg1_year")
  }

  test("test timeseries drop datamap 1: drop datamap should throw exception, maintable hasn't datamap") {
    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[NoSuchDataMapException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    }
    assert(e.getMessage.equals(
      "Datamap with name agg1_month does not exist"))
  }

  test("test timeseries drop datamap 2: should support drop datamap IF EXISTS, maintable hasn't datamap") {
    // DROP DATAMAP IF EXISTS DataMapName
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    sql(s"DROP DATAMAP IF EXISTS agg1_month ON TABLE mainTable")
    assert(true)
  }

  test("test timeseries drop datamap 3: should support drop datamap, maintable has datamap") {
    sql(
      s"""
         | CREATE DATAMAP agg1_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'MONTH_GRANULARITY'='1')
         | AS SELECT dataTime, SUM(age) from mainTable
         | GROUP BY dataTime
       """.stripMargin)

    // Before DROP DATAMAP
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "agg1_month")

    // DROP DATAMAP DataMapName
    sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[NoSuchDataMapException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    }
    assert(e.getMessage.equals(
      "Datamap with name agg1_month does not exist"))
  }

  test("test timeseries drop datamap 4: should support drop datamap with IF EXISTS, maintable has datamap") {
    sql(
      s"""
         | CREATE DATAMAP agg1_month ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'MONTH_GRANULARITY'='1')
         | AS SELECT dataTime, SUM(age) from mainTable
         | GROUP BY dataTime
       """.stripMargin)
    // DROP DATAMAP IF EXISTS DataMapName
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "agg1_month")

    // DROP DATAMAP DataMapName
    sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    }
    assert(e.getMessage.equals(
      "Datamap with name agg1_month does not exist"))
  }

  test("test timeseries drop datamap 5: drop datamap without IF EXISTS when table not exists, catch MalformedCarbonCommandException") {
    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found"))
  }

  test("test timeseries drop datamap 6: drop datamap with IF EXISTS when table not exists, catch MalformedCarbonCommandException") {
    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(s"DROP DATAMAP IF EXISTS agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found"))
  }

  test("test timeseries drop datamap 7: drop datamap should throw exception if table not exist, catch ProcessMetaDataException") {
    // DROP DATAMAP DataMapName if the DataMapName not exists and
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[ProcessMetaDataException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found "))
  }

  test("test timeseries drop datamap 8: should throw exception if table not exist with IF EXISTS, catch ProcessMetaDataException") {
    // DROP DATAMAP DataMapName if the DataMapName not exists
    // DROP DATAMAP should throw exception if table not exist, even though there is IF EXISTS"
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[ProcessMetaDataException] {
      sql(s"DROP DATAMAP IF EXISTS agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found "))
  }

  test("test timeseries drop datamap 9: drop datamap when table not exists, there are datamap in database") {
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'minute_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) from mainTable
         |GROUP BY dataTime
      """.stripMargin)

    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[ProcessMetaDataException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found"))
  }


  test("test timeseries drop datamap 10: drop datamap when table not exists, there are datamap in database") {
    sql(
      s"""CREATE DATAMAP agg3 ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'month_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) from mainTable
         |GROUP BY dataTime
      """.stripMargin)

    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[ProcessMetaDataException] {
      sql(s"DROP DATAMAP IF EXISTS agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains(
      "Dropping datamap agg1_month failed: Table or view 'maintablenotexists' not found"))
  }

  test("test timeseries drop datamap 11: drop datamap when table not exists, there are datamap in database") {
    sql(
      s"""
         |CREATE DATAMAP agg4 ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='dataTime',
         |   'month_GRANULARITY'='1')
         |AS SELECT dataTime, SUM(age) from mainTable
         |GROUP BY dataTime
      """.stripMargin)

    // DROP DATAMAP DataMapName if the DataMapName not exists
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), false, "agg1_month")
    val e: Exception = intercept[AnalysisException] {
      sql(s"DROP DATAMAP IF NOT EXISTS agg1_month ON TABLE mainTableNotExists")
    }
    assert(e.getMessage.contains("failure"))
  }

  override def afterAll: Unit = {
    dropTable("mainTable")
    dropTable("mainTableNotExists")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
  }
}
