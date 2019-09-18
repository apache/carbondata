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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeSeriesUnsupportedSuite extends CarbonQueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val timeSeries = "TIMESERIES"
  var timestampFormat: String = _

  override def beforeAll: Unit = {
    timestampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  override def beforeEach(): Unit = {
    sql("drop table if exists mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |   mytime TIMESTAMP,
        |   name STRING,
        |   age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  test("test timeseries unsupported 1: don't support insert") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
    val e = intercept[UnsupportedOperationException] {
      sql(s"INSERT INTO maintable_agg1_minute VALUES('2016-02-23 09:01:00.0', 60)")
    }
    assert(e.getMessage.equalsIgnoreCase(
      "Cannot insert/load data directly into pre-aggregate/child table"))

    // check value after inserting
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
  }

  test("test timeseries unsupported 2: don't support insert") {
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")
    val e = intercept[UnsupportedOperationException] {
      sql(s"INSERT INTO maintable_agg1_minute VALUES('2016-02-23 09:01:00.0', 60)")
    }
    assert(e.getMessage.equalsIgnoreCase(
      "Cannot insert/load data directly into pre-aggregate/child table"))
  }

  test("test timeseries unsupported 3: don't support insert") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT
         |    mytime,
         |    name,
         |    SUM(age)
         |from mainTable
         |GROUP BY mytime, name
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")

    val e = intercept[UnsupportedOperationException] {
      sql(s"INSERT INTO maintable_agg1_minute VALUES('2016-02-23 09:01:00.0', 'hello', 60)")
    }
    assert(e.getMessage.equalsIgnoreCase(
      "Cannot insert/load data directly into pre-aggregate/child table"))
  }

  test("test timeseries unsupported 4: don't support load") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT
         |    mytime,
         |    name,
         |    SUM(age) AS age
         |from mainTable
         |GROUP BY mytime, name
        """.stripMargin)
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")

    val e = intercept[UnsupportedOperationException] {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv'
           | INTO TABLE maintable_agg1_minute
           | OPTIONS('FILEHEADER'='maintable_mytime,maintable_name,maintable_age_SUM')
         """.stripMargin)
    }
    assert(e.getMessage.equalsIgnoreCase(
      "Cannot insert/load data directly into pre-aggregate/child table"))
  }

  test("test timeseries unsupported 5: don't support update") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
    val e = intercept[Exception] {
      sql("update maintable_agg1_minute SET (maintable_age_SUM) = (maintable_age_SUM+1)").show()
    }
    assert(e.getMessage.equalsIgnoreCase(
      "Update operation is not supported for pre-aggregate table"))

    // check value after inserting
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
  }

  test("test timeseries unsupported 6: don't support update") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
    val e = intercept[Exception] {
      sql(
        """
          | update maintable_agg1_minute
          | SET (maintable_mytime, maintable_age_SUM)=('2016-02-23 09:11:00.0', 160)
          | WHERE maintable_age_SUM = '60'
        """.stripMargin).show
    }
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
    assert(e.getMessage.equalsIgnoreCase(
      "Update operation is not supported for pre-aggregate table"))
  }

  test("test timeseries unsupported 7: don't support delete") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
    val e = intercept[UnsupportedOperationException] {
      sql("delete FROM maintable_agg1_minute")
    }

    assert(e.getMessage.equalsIgnoreCase(
      "Delete operation is not supported for pre-aggregate table"))

    // check value after inserting
    checkAnswer(sql("SELECT * FROM maintable_agg1_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 09:01:00.0"), 60),
        Row(Timestamp.valueOf("2016-02-23 09:02:00.0"), 140)))
  }

  test("test timeseries unsupported 8: don't support alter") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' INTO TABLE mainTable")
    sql(
      s"""CREATE DATAMAP agg1_minute ON TABLE mainTable
         |USING '$timeSeries'
         |DMPROPERTIES (
         |   'event_time'='mytime',
         |   'MINUTE_GRANULARITY'='1')
         |AS SELECT mytime, SUM(age) FROM mainTable
         |GROUP BY mytime
        """.stripMargin)
    // before alter
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")

    // alter
    val e = intercept[Exception] {
      sql("alter table maintable_agg1_minute rename to maintable_agg1_minute_new")
    }
    assert(e.getMessage.contains(
      "Rename operation for datamaps is not supported."))

    // check datamap after alter
    checkExistence(sql("SHOW DATAMAP ON TABLE mainTable"), true, "maintable_agg1_minute")
  }

  override def afterAll: Unit = {
    dropTable("mainTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
  }
}
