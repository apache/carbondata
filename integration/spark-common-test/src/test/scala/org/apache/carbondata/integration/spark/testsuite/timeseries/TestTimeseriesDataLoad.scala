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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES
import org.apache.carbondata.core.util.CarbonProperties

class TestTimeseriesDataLoad extends QueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString

  override def beforeAll: Unit = {
    SparkUtil4Test.createTaskMockUp(sqlContext)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists mainTable")
    sql("drop table if exists table_03")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
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

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
    sql("CREATE TABLE table_03 (imei string,age int,mac string,productdate timestamp,updatedate timestamp,gamePointId double,contractid double ) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA inpath '$resourcesPath/data_sort.csv' INTO table table_03 options ('DELIMITER'=',', 'QUOTECHAR'='','FILEHEADER'='imei,age,mac,productdate,updatedate,gamePointId,contractid')")

    sql(
      s"""
         | CREATE DATAMAP ag1_second ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |    'EVENT_TIME'='productdate',
         |    'SECOND_GRANULARITY'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ag1_minute ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |    'EVENT_TIME'='productdate',
         |    'minute_granularity'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ag1_hour ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='productdate',
         |    'HOUR_GRANULARITY'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ag1_day ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |    'EVENT_TIME'='productdate',
         |    'DAY_GRANULARITY'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ag1_month ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |    'EVENT_TIME'='productdate',
         |    'month_granularity'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ag1_year ON TABLE table_03
         | USING '$timeSeries'
         | DMPROPERTIES (
         |    'EVENT_TIME'='productdate',
         |    'year_granularity'='1')
         | AS SELECT productdate,mac,SUM(age) FROM table_03
         | GROUP BY productdate,mac
       """.stripMargin)

  }
  test("test Year level timeseries data validation1 ") {
    checkAnswer( sql("select count(*) from table_03_ag1_year"),
      Seq(Row(4)))
  }

  test("test month level timeseries data validation1 ") {
    checkAnswer( sql("select count(*) from table_03_ag1_month"),
      Seq(Row(4)))
  }

  test("test day level timeseries data validation1 ") {
    checkAnswer( sql("select count(*) from table_03_ag1_day"),
      Seq(Row(12)))
  }

  test("test Year level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_year"),
      Seq(Row(Timestamp.valueOf("2016-01-01 00:00:00.0"),200)))
  }

  test("test month level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_month"),
      Seq(Row(Timestamp.valueOf("2016-02-01 00:00:00.0"),200)))
  }

  test("test day level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_day"),
      Seq(Row(Timestamp.valueOf("2016-02-23 00:00:00.0"),200)))
  }

  test("test hour level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_hour"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:00:00.0"),200)))
  }

  test("test minute level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00.0"),60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00.0"),140)))
  }

  test("test second level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"),10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"),20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"),30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"),40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"),50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"),50)))
  }

  test("test if timeseries load is successful ON TABLE creation") {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
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
    checkAnswer( sql("select * FROM maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"),10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"),20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"),30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"),40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"),50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"),50)))
  }

  test("create datamap without 'if not exists' after load data into mainTable and create datamap") {
    sql("drop table if exists mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |   mytime timestamp,
        |   name string,
        |   age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")

    sql(
      s"""
         | CREATE DATAMAP agg0_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='mytime',
         |   'second_granularity'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
        """.stripMargin)

    checkAnswer(sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"), 10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"), 20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"), 30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"), 40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"), 50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"), 50)))
    val e: Exception = intercept[TableAlreadyExistsException] {
      sql(
        s"""
           | CREATE DATAMAP agg0_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           |   'EVENT_TIME'='mytime',
           |   'second_granularity'='1')
           | AS SELECT mytime, SUM(age) FROM mainTable
           | GROUP BY mytime
        """.stripMargin)
    }
    assert(e.getMessage.contains("already exists in database"))
  }

  test("create datamap with 'if not exists' after load data into mainTable and create datamap") {
    sql("drop table if exists mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |   mytime timestamp,
        |   name string,
        |   age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")

    sql(
      s"""
         | CREATE DATAMAP agg0_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='mytime',
         |   'second_granularity'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
        """.stripMargin)

    checkAnswer(sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"), 10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"), 20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"), 30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"), 40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"), 50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"), 50)))

    sql(
      s"""
         | CREATE DATAMAP IF NOT EXISTS  agg0_second ON TABLE mainTable
         | USING '$timeSeries'
         | DMPROPERTIES (
         |   'EVENT_TIME'='mytime',
         |   'second_granularity'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
        """.stripMargin)

    checkAnswer(sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"), 10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"), 20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"), 30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"), 40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"), 50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"), 50)))
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists table_03")
  }
}
