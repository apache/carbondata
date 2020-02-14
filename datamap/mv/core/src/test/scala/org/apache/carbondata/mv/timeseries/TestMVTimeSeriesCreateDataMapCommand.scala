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

package org.apache.carbondata.mv.timeseries

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.rewrite.TestUtil

class TestMVTimeSeriesCreateDataMapCommand extends QueryTest with BeforeAndAfterAll {

  private val timestampFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    drop()
    sql("CREATE TABLE maintable (empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        "deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int, utilization int,salary int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE maintable  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }

  def drop(): Unit = {
    sql("drop table if exists products")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv_timeseries create materialized view") {
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 " +
      " as select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    val result = sql("show materialized views on table maintable").collectAsList()
    assert(result.get(0).get(0).toString.equalsIgnoreCase("datamap1"))
    assert(result.get(0).get(5).toString.equalsIgnoreCase("ENABLED"))
    val df = sql("select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    sql("drop materialized view if exists datamap1")
  }

  test("test mv_timeseries create lazy materialized view") {
    sql("drop materialized view if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 with deferred refresh as " +
        "select timeseries(projectjoindate,'second') from maintable group by timeseries(projectjoindate,'second')")
    }.getMessage.contains("MV TimeSeries queries does not support Lazy Rebuild")
  }

  test("test mv_timeseries create materialized view with multiple granularity") {
    sql("drop materialized view if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'second'), timeseries(projectjoindate,'hour') from maintable")
    }.getMessage.contains("Multiple timeseries udf functions are defined in Select statement with different granularities")
  }

  test("test mv_timeseries create materialized view with date type as timeseries_column") {
    sql("drop table IF EXISTS maintable_new")
    sql("CREATE TABLE maintable_new (projectcode int, projectjoindate date, projectenddate Timestamp,attendance int) " +
        "STORED AS carbondata")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'day') from maintable_new")
    val result = sql("show materialized views on table maintable_new").collectAsList()
    assert(result.get(0).get(0).toString.equalsIgnoreCase("datamap1"))
    assert(result.get(0).get(5).toString.equalsIgnoreCase("ENABLED"))
    sql("drop table IF EXISTS maintable_new")
  }

  test("test mv_timeseries create materialized view with date type as timeseries_column with incorrect granularity") {
    sql("drop table IF EXISTS maintable_new")
    sql("CREATE TABLE maintable_new (projectcode int, projectjoindate date, projectenddate Timestamp,attendance int) " +
        "STORED AS carbondata")
    sql("drop materialized view if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'second') from maintable_new")
    }.getMessage.contains("Granularity should be of DAY/WEEK/MONTH/YEAR, for timeseries column of Date type")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'five_minute') from maintable_new")
    }.getMessage.contains("Granularity should be of DAY/WEEK/MONTH/YEAR, for timeseries column of Date type")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'hour') from maintable_new")
      }.getMessage.contains("Granularity should be of DAY/WEEK/MONTH/YEAR, for timeseries column of Date type")
    sql("drop table IF EXISTS maintable_new")
  }

  test("test mv_timeseries for same event_column with different granularities") {
    def dropDataMaps = {
      sql("drop materialized view if exists datamap1")
      sql("drop materialized view if exists datamap2")
      sql("drop materialized view if exists datamap3")
      sql("drop materialized view if exists datamap4")
      sql("drop materialized view if exists datamap5")
    }
    dropDataMaps
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql(
      "create materialized view datamap2 as " +
      "select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    sql(
      "create materialized view datamap3 as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    sql(
      "create materialized view datamap4 as " +
      "select timeseries(projectjoindate,'day'), sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    sql(
      "create materialized view datamap5 as " +
      "select timeseries(projectjoindate,'year'), sum(projectcode) from maintable group by timeseries(projectjoindate,'year')")
    dropDataMaps
  }

  test("test mv_timeseries create materialized view with more event_columns") {
    sql("drop materialized view if exists datamap1")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'hour'), timeseries(projectenddate,'hour') from maintable")
    }.getMessage.contains(
        "Multiple timeseries udf functions are defined in Select statement with different timestamp columns")
  }

  test("test mv_timeseries create materialized view with same granularity and different ctas") {
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql("drop materialized view if exists datamap2")
    sql(
      "create materialized view datamap2 as " +
      "select timeseries(projectjoindate,'second'), sum(projectcode) from maintable where projectjoindate='29-06-2008 00:00:00.0' " +
      "group by timeseries(projectjoindate,'second')")
    sql("drop materialized view if exists datamap1")
    sql("drop materialized view if exists datamap2")
  }

  test("insert and create materialized view in progress") {
    sql("drop materialized view if exists datamap1")
    val query = s"LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE maintable  " +
                s"OPTIONS('DELIMITER'= ',')"
    val executorService = Executors.newFixedThreadPool(4)
    executorService.submit(new QueryTask(query))
    intercept[UnsupportedOperationException] {
      sql(
        "create materialized view datamap1 as " +
        "select timeseries(projectjoindate,'year'), sum(projectcode) from maintable group by timeseries(projectjoindate,'year')")
    }.getMessage
      .contains("Cannot create mv materialized view table when insert is in progress on parent table: maintable")
    executorService.shutdown()
    executorService.awaitTermination(2, TimeUnit.HOURS)
    sql("drop materialized view if exists datamap1")
  }

  test("test create materialized view with incorrect timeseries_column and granularity") {
    sql("drop materialized view if exists datamap2")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap2 as " +
        "select timeseries(projectjoindate,'time'), sum(projectcode) from maintable group by timeseries(projectjoindate,'time')")
    }.getMessage.contains("Granularity time is invalid")
    intercept[MalformedCarbonCommandException] {
      sql(
        "create materialized view datamap2 as " +
        "select timeseries(empname,'second'), sum(projectcode) from maintable group by timeseries(empname,'second')")
    }.getMessage.contains("MV Timeseries is only supported on Timestamp/Date column")
  }

  test("test timeseries with case sensitive granularity") {
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1 " +
      " as select timeseries(projectjoindate,'Second'), sum(projectcode) from maintable group by timeseries(projectjoindate,'Second')")
    val df1 = sql("select timeseries(projectjoindate,'SECOND'), sum(projectcode) from maintable group by timeseries(projectjoindate,'SECOND')")
    val df2 = sql("select timeseries(projectjoinDATE,'SECOnd'), sum(projectcode) from maintable where projectcode=8 group by timeseries(projectjoinDATE,'SECOnd')")
    TestUtil.verifyMVDataMap(df1.queryExecution.optimizedPlan, "datamap1")
    TestUtil.verifyMVDataMap(df2.queryExecution.optimizedPlan, "datamap1")
    sql("drop materialized view if exists datamap1")
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        sql(query).collect()
      } catch {
        case exception: Exception => LOGGER.error(exception.getMessage)
          result = "FAIL"
      }
      result
    }
  }

  override def afterAll(): Unit = {
    drop()
    if (null != timestampFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
    }
  }
}
