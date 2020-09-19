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

package org.apache.carbondata.view.timeseries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.view.rewrite.TestUtil

class TestMVTimeSeriesLoadAndQuery extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll(): Unit = {
    drop()
    createTable()
  }

  test("create MV timeseries materialized view with simple projection and aggregation and filter") {
    sql("drop materialized view if exists mv1")
    sql("drop materialized view if exists mv2")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    loadData("maintable")
    val df = sql("select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable where timeseries(projectjoindate,'minute') = '2016-02-23 09:17:00' group by timeseries(projectjoindate,'minute')")

    sql("select * from mv1").collect()
    val df1 = sql("select timeseries(projectjoindate,'minute'),sum(projectcode) from maintable where timeseries(projectjoindate,'minute') = '2016-02-23 09:17:00'" +
                  "group by timeseries(projectjoindate,'minute')")
    assert(TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mv1"))
    val df2 = sql("select timeseries(projectjoindate,'MINUTE'), sum(projectcode) from maintable where timeseries(projectjoindate,'MINute') = '2016-02-23 09:17:00' group by timeseries(projectjoindate,'MINUTE')")
    TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "mv1")
    dropMV("mv1")
  }

  test("test mv timeseries with ctas and filter in actual query") {
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    loadData("maintable")
    val df = sql("select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable where timeseries(projectjoindate,'hour') = '2016-02-23 09:00:00' " +
                 "group by timeseries(projectjoindate,'hour')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    dropMV("mv1")
  }

  test("test mv timeseries with multiple granularity mvs") {
    dropMV("mv1")
    dropMV("mv2")
    dropMV("mv3")
    dropMV("mv4")
    dropMV("mv5")
    dropMV("mv6")
    loadData("maintable")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'minute'), sum(salary) from maintable group by timeseries(projectjoindate,'minute')")
    sql(
      "create materialized view mv2 as " +
      "select timeseries(projectjoindate,'hour'), sum(salary) from maintable group by timeseries(projectjoindate,'hour')")
    sql(
      "create materialized view mv3 as " +
      "select timeseries(projectjoindate,'fifteen_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'fifteen_minute')")
    sql(
      "create materialized view mv4 as " +
      "select timeseries(projectjoindate,'five_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'five_minute')")
    sql(
      "create materialized view mv5 as " +
      "select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    sql(
      "create materialized view mv6 as " +
      "select timeseries(projectjoindate,'year'), sum(salary) from maintable group by timeseries(projectjoindate,'year')")
    val df1 = sql("select timeseries(projectjoindate,'minute'), sum(salary) from maintable group by timeseries(projectjoindate,'minute')")
    checkPlan("mv1", df1)
    val df2 = sql("select timeseries(projectjoindate,'hour'), sum(salary) from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("mv2", df2)
    val df3 = sql("select timeseries(projectjoindate,'fifteen_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'fifteen_minute')")
    checkPlan("mv3", df3)
    val df4 = sql("select timeseries(projectjoindate,'five_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'five_minute')")
    checkPlan("mv4", df4)
    val df5 = sql("select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    checkPlan("mv5", df5)
    val df6 = sql("select timeseries(projectjoindate,'year'), sum(salary) from maintable group by timeseries(projectjoindate,'year')")
    checkPlan("mv6", df6)
    val result = sql("show materialized views on table maintable").collect()
    result.find(_.get(1).toString.contains("mv1")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(1).toString.contains("mv2")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(1).toString.contains("mv3")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(1).toString.contains("mv4")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(1).toString.contains("mv5")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(1).toString.contains("mv6")) match {
      case Some(row) => assert(row.get(2).toString.contains("ENABLED"))
      case None => assert(false)
    }
    dropMV("mv1")
    dropMV("mv2")
    dropMV("mv3")
    dropMV("mv4")
    dropMV("mv5")
    dropMV("mv6")
  }

  test("test mv timeseries with week granular select data") {
    dropMV("mv1")
    loadData("maintable")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
   /*
    * +-----------------------------------+----------+
    * |UDF:timeseries_projectjoindate_week|sum_salary|
    * +-----------------------------------+----------+
    * |2016-02-21 00:00:00                |3801      |
    * |2016-03-20 00:00:00                |400.2     |
    * |2016-04-17 00:00:00                |350.0     |
    * |2016-03-27 00:00:00                |150.6     |
    * +-----------------------------------+----------+
    */
    val df1 = sql("select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    checkPlan("mv1", df1)
    checkExistence(df1, true, "2016-02-21 00:00:00.0" )
    dropMV("mv1")
  }

  test("test timeseries with different aggregations") {
    dropMV("mv1")
    dropMV("mv2")
    loadData("maintable")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'hour'), avg(salary), max(salary) from maintable group by timeseries(projectjoindate,'hour')")
    sql(
      "create materialized view mv2 as " +
      "select timeseries(projectjoindate,'day'), count(projectcode), min(salary) from maintable group by timeseries(projectjoindate,'day')")
    val df1 = sql("select timeseries(projectjoindate,'hour'), avg(salary), max(salary) from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("mv1", df1)
    val df2 = sql("select timeseries(projectjoindate,'day'), count(projectcode), min(salary) from maintable group by timeseries(projectjoindate,'day')")
    checkPlan("mv2", df2)
    dropMV("mv1")
    dropMV("mv2")
  }

  test("test timeseries with and and or filters") {
    dropMV("mv1")
    dropMV("mv2")
    dropMV("mv3")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    loadData("maintable")
    var df1 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df1)
    df1 = sql("select timeseries(projectjoindate,'MONth'), max(salary) from maintable where timeseries(projectjoindate,'MoNtH') = '2016-03-01 00:00:00' or  timeseries(projectjoinDATE,'MONth') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'MONth')")
    TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mv1")
    sql(
      "create materialized view mv2 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    val df2 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("mv2", df2)
    sql(
      "create materialized view mv3 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-04-01 00:00:00'  group by timeseries(projectjoindate,'month')")
    val df3 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-04-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("mv3", df3)
    dropMV("mv1")
    dropMV("mv2")
    dropMV("mv3")
  }

  test("test timeseries with simple projection of time aggregation and between filter") {
    dropMV("mv1")
    loadData("maintable")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') from maintable group by timeseries(projectjoindate,'month')")
    val df1 = sql("select timeseries(projectjoindate,'month') from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df1)
    val df2 = sql("select timeseries(projectjoindate,'month') from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df2)
    val df3 = sql("select timeseries(projectjoindate,'month') from maintable where timeseries(projectjoindate,'month') between '2016-03-01 00:00:00' and '2016-04-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df3)
    dropMV("mv1")
  }

  test("test mv timeseries with dictinct, cast") {
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    loadData("maintable")
    val df1 = sql("select distinct(projectcode) from maintable")
    checkPlan("mv1", df1)
    val df2 = sql("select distinct(timeseries(projectjoindate,'month')) from maintable")
    checkPlan("mv1", df2)
    // TODO: cast expression and group by not allowing to create mv, check later
//    sql(
//      "create materialized view mv2 as " +
//      "select timeseries(projectjoindate,'month'),cast(floor((projectcode + 1000) / 900) * 900 - 2000 AS INT) from maintable group by timeseries(projectjoindate,'month'),projectcode")
//    val df3 = sql("select timeseries(projectjoindate,'month'),cast(floor((projectcode + 1000) / 900) * 900 - 2000 AS INT) from maintable group by timeseries(projectjoindate,'month'),projectcode")
//    checkPlan("mv2", df3)
    dropMV("mv1")
  }

  test("test mvtimeseries with alias") {
    val result = sql("select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("mv1", df1)
    val df2 = sql("select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("mv1", df2)
    checkAnswer(result, df2)
    val df4 = sql("select timeseries(projectjoindate,'month'),projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("mv1", df4)
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    val df3 = sql("select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("mv1", df3)
    dropMV("mv1")
  }

  test("test mv timeseries with case when and Sum + Sum") {
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') ,sum(CASE WHEN projectcode=5 THEN salary ELSE 0 END)  from maintable group by timeseries(projectjoindate,'month')")
    val df = sql("select timeseries(projectjoindate,'month') ,sum(CASE WHEN projectcode=5 THEN salary ELSE 0 END)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df)
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'hour') ,sum(projectcode) + sum(salary)  from maintable group by timeseries(projectjoindate,'hour')")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'hour') ,sum(projectcode) + sum(salary)  from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("mv1", df1)
    dropMV("mv1")
  }

  test("test mv timeseries with IN filter subquery") {
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'hour') ,sum(projectcode)  from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select max(salary) from maintable where projectcode IN (select sum(projectcode)  from maintable group by timeseries(projectjoindate,'hour')) ")
    checkPlan("mv1", df)
    dropMV("mv1")
  }

  test("test mv timeseries duplicate columns and constant columns") {
    // new optimized insert into flow doesn't support duplicate column names, so send it to old flow
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    dropMV("mv1")
    intercept[MalformedMVCommandException] {
      sql(
        "create materialized view mv1 as " +
          "select timeseries(projectjoindate,'month') ,sum(projectcode),sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    }
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') ,sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'month') ,sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df1)
    val df2 = sql("select timeseries(projectjoindate,'month') ,sum(projectcode),sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df2)
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') ,sum(1) ex  from maintable group by timeseries(projectjoindate,'month')")
    val df3 = sql("select timeseries(projectjoindate,'month') ,sum(1)  ex from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df3)
    dropMV("mv1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")
  }

  test("test mv timeseries with like filters") {
    dropMV("mv1")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(projectjoindate,'month') ,sum(salary) from maintable  where salary NOT LIKE '6%' group by timeseries(projectjoindate,'month')")
    val df1 = sql("select timeseries(projectjoindate,'month') ,sum(salary) from maintable  where salary NOT LIKE '6%' group by timeseries(projectjoindate,'month')")
    checkPlan("mv1", df1)
    dropMV("mv1")
  }

  test("test mv timeseries with join scenario") {
    // this test case mv table is created with distinct column (2 columns),
    // but insert projection has duplicate column(3 columns). Cannot support in new insert into flow
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("drop table if exists secondtable")
    sql(
      "CREATE TABLE secondtable (empno int,empname string, projectcode int, projectjoindate " +
      "Timestamp,salary double) STORED AS carbondata")
    loadData("secondtable")
    sql(
      "create materialized view mv1 as " +
      "select timeseries(t1.projectjoindate,'month'), sum(t1.projectcode), sum(t2.projectcode) " +
      " from maintable t1 inner join secondtable t2 where" +
      " t2.projectcode = t1.projectcode group by timeseries(t1.projectjoindate,'month')")
    val df = sql("select timeseries(t1.projectjoindate,'month'), sum(t1.projectcode), sum(t2.projectcode)" +
      " from maintable t1 inner join secondtable t2 where" +
      " t2.projectcode = t1.projectcode group by timeseries(t1.projectjoindate,'month')")
    checkPlan("mv1", df)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")
  }

  test("test create materialized view with group by columns not present in projection") {
    sql("drop materialized view if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm as select timeseries(projectjoindate,'day') from maintable where empname='chandler' group by timeseries(projectjoindate,'day'),empname")
    }.getMessage.contains("Group by columns must be present in project columns")
    sql("create materialized view dm as select timeseries(projectjoindate,'day'),empname from maintable where empname='chandler' group by timeseries(projectjoindate,'day'),empname")
    var df = sql("select timeseries(projectjoindate,'day'),empname from maintable where empname='chandler' group by timeseries(projectjoindate,'day'),empname")
    checkPlan("dm", df)
    df = sql("select timeseries(projectjoindate,'day') from maintable where empname='chandler' group by timeseries(projectjoindate,'day'),empname")
    checkPlan("dm", df)
    sql("drop materialized view if exists dm ")
  }

  override def afterAll(): Unit = {
    drop()
  }

  def drop(): Unit = {
    sql("drop table if exists maintable")
  }

  def dropMV(viewName: String): Unit = {
    sql(s"drop materialized view if exists $viewName")
  }

  def createTable(): Unit = {
    sql(
      "CREATE TABLE maintable (empno int,empname string, projectcode int, projectjoindate " +
      "Timestamp,salary double) STORED AS carbondata")
  }

  def loadData(table: String): Unit = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/mv_sampledata.csv' INTO TABLE $table  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }

  def checkPlan(mvName: String, df: DataFrame): Unit = {
    val analyzed = df.queryExecution.optimizedPlan
    assert(TestUtil.verifyMVHit(analyzed, mvName))
  }
  // scalastyle:on lineLength
}
