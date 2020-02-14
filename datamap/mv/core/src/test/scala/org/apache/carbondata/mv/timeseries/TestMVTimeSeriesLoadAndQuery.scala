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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.mv.rewrite.TestUtil

class TestMVTimeSeriesLoadAndQuery extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    drop()
    createTable()
  }

  test("create MV timeseries materialized view with simple projection and aggregation and filter") {
    sql("drop materialized view if exists datamap1")
    sql("drop materialized view if exists datamap2")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    loadData("maintable")
    val df = sql("select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable group by timeseries(projectjoindate,'minute')")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'minute'), sum(projectcode) from maintable where timeseries(projectjoindate,'minute') = '2016-02-23 09:17:00' group by timeseries(projectjoindate,'minute')")

    sql("select * from datamap1_table").show(false)
    val df1 = sql("select timeseries(projectjoindate,'minute'),sum(projectcode) from maintable where timeseries(projectjoindate,'minute') = '2016-02-23 09:17:00'" +
                  "group by timeseries(projectjoindate,'minute')")
    assert(TestUtil.verifyMVDataMap(df1.queryExecution.optimizedPlan, "datamap1"))
    val df2 = sql("select timeseries(projectjoindate,'MINUTE'), sum(projectcode) from maintable where timeseries(projectjoindate,'MINute') = '2016-02-23 09:17:00' group by timeseries(projectjoindate,'MINUTE')")
    TestUtil.verifyMVDataMap(df2.queryExecution.optimizedPlan, "datamap1")
    dropDataMap("datamap1")
  }

  test("test mv timeseries with ctas and filter in actual query") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    loadData("maintable")
    val df = sql("select timeseries(projectjoindate,'hour'), sum(projectcode) from maintable where timeseries(projectjoindate,'hour') = '2016-02-23 09:00:00' " +
                 "group by timeseries(projectjoindate,'hour')")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    dropDataMap("datamap1")
  }

  test("test mv timeseries with multiple granularity datamaps") {
    dropDataMap("datamap1")
    dropDataMap("datamap2")
    dropDataMap("datamap3")
    dropDataMap("datamap4")
    dropDataMap("datamap5")
    dropDataMap("datamap6")
    loadData("maintable")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'minute'), sum(salary) from maintable group by timeseries(projectjoindate,'minute')")
    sql(
      "create materialized view datamap2 as " +
      "select timeseries(projectjoindate,'hour'), sum(salary) from maintable group by timeseries(projectjoindate,'hour')")
    sql(
      "create materialized view datamap3 as " +
      "select timeseries(projectjoindate,'fifteen_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'fifteen_minute')")
    sql(
      "create materialized view datamap4 as " +
      "select timeseries(projectjoindate,'five_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'five_minute')")
    sql(
      "create materialized view datamap5 as " +
      "select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    sql(
      "create materialized view datamap6 as " +
      "select timeseries(projectjoindate,'year'), sum(salary) from maintable group by timeseries(projectjoindate,'year')")
    val df1 = sql("select timeseries(projectjoindate,'minute'), sum(salary) from maintable group by timeseries(projectjoindate,'minute')")
    checkPlan("datamap1", df1)
    val df2 = sql("select timeseries(projectjoindate,'hour'), sum(salary) from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("datamap2", df2)
    val df3 = sql("select timeseries(projectjoindate,'fifteen_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'fifteen_minute')")
    checkPlan("datamap3", df3)
    val df4 = sql("select timeseries(projectjoindate,'five_minute'), sum(salary) from maintable group by timeseries(projectjoindate,'five_minute')")
    checkPlan("datamap4", df4)
    val df5 = sql("select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    checkPlan("datamap5", df5)
    val df6 = sql("select timeseries(projectjoindate,'year'), sum(salary) from maintable group by timeseries(projectjoindate,'year')")
    checkPlan("datamap6", df6)
    val result = sql("show materialized views on table maintable").collect()
    result.find(_.get(0).toString.contains("datamap1")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(0).toString.contains("datamap2")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(0).toString.contains("datamap3")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(0).toString.contains("datamap4")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(0).toString.contains("datamap5")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    result.find(_.get(0).toString.contains("datamap6")) match {
      case Some(row) => assert(row.get(5).toString.contains("ENABLED"))
      case None => assert(false)
    }
    dropDataMap("datamap1")
    dropDataMap("datamap2")
    dropDataMap("datamap3")
    dropDataMap("datamap4")
    dropDataMap("datamap5")
    dropDataMap("datamap6")
  }

  test("test mv timeseries with week granular select data") {
    dropDataMap("datamap1")
    loadData("maintable")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
/*
    +-----------------------------------+----------+
    |UDF:timeseries_projectjoindate_week|sum_salary|
    +-----------------------------------+----------+
    |2016-02-21 00:00:00                |3801      |
    |2016-03-20 00:00:00                |400.2     |
    |2016-04-17 00:00:00                |350.0     |
    |2016-03-27 00:00:00                |150.6     |
    +-----------------------------------+----------+*/
    val df1 = sql("select timeseries(projectjoindate,'week'), sum(salary) from maintable group by timeseries(projectjoindate,'week')")
    checkPlan("datamap1", df1)
    checkExistence(df1, true, "2016-02-21 00:00:00.0" )
    dropDataMap("datamap1")
  }

  test("test timeseries with different aggregations") {
    dropDataMap("datamap1")
    dropDataMap("datamap2")
    loadData("maintable")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'hour'), avg(salary), max(salary) from maintable group by timeseries(projectjoindate,'hour')")
    sql(
      "create materialized view datamap2 as " +
      "select timeseries(projectjoindate,'day'), count(projectcode), min(salary) from maintable group by timeseries(projectjoindate,'day')")
    val df1 = sql("select timeseries(projectjoindate,'hour'), avg(salary), max(salary) from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("datamap1", df1)
    val df2 = sql("select timeseries(projectjoindate,'day'), count(projectcode), min(salary) from maintable group by timeseries(projectjoindate,'day')")
    checkPlan("datamap2", df2)
    dropDataMap("datamap1")
    dropDataMap("datamap2")
  }

  test("test timeseries with and and or filters") {
    dropDataMap("datamap1")
    dropDataMap("datamap2")
    dropDataMap("datamap3")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    loadData("maintable")
    var df1 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df1)
    df1 = sql("select timeseries(projectjoindate,'MONth'), max(salary) from maintable where timeseries(projectjoindate,'MoNtH') = '2016-03-01 00:00:00' or  timeseries(projectjoinDATE,'MONth') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'MONth')")
    TestUtil.verifyMVDataMap(df1.queryExecution.optimizedPlan, "datamap1")
    sql(
      "create materialized view datamap2 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    val df2 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap2", df2)
    sql(
      "create materialized view datamap3 as " +
      "select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-04-01 00:00:00'  group by timeseries(projectjoindate,'month')")
    val df3 = sql("select timeseries(projectjoindate,'month'), max(salary) from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' and  timeseries(projectjoindate,'month') = '2016-02-01 00:00:00' or  timeseries(projectjoindate,'month') = '2016-04-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap3", df3)
    dropDataMap("datamap1")
    dropDataMap("datamap2")
    dropDataMap("datamap3")
  }

  test("test timeseries with simple projection of time aggregation and between filter") {
    dropDataMap("datamap1")
    loadData("maintable")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') from maintable group by timeseries(projectjoindate,'month')")
    val df1 = sql("select timeseries(projectjoindate,'month') from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df1)
    val df2 = sql("select timeseries(projectjoindate,'month') from maintable where timeseries(projectjoindate,'month') = '2016-03-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df2)
    val df3 = sql("select timeseries(projectjoindate,'month') from maintable where timeseries(projectjoindate,'month') between '2016-03-01 00:00:00' and '2016-04-01 00:00:00' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df3)
    dropDataMap("datamap1")
  }

  test("test mv timeseries with dictinct, cast") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    loadData("maintable")
    val df1 = sql("select distinct(projectcode) from maintable")
    checkPlan("datamap1", df1)
    val df2 = sql("select distinct(timeseries(projectjoindate,'month')) from maintable")
    checkPlan("datamap1", df2)
    // TODO: cast expression and group by not allowing to create datamap, check later
//    sql(
//      "create materialized view datamap2 as " +
//      "select timeseries(projectjoindate,'month'),cast(floor((projectcode + 1000) / 900) * 900 - 2000 AS INT) from maintable group by timeseries(projectjoindate,'month'),projectcode")
//    val df3 = sql("select timeseries(projectjoindate,'month'),cast(floor((projectcode + 1000) / 900) * 900 - 2000 AS INT) from maintable group by timeseries(projectjoindate,'month'),projectcode")
//    checkPlan("datamap2", df3)
    dropDataMap("datamap1")
  }

  test("test mvtimeseries with alias") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("datamap1", df1)
    // TODO: fix the base issue of alias with group by
    //   val df2 = sql("select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    //   checkPlan("datamap1", df2)
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month'),projectcode from maintable group by timeseries(projectjoindate,'month'),projectcode")
    val df3 = sql("select timeseries(projectjoindate,'month') as t,projectcode as y from maintable group by timeseries(projectjoindate,'month'),projectcode")
    checkPlan("datamap1", df3)
    dropDataMap("datamap1")
  }

  test("test mv timeseries with case when and Sum + Sum") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') ,sum(CASE WHEN projectcode=5 THEN salary ELSE 0 END)  from maintable group by timeseries(projectjoindate,'month')")
    val df = sql("select timeseries(projectjoindate,'month') ,sum(CASE WHEN projectcode=5 THEN salary ELSE 0 END)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df)
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'hour') ,sum(projectcode) + sum(salary)  from maintable group by timeseries(projectjoindate,'hour')")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'hour') ,sum(projectcode) + sum(salary)  from maintable group by timeseries(projectjoindate,'hour')")
    checkPlan("datamap1", df1)
    dropDataMap("datamap1")
  }

  test("test mv timeseries with IN filter subquery") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'hour') ,sum(projectcode)  from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select max(salary) from maintable where projectcode IN (select sum(projectcode)  from maintable group by timeseries(projectjoindate,'hour')) ")
    checkPlan("datamap1", df)
    dropDataMap("datamap1")
  }

  test("test mv timeseries duplicate columns and constant columns") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') ,sum(projectcode),sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    loadData("maintable")
    val df1 = sql("select timeseries(projectjoindate,'month') ,sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df1)
    val df2 = sql("select timeseries(projectjoindate,'month') ,sum(projectcode),sum(projectcode)  from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df2)
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') ,sum(1) ex  from maintable group by timeseries(projectjoindate,'month')")
    val df3 = sql("select timeseries(projectjoindate,'month') ,sum(1)  ex from maintable group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df3)
    dropDataMap("datamap1")
  }

  test("test mv timeseries with like filters") {
    dropDataMap("datamap1")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(projectjoindate,'month') ,sum(salary) from maintable  where salary NOT LIKE '6%' group by timeseries(projectjoindate,'month')")
    val df1 = sql("select timeseries(projectjoindate,'month') ,sum(salary) from maintable  where salary NOT LIKE '6%' group by timeseries(projectjoindate,'month')")
    checkPlan("datamap1", df1)
    dropDataMap("datamap1")
  }

  test("test mv timeseries with join scenario") {
    sql("drop table if exists secondtable")
    sql(
      "CREATE TABLE secondtable (empno int,empname string, projectcode int, projectjoindate " +
      "Timestamp,salary double) STORED AS carbondata")
    loadData("secondtable")
    sql(
      "create materialized view datamap1 as " +
      "select timeseries(t1.projectjoindate,'month'), sum(t1.projectcode), sum(t2.projectcode) " +
      " from maintable t1 inner join secondtable t2 where" +
      " t2.projectcode = t1.projectcode group by timeseries(t1.projectjoindate,'month')")
    val df =  sql(
      "select timeseries(t1.projectjoindate,'month'), sum(t1.projectcode), sum(t2.projectcode)" +
      " from maintable t1 inner join secondtable t2 where" +
      " t2.projectcode = t1.projectcode group by timeseries(t1.projectjoindate,'month')")
    checkPlan("datamap1", df)
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

  def dropDataMap(datamapName: String): Unit = {
    sql(s"drop materialized view if exists $datamapName")
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

  def checkPlan(dataMapName: String, df: DataFrame): Unit = {
    val analyzed = df.queryExecution.optimizedPlan
    assert(TestUtil.verifyMVDataMap(analyzed, dataMapName))
  }
}
