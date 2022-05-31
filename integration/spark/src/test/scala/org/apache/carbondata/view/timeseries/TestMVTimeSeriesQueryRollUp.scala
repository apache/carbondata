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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.view.rewrite.TestUtil

class TestMVTimeSeriesQueryRollUp extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll(): Unit = {
    drop()
    createTable()
    loadData("maintable")
    sql("set carbon.enable.mv = true")
  }

  override def afterAll(): Unit = {
    drop()
    sql("set carbon.enable.mv = false")
  }

  test("test timeseries query rollup with simple projection") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),projectcode from maintable")
    val df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with group by - scenario-1") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable group by timeseries(projectjoindate,'day'),projectcode")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable group by timeseries(projectjoindate,'second'),projectcode")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),projectcode from maintable group by timeseries(projectjoindate,'hour'),projectcode")
    var df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable group by timeseries(projectjoindate,'day'),projectcode")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    df = sql("select timeseries(projectjoindate,'second'),projectcode from maintable group by timeseries(projectjoindate,'second'),projectcode")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    df = sql("select timeseries(projectjoindate,'second') from maintable group by timeseries(projectjoindate,'second')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with group by - scenario-1 with single materialized view ") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable group by timeseries(projectjoindate,'day'),projectcode")
    sql("drop materialized view  if exists mv1")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable group by timeseries(projectjoindate,'second'),projectcode")
    var df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable group by timeseries(projectjoindate,'day'),projectcode")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(result, df)
    df = sql("select timeseries(projectjoindate,'second'),projectcode from maintable group by timeseries(projectjoindate,'second'),projectcode")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    df = sql("select timeseries(projectjoindate,'second') from maintable group by timeseries(projectjoindate,'second')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    sql("drop materialized view  if exists mv1")
  }

  test("test timeseries query rollup with simple projection with group by - scenario-2") {
    val result = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with filter") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),projectcode from maintable")
    val df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with group by & filter - scenario 1") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8 " +
                     "group by timeseries(projectjoindate,'day'),projectcode")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql("create materialized view  mv1  as " +
        "select timeseries(projectjoindate,'second'),projectcode from maintable group by " +
        "timeseries(projectjoindate,'second'),projectcode")
    sql("create materialized view  mv2  as " +
        "select timeseries(projectjoindate,'hour'),projectcode from maintable group by timeseries" +
        "(projectjoindate,'hour'),projectcode")
    val df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8 " +
                 "group by timeseries(projectjoindate,'day'),projectcode")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with group by & filter - scenario 2") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8 group by timeseries(projectjoindate,'day'),projectcode")
    sql("drop materialized view  if exists mv1")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable where projectcode=1 group by timeseries(projectjoindate,'second'),projectcode")
    val df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable where projectcode=8 group by timeseries(projectjoindate,'day'),projectcode")
    assert(!TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
  }

  test("test timeseries query rollup with simple projection with alias- scenario 1") {
    val result = sql("select timeseries(projectjoindate,'day') as a,projectcode as b from maintable")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),projectcode from maintable")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),projectcode from maintable")
    val df = sql("select timeseries(projectjoindate,'day') as a,projectcode as b from maintable")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with simple projection with alias- scenario 2") {
    val result = sql("select timeseries(projectjoindate,'day'),projectcode from maintable")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second') as a,projectcode as b from maintable")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour') as a,projectcode as b from maintable")
    val df = sql("select timeseries(projectjoindate,'day'),projectcode from maintable")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with projection with alias and group by- scenario 1") {
    val result = sql("select timeseries(projectjoindate,'day') as a,sum(projectcode) as b from maintable group by timeseries(projectjoindate,'day')")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select timeseries(projectjoindate,'day') as a,sum(projectcode) as b from maintable group by timeseries(projectjoindate,'day')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test timeseries query rollup with projection with alias and group by- scenario 2") {
    val result = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second') as a,sum(projectcode) as b from maintable group by timeseries(projectjoindate,'second')")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour') as a,sum(projectcode) as b from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("rollup not supported for join queries") {
    sql("drop table if exists maintable1")
    sql("CREATE TABLE maintable1 (empno int,empname string, projectcode int, projectjoindate " +
      "Timestamp,salary double) STORED AS CARBONDATA")
    loadData("maintable1")
    val result = sql("select timeseries(t1.projectjoindate,'day'),count(timeseries(t2.projectjoindate,'day')),sum(t2.projectcode) from maintable t1 inner join maintable1 t2 " +
        "on (timeseries(t1.projectjoindate,'day')=timeseries(t2.projectjoindate,'day')) group by timeseries(t1.projectjoindate,'day')")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql("create materialized view  mv1  as " +
      "select timeseries(t1.projectjoindate,'second'),count(timeseries(t2.projectjoindate,'second')),sum(t2.projectcode) from maintable t1 inner join maintable1 t2 " +
      "on (timeseries(t1.projectjoindate,'second')=timeseries(t2.projectjoindate,'second')) group by timeseries(t1.projectjoindate,'second')")
    sql("create materialized view  mv2  as " +
      "select timeseries(t1.projectjoindate,'hour'),count(timeseries(t2.projectjoindate,'hour')),sum(t2.projectcode) from maintable t1 inner join maintable1 t2 " +
      "on (timeseries(t1.projectjoindate,'hour')=timeseries(t2.projectjoindate,'hour')) group by timeseries(t1.projectjoindate,'hour')")
    val df = sql("select timeseries(t1.projectjoindate,'day'),count(timeseries(t2.projectjoindate,'day')),sum(t2.projectcode) from maintable t1 inner join maintable1 t2 " +
        "on (timeseries(t1.projectjoindate,'day')=timeseries(t2.projectjoindate,'day')) group by timeseries(t1.projectjoindate,'day')")
    assert(!TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("rollup not supported for timeseries udf in filter") {
    val result = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable where timeseries(projectjoindate,'day')='2016-02-23 00:00:00' group by timeseries(projectjoindate,'day')")
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
    sql(
      "create materialized view  mv1  as " +
      "select timeseries(projectjoindate,'second'),sum(projectcode) from maintable group by timeseries(projectjoindate,'second')")
    sql(
      "create materialized view  mv2  as " +
      "select timeseries(projectjoindate,'hour'),sum(projectcode) from maintable group by timeseries(projectjoindate,'hour')")
    val df = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable where timeseries(projectjoindate,'day')='2016-02-23 00:00:00' group by timeseries(projectjoindate,'day')")
    assert(!TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv2"))
    checkAnswer(result, df)
    sql("drop materialized view  if exists mv1")
    sql("drop materialized view  if exists mv2")
  }

  test("test rollup for timeseries column of Date type") {
    drop()
    sql("CREATE TABLE maintable (empno int,empname string, projectcode int, projectjoindate " +
      "date,salary double) STORED AS CARBONDATA")
    sql("insert into maintable select 11,'joey',2,'2016-02-23',300")
    sql("insert into maintable select 13,'pheobe',1,'2016-02-23',450")
    sql("insert into maintable select 22,'cathy',1,'2016-02-25',450.5")
    sql("drop materialized view  if exists mv1")
    val result = sql("select timeseries(projectjoindate,'week'),sum(projectcode) from maintable group by timeseries(projectjoindate,'week')")
    sql("create materialized view  mv1  as select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    val dayDF = sql("select timeseries(projectjoindate,'day'),sum(projectcode) from maintable group by timeseries(projectjoindate,'day')")
    assert(TestUtil.verifyMVHit(dayDF.queryExecution.optimizedPlan, "mv1"))
    val weekDF = sql("select timeseries(projectjoindate,'week'),sum(projectcode) from maintable group by timeseries(projectjoindate,'week')")
    assert(TestUtil.verifyMVHit(weekDF.queryExecution.optimizedPlan, "mv1"))
    checkAnswer(result, weekDF)
    sql("drop materialized view  if exists mv1")
    drop()
  }

  def drop(): Unit = {
    sql("drop table if exists maintable")
  }

  def createTable(): Unit = {
    sql(
      "CREATE TABLE maintable (empno int,empname string, projectcode int, projectjoindate " +
      "Timestamp,salary double) STORED AS CARBONDATA")
  }

  def loadData(table: String): Unit = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/mv_sampledata.csv' INTO TABLE $table  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }
  // scalastyle:on lineLength
}
