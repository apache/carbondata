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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.BeforeAndAfterAll


class TestTimeseriesTableSelection extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    SparkUtil4Test.createTaskMockUp(sqlContext)
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='mytime', 'timeseries.hierarchy'='second=1,minute=1,hour=1,day=1,month=1,year=1') as select mytime, sum(age) from mainTable group by mytime")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
  }

  test("test PreAggregate table selection 1") {
    val df = sql("select mytime from mainTable group by mytime")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 2") {
    val df = sql("select timeseries(mytime,'hour') from mainTable group by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test PreAggregate table selection 3") {
    val df = sql("select timeseries(mytime,'milli') from mainTable group by timeseries(mytime,'milli')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 4") {
    val df = sql("select timeseries(mytime,'year') from mainTable group by timeseries(mytime,'year')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_year")
  }

  test("test PreAggregate table selection 5") {
    val df = sql("select timeseries(mytime,'day') from mainTable group by timeseries(mytime,'day')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_day")
  }

  test("test PreAggregate table selection 6") {
    val df = sql("select timeseries(mytime,'month') from mainTable group by timeseries(mytime,'month')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_month")
  }

  test("test PreAggregate table selection 7") {
    val df = sql("select timeseries(mytime,'minute') from mainTable group by timeseries(mytime,'minute')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_minute")
  }

  test("test PreAggregate table selection 8") {
    val df = sql("select timeseries(mytime,'second') from mainTable group by timeseries(mytime,'second')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_second")
  }

  test("test PreAggregate table selection 9") {
    val df = sql("select timeseries(mytime,'hour') from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 10") {
    val df = sql("select timeseries(mytime,'hour') from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 11") {
    val df = sql("select timeseries(mytime,'hour'),sum(age) from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 12") {
    val df = sql("select timeseries(mytime,'hour')as hourlevel,sum(age) as sum from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 13") {
    val df = sql("select timeseries(mytime,'hour')as hourlevel,sum(age) as sum from mainTable where timeseries(mytime,'hour')='x' and name='vishal' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable")
  }


  val timeSeries = "preaggregate"

  test("test timeseries create table 13: don't support minute=2") {
    sql(
      s"""create datamap agg1 on table mainTable using '$timeSeries'
         |DMPROPERTIES (
         |   'timeseries.eventTime'='mytime',
         |   'timeseries.hierarchy'='minute=2')
         |as select mytime, sum(age) from mainTable
         |group by mytime
        """.stripMargin)
//    checkExistence(sql("show tables"), true, "maintable_agg1_minute")

    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:02:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
      """.stripMargin)
    df.show()

    sql("select * from maintable_agg1_minute").show()
//    checkAnswer( sql("select * from maintable_agg1_hour"),
//      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"),10),
//        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"),20),
//        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"),30),
//        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"),40),
//        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"),50),
//        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"),50)))
  }


  test("test PreAggregate table selection 33: filter <= and >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:02:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
      """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }


  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit ={
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is from create preaTable1regate table class so no need to transform the query plan
      case ca:CarbonRelation =>
        if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        ca
      case logicalRelation:LogicalRelation =>
        if(logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        logicalRelation
    }
    if(!isValidPlan) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}
