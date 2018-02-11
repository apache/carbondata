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

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES

class TestTimeseriesTableSelection extends QueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString

  override def beforeAll: Unit = {
    SparkUtil4Test.createTaskMockUp(sqlContext)
    sql("drop table if exists mainTable")
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
         | 'YEAR_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM mainTable
         | GROUP BY mytime
       """.stripMargin)

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

  test("test timeseries table selection 14: Granularity only support 1 and throw Exception") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg3_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'HOUR_GRANULARITY'='2')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
  }

  test("test timeseries table selection 15: Granularity only support 1 and throw Exception") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg3_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'HOUR_GRANULARITY'='1.5')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
  }

  test("test timeseries table selection 16: Granularity only support 1 and throw Exception") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE DATAMAP agg3_second ON TABLE mainTable
           | USING '$timeSeries'
           | DMPROPERTIES (
           | 'EVENT_TIME'='dataTime',
           | 'HOUR_GRANULARITY'='-1')
           | AS SELECT dataTime, SUM(age) FROM mainTable
           | GROUP BY dataTime
       """.stripMargin)
    }
    assert(e.getMessage.contains("Granularity only support 1"))
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
