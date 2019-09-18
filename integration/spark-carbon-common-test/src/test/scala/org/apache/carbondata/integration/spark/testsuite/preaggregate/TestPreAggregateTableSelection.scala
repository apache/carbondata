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
package org.apache.carbondata.integration.spark.testsuite.preaggregate

import java.io.File

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.CarbonSparkQueryTest

class TestPreAggregateTableSelection extends CarbonSparkQueryTest with BeforeAndAfterAll {

  val timeSeries = TIMESERIES.toString

  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    sql("drop table if exists mainTable")
    sql("drop table if exists mainTableavg")
    sql("drop table if exists agg0")
    sql("drop table if exists agg1")
    sql("drop table if exists agg2")
    sql("drop table if exists agg3")
    sql("drop table if exists agg4")
    sql("drop table if exists agg5")
    sql("drop table if exists agg6")
    sql("drop table if exists agg7")
    sql("DROP TABLE IF EXISTS maintabledict")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select name from mainTable group by name")
    sql("create datamap agg1 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql("create datamap agg2 on table mainTable using 'preaggregate' as select name,sum(id) from mainTable group by name")
    sql("create datamap agg3 on table mainTable using 'preaggregate' as select name,count(id) from mainTable group by name")
    sql("create datamap agg4 on table mainTable using 'preaggregate' as select name,sum(age),count(id) from mainTable group by name")
    sql("create datamap agg5 on table mainTable using 'preaggregate' as select name,avg(age) from mainTable group by name")
    sql("create datamap agg6 on table mainTable using 'preaggregate' as select name,min(age) from mainTable group by name")
    sql("create datamap agg7 on table mainTable using 'preaggregate' as select name,max(age) from mainTable group by name")
    sql("create datamap agg8 on table maintable using 'preaggregate' as select name, sum(id), avg(id) from maintable group by name")
    sql("create datamap agg9 on table maintable using 'preaggregate' as select name, count(*) from maintable group by name")
    sql("CREATE TABLE mainTableavg(id int, name string, city string, age bigint) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTableavg using 'preaggregate' as select name,sum(age), avg(age) from mainTableavg group by name")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTableavg")
  }

  test("test PreAggregate table selection 1") {
    val df = sql("select name from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection with count(*)") {
    val df = sql("select name, count(*) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg9")
  }

  test("test PreAggregate table selection 2") {
    val df = sql("select name from mainTable where name in (select name from mainTable) group by name")
    preAggTableValidator(df.queryExecution.analyzed, "mainTable")
  }

  test("test PreAggregate table selection 3") {
    val df = sql("select name from mainTable where name in (select name from mainTable group by name) group by name")
    preAggTableValidator(df.queryExecution.analyzed, "mainTable")
  }

  test("test PreAggregate table selection 4") {
    val df = sql("select name from mainTable where name in('vishal') group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 5") {
    val df = sql("select name, sum(age) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection with table alias") {
    val df = sql("select name, sum(age) from mainTable as t1 group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 6") {
    val df = sql("select sum(age) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 7") {
    val df = sql("select sum(id) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg2")
  }

  test("test PreAggregate table selection 8") {
    val df = sql("select count(id) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg3")
  }

  test("test PreAggregate table selection 9") {
    val df = sql("select sum(age), count(id) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg4")
  }

  test("test PreAggregate table selection 10") {
    val df = sql("select avg(age) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg5")
  }

  test("test PreAggregate table selection 11") {
    val df = sql("select max(age) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg7")
  }

  test("test PreAggregate table selection 12") {
    val df = sql("select min(age) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg6")
  }

  test("test PreAggregate table selection 13") {
    val df = sql("select name, sum(age) from mainTable where city = 'Bangalore' group by name")
    preAggTableValidator(df.queryExecution.analyzed, "mainTable")
  }

  test("test PreAggregate table selection 14") {
    val df = sql("select sum(age) from mainTable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 15") {
    val df = sql("select avg(age) from mainTable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg5")
  }

  test("test PreAggregate table selection 16") {
    val df = sql("select max(age) from mainTable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg7")
  }

  test("test PreAggregate table selection 17") {
    val df = sql("select min(age) from mainTable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg6")
  }

  test("test PreAggregate table selection 18") {
    val df = sql("select count(id) from mainTable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg3")
  }

  test("test PreAggregate table selection 19: test sum and avg on same column should give proper results") {
    val df = sql("select name, sum(id), avg(id) from maintable group by name")
    checkAnswer(df, Seq(Row("david",1,1.0), Row("jarry",6,3.0), Row("kunal",4,4.0), Row("eason",2,2.0), Row("vishal",4,4.0)))
    checkPreAggTable(df, false, "maintable_agg5", "maintable_agg1")
    checkPreAggTable(df, true, "maintable_agg8")
  }

  test("test PreAggregate table selection 20") {
    val df = sql("select name from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 21") {
    val df = sql("select name as NewName from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 22") {
    val df = sql("select name, sum(age) from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 23") {
    val df = sql("select name as NewName, sum(age) as sum from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 24") {
    val df = sql("select name as NewName, sum(age) as sum from mainTable where name='vishal' group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test PreAggregate table selection 25") {
    val df = sql("select name as NewName, sum(age) as sum from mainTable where city = 'Bangalore' group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 26") {
    val df = sql("select name from mainTable where name='vishal' group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 27") {
    val df = sql("select name as NewName from mainTable where name='vishal' group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 28") {
    val df = sql("select name as NewName, sum(case when age=2016 then 1 else 0 end) as sum from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
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


  test("test PreAggregate table selection 29") {
    val df = sql("select sum(id) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg2")
  }

  test("test PreAggregate table selection 30") {
    val df = sql("select a.name from mainTable a group by a.name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 31") {
    val df = sql("select a.name as newName from mainTable a group by a.name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 32") {
    val df = sql("select a.name as newName from mainTable a  where a.name='vishal' group by a.name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

 test("test PreAggregate table selection 33: Test query with math operation hitting fact table") {
    val df =  sql("select sum(id)+count(id) from maintable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 34: test if pre-agg table is hit with filter condition") {
    sql("DROP TABLE IF EXISTS filtertable")
    sql(
      """
        | CREATE TABLE filtertable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age STRING)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='name,age')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table filtertable")
    sql(
      """
        | CREATE DATAMAP agg9
        | ON TABLE filtertable
        | USING 'preaggregate'
        | AS SELECT name, age, SUM(age)
        |     FROM filtertable
        |     GROUP BY name, age
      """.stripMargin)
    val df = sql("SELECT name, SUM(age) FROM filtertable WHERE age = '29' GROUP BY name, age")
    preAggTableValidator(df.queryExecution.analyzed, "filtertable_agg9")
    checkAnswer(df, Row("vishal", 29))
  }

  test("test PreAggregate table selection 35: test pre-agg table with group by condition") {
    sql("DROP TABLE IF EXISTS grouptable")
    sql(
      """
        | CREATE TABLE grouptable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age STRING)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='name,age')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table grouptable")
    sql(
      """
        | CREATE DATAMAP agg9
        | ON TABLE grouptable
        | USING 'preaggregate'
        | AS SELECT SUM(id)
        |     FROM grouptable
        |     GROUP BY city
      """.stripMargin)
    val df = sql("SELECT SUM(id) FROM grouptable GROUP BY city")
    preAggTableValidator(df.queryExecution.analyzed, "grouptable_agg9")
    checkAnswer(df, Seq(Row(3), Row(3), Row(4), Row(7)))
  }

  test("test PreAggregate table selection 36: test PreAggregate table selection with timeseries and normal together") {
    sql("drop table if exists maintabletime")
    sql(
      "create table maintabletime(year int,month int,name string,salary int,dob timestamp) stored" +
      " by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23'," +
      "'sort_columns'='month,year,name')")
    sql("insert into maintabletime select 10,11,'babu',12,'2014-01-01 00:00:00'")
    sql(
      "create datamap agg0 on table maintabletime using 'preaggregate' as select dob,name from " +
      "maintabletime group by dob,name")

  sql(
    s"""
       | CREATE DATAMAP agg1_year ON TABLE maintabletime
       | USING '$timeSeries'
       | DMPROPERTIES (
       | 'EVENT_TIME'='dob',
       | 'YEAR_GRANULARITY'='1')
       | AS SELECT dob, name FROM maintabletime
       | GROUP BY dob,name
       """.stripMargin)

  val df = sql("SELECT timeseries(dob,'year') FROM maintabletime GROUP BY timeseries(dob,'year')")
  preAggTableValidator(df.queryExecution.analyzed, "maintabletime_agg1_year")
  sql("DROP TABLE IF EXISTS maintabletime")

  }

  test("test table selection when unsupported aggregate function is present") {
    sql("DROP TABLE IF EXISTS maintabletime")
    sql(
      "create table maintabletime(year int,month int,name string,salary int,dob string) stored" +
      " by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23'," +
      "'sort_columns'='month,year,name')")
    sql("insert into maintabletime select 10,11,'x',12,'2014-01-01 00:00:00'")
    sql(
      "create datamap agg0 on table maintabletime using 'preaggregate' as select name,sum(salary) from " +
      "maintabletime group by name")

    sql("select var_samp(name) from maintabletime  where name='Mikka' ")
  }

  test("test PreAggregate table selection 38: for sum and avg in aggregate table with bigint") {
    val df = sql("select avg(age) from mainTableavg")
    preAggTableValidator(df.queryExecution.analyzed, "mainTableavg_agg0")
  }

  test("test PreAggregate table selection for avg with maintable containing dictionary include for group by column") {
    sql(
      "create table maintabledict(year int,month int,name string,salary int,dob string) stored" +
      " by 'carbondata' tblproperties('DICTIONARY_INCLUDE'='year')")
    sql("insert into maintabledict select 10,11,'x',12,'2014-01-01 00:00:00'")
    sql("insert into maintabledict select 10,11,'x',12,'2014-01-01 00:00:00'")
    sql(
      "create datamap aggdict on table maintabledict using 'preaggregate' as select year,avg(year) from " +
      "maintabledict group by year")
    val df = sql("select year,avg(year) from maintabledict group by year")
    checkAnswer(df, Seq(Row(10,10.0)))
  }

  test("explain projection query") {
    val rows = sql("explain select name, age from mainTable").collect()
    assertResult(
      """== CarbonData Profiler ==
        |Table Scan on maintable
        | - total: 1 blocks, 1 blocklets
        | - filter: none
        | - pruned by Main DataMap
        |    - skipped: 0 blocks, 0 blocklets
        |""".stripMargin)(rows(0).getString(0))
  }

  test("explain projection query hit datamap") {
    val rows = sql("explain select name,sum(age) from mainTable group by name").collect()
    assertResult(
      """== CarbonData Profiler ==
        |Query rewrite based on DataMap:
        | - agg1 (preaggregate)
        |Table Scan on maintable_agg1
        | - total: 1 blocks, 1 blocklets
        | - filter: none
        | - pruned by Main DataMap
        |    - skipped: 0 blocks, 0 blocklets
        |""".stripMargin)(rows(0).getString(0))
  }

  test("explain filter query") {
    sql("explain select name,sum(age) from mainTable where name = 'a' group by name").show(false)
    val rows = sql("explain select name,sum(age) from mainTable where name = 'a' group by name").collect()
    assertResult(
      """== CarbonData Profiler ==
        |Query rewrite based on DataMap:
        | - agg1 (preaggregate)
        |Table Scan on maintable_agg1
        | - total: 1 blocks, 1 blocklets
        | - filter: (maintable_name <> null and maintable_name = a)
        | - pruned by Main DataMap
        |    - skipped: 1 blocks, 1 blocklets
        |""".stripMargin)(rows(0).getString(0))

  }

  test("explain query with multiple table") {
    val query = "explain select t1.city,sum(t1.age) from mainTable t1, mainTableavg t2 " +
                "where t1.name = t2.name and t1.id < 3 group by t1.city"
    sql(query).show(false)
    val rows = sql(query).collect()
    assert(rows(0).getString(0).contains(
      """
        |Table Scan on maintable
        | - total: 1 blocks, 1 blocklets
        | - filter: ((id <> null and id < 3) and name <> null)
        | - pruned by Main DataMap
        |    - skipped: 0 blocks, 0 blocklets""".stripMargin))
    assert(rows(0).getString(0).contains(
      """
        |Table Scan on maintableavg
        | - total: 1 blocks, 1 blocklets
        | - filter: name <> null
        | - pruned by Main DataMap
        |    - skipped: 0 blocks, 0 blocklets""".stripMargin))

  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists mainTable_avg")
    sql("drop table if exists lineitem")
    sql("DROP TABLE IF EXISTS maintabletime")
    sql("DROP TABLE IF EXISTS maintabledict")
    sql("DROP TABLE IF EXISTS mainTableavg")
    sql("DROP TABLE IF EXISTS filtertable")
    sql("DROP TABLE IF EXISTS grouptable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }

}
