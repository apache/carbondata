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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggregateTableSelection extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists agg0")
    sql("drop table if exists agg1")
    sql("drop table if exists agg2")
    sql("drop table if exists agg3")
    sql("drop table if exists agg4")
    sql("drop table if exists agg5")
    sql("drop table if exists agg6")
    sql("drop table if exists agg7")
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
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
  }

  test("test sum and avg on same column should give proper results") {
    val df = sql("select name, sum(id), avg(id) from maintable group by name")
    checkAnswer(df, Seq(Row("david",1,1.0), Row("jarry",6,3.0), Row("kunal",4,4.0), Row("eason",2,2.0), Row("vishal",4,4.0)))
  }


  test("test PreAggregate table selection 1") {
    val df = sql("select name from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
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

  test("test if pre-agg table is hit with filter condition") {
    sql("drop table if exists filtertable")
    sql("CREATE TABLE filtertable(id int, name string, city string, age string) STORED BY" +
        " 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='name,age')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table filtertable")
    sql("create datamap agg9 on table filtertable using 'preaggregate' as select name, age, sum(age) from filtertable group by name, age")
    val df = sql("select name, sum(age) from filtertable where age = '29' group by name, age")
    preAggTableValidator(df.queryExecution.analyzed, "filtertable_agg9")
    checkAnswer(df, Row("vishal", 29))
  }

  test("test PreAggregate table selection 29") {
    val df = sql("select sum(id) from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg2")
  }

  test("test pre-agg table with group by condition") {
    sql("drop table if exists grouptable")
    sql("CREATE TABLE grouptable(id int, name string, city string, age string) STORED BY" +
        " 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='name,age')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table grouptable")
    sql(
      "create datamap agg9 on table grouptable using 'preaggregate' as select sum(id) from grouptable group by city")
    val df = sql("select sum(id) from grouptable group by city")
    preAggTableValidator(df.queryExecution.analyzed, "grouptable_agg9")
    checkAnswer(df, Seq(Row(3), Row(3), Row(4), Row(7)))
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

  test("Test query with math operation hitting fact table") {
    val df =  sql("select sum(id)+count(id) from maintable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists lineitem")
  }

}
