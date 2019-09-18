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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggregateExpressions extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
  }

  test("test pre agg create table with expression 1") {
    sql(
      s"""
         | CREATE DATAMAP agg0 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | count(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0"), true, "maintable_age_count")
  }

  test("test pre agg create table with expression 2") {
    sql(
      s"""
         | CREATE DATAMAP agg1 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=35 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg1"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 3") {
    sql(
      s"""
         | CREATE DATAMAP agg2 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=35 THEN id ELSE 0 END),
         | city
         | FROM mainTable GROUP BY name,city
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg2"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 4") {
    sql(
      s"""
         | CREATE DATAMAP agg3 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=27 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg3"), true, "maintable_column_0_sum")
  }

  test("test pre agg create table with expression 5") {
    sql(
      s"""
         | CREATE DATAMAP agg4 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | sum(CASE WHEN age=27 THEN id ELSE 0 END),
         | SUM(CASE WHEN age=35 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg4"), true, "maintable_column_0_sum")
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg4"), true, "maintable_column_1_sum")
  }

  test("test pre agg create table with expression 6") {
    sql(
      s"""
         | CREATE DATAMAP agg5 ON TABLE mainTable USING 'preaggregate' AS
         | SELECT name,
         | COUNT(CASE WHEN age=27 THEN(CASE WHEN name='eason' THEN id ELSE 0 END) ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin)
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg5"), true, "maintable_column_0_count")
  }

  test("test pre agg table selection with expression 1") {
    val df = sql("select name as NewName, count(age) as sum from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test pre agg table selection with expression 2") {
    val df = sql("select name as NewName, sum(case when age=35 then id else 0 end) as sum from mainTable group by name order by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
  }

  test("test pre agg table selection with expression 3") {
    val df = sql("select sum(case when age=35 then id else 0 end) from maintable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg1")
    checkAnswer(df, Seq(Row(6.0)))
  }

  test("test pre agg table selection with expression 4") {
    val df = sql("select sum(case when age=27 then id else 0 end) from maintable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg3")
    checkAnswer(df, Seq(Row(2.0)))
  }

  test("test pre agg table selection with expression 5") {
    val df = sql("select sum(case when age=27 then id else 0 end), sum(case when age=35 then id else 0 end) from maintable")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg4")
    checkAnswer(df, Seq(Row(2.0,6.0)))
  }

  /**
   * Below method will be used to validate the table name is present in the plan or not
   *
   * @param plan            query plan
   * @param actualTableName table name to be validated
   */
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

  test("Test Pre_aggregate with decimal column with order by") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, decimal_col decimal(30,16)) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',452.564")
    sql(
      "create datamap ag1 on table maintable using 'preaggregate' as select name,avg(decimal_col)" +
      " from maintable group by name")
    checkAnswer(sql("select avg(decimal_col) from maintable group by name order by name"),
      Seq(Row(452.56400000000000000000)))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
  }

}
