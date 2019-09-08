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

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}


class TestPreAggStreaming extends CarbonQueryTest with BeforeAndAfterAll {


  override def beforeAll: Unit = {
    dropAll
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format' tblproperties('streaming'='true')")
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select name from mainTable group by name")
    sql("create datamap agg1 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql("create datamap agg2 on table mainTable using 'preaggregate' as select name,avg(age) from mainTable group by name")
    sql("create datamap agg3 on table mainTable using 'preaggregate' as select name,sum(CASE WHEN age=35 THEN id ELSE 0 END) from mainTable group by name")
    sql("CREATE TABLE mainTableStreamingOne(id int, name string, city string, age smallint) STORED BY 'org.apache.carbondata.format' tblproperties('streaming'='true')")
    sql("create datamap aggStreamingAvg on table mainTableStreamingOne using 'preaggregate' as select name,avg(age) from mainTableStreamingOne group by name")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTableStreamingOne")
    sql("CREATE TABLE origin(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format' tblproperties('streaming'='true')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table origin")
    sql("CREATE TABLE binary_stream(id int, label boolean, name string,image binary,autoLabel boolean) STORED BY 'org.apache.carbondata.format' tblproperties('streaming'='true')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/binaryDataBase64.csv' into table binary_stream OPTIONS('header'='false','DELIMITER'=',','binary_decoder'='baSe64')")
    sql("create datamap agg0 on table binary_stream using 'preaggregate' as select name from binary_stream group by name")
  }

  test("Test Pre Agg Streaming with project column and group by") {
    val df = sql("select name from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name from origin group by name"))
  }

  test("Test binary with stream and preaggregate") {
    val df = sql("select name from binary_stream group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name from binary_stream group by name"))
  }

  test("Test Pre Agg Streaming table Agg Sum Aggregation") {
    val df = sql("select name, sum(age) from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name, sum(age) from origin group by name"))
  }

  test("Test Pre Agg Streaming table with UDF") {
    val df = sql("select substring(name,1,1), sum(age) from maintable group by substring(name,1,1)")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select substring(name,1,1), sum(age) from origin group by substring(name,1,1)"))
  }

  test("Test Pre Agg Streaming table with UDF Only in group by") {
    val df = sql("select sum(age) from maintable group by substring(name,1,1)")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select sum(age) from origin group by substring(name,1,1)"))
  }

  test("Test Pre Agg Streaming table With Sum Aggregation And Order by") {
    val df = sql("select name, sum(age) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name, sum(age) from origin group by name order by name"))
  }

  test("Test Pre Agg Streaming table With Avg Aggregation") {
    val df = sql("select name, avg(age) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name, avg(age) from origin group by name order by name"))
  }

  test("Test Pre Agg Streaming table With Expression Aggregation") {
    val df = sql("select name, sum(CASE WHEN age=35 THEN id ELSE 0 END) from maintable group by name order by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name, sum(CASE WHEN age=35 THEN id ELSE 0 END) from origin group by name order by name"))
  }

  test("Test Pre Agg Streaming table With only aggregate expression and group by") {
    val df = sql("select sum(age) from maintable group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select sum(age) from origin group by name"))
  }

  test("Test Pre Agg Streaming table With small int and avg") {
    val df = sql("select name, avg(age) from mainTableStreamingOne group by name")
    assert(validateStreamingTablePlan(df.queryExecution.analyzed))
    checkAnswer(df, sql("select name, avg(age) from origin group by name"))
  }

  /**
   * Below method will be used validate whether plan is already updated in case of streaming table
   * In case of streaming table it will add UnionNode to get the data from fact and aggregate both
   * as aggregate table will be updated after each handoff.
   * So if plan is already updated no need to transform the plan again
   * @param logicalPlan
   * query plan
   * @return whether need to update the query plan or not
   */
  def validateStreamingTablePlan(logicalPlan: LogicalPlan) : Boolean = {
    var isChildTableExists: Boolean = false
    logicalPlan.transform {
      case union @ Union(Seq(plan1, plan2)) =>
        plan2.collect{
          case logicalRelation: LogicalRelation if
          logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .isChildDataMap =>
            isChildTableExists = true
        }
        union
    }
    isChildTableExists
  }

  private def dropAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists mainTableStreamingOne")
    sql("drop table if exists origin")
    sql("drop table if exists binary_stream")
  }

  override def afterAll: Unit = {
    dropAll
  }
}
