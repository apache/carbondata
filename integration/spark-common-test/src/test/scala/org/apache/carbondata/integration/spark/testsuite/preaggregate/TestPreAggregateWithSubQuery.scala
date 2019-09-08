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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll


class TestPreAggregateWithSubQuery extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("DROP TABLE IF EXISTS mainTable1")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE mainTable1(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable1")
  }

  test("test sub query PreAggregate table selection 1") {
    val df = sql(
      """
        | SELECT t2.newnewname AS newname
        | FROM mainTable1 t1
        | JOIN (
        |     select
        |       name AS newnewname,
        |       sum(age) AS sum
        |     FROM mainTable
        |     GROUP BY name ) t2
        | ON t1.name = t2.newnewname
        | GROUP BY t2.newnewname
      """.stripMargin)
    matchTable(collectLogicalRelation(df.queryExecution.analyzed), "maintable_agg0")
  }

  test("test sub query PreAggregate table selection 2") {
    val df = sql("select t1.name,t1.city from mainTable1 t1 join (select name as newnewname,sum(age) as sum from mainTable group by name )t2 on t1.name=t2.newnewname")
    matchTable(collectLogicalRelation(df.queryExecution.analyzed), "maintable_agg0")
  }

  test("test sub query PreAggregate table selection 3") {
    val df = sql("select t1.name,t2.sum from mainTable1 t1 join (select name as newnewname,sum(age) as sum from mainTable group by name )t2 on t1.name=t2.newnewname")
    matchTable(collectLogicalRelation(df.queryExecution.analyzed), "maintable_agg0")
  }

  test("test sub query PreAggregate table selection 4") {
    val df = sql("select t1.name,t2.sum from mainTable1 t1 join (select name,sum(age) as sum from mainTable group by name )t2 on t1.name=t2.name group by t1.name, t2.sum")
    matchTable(collectLogicalRelation(df.queryExecution.analyzed), "maintable_agg0")
  }

  /**
   * Below method will be used to collect all the logical relation from logical plan
   * @param logicalPlan
   * query logical plan
   * @return all the logical relation
   */
  def collectLogicalRelation(logicalPlan: LogicalPlan) : Seq[LogicalRelation] = {
    logicalPlan.collect{
      case l:LogicalRelation => l
    }
  }

  /**
   * Below method will be used to match the logical relation
   * @param logicalRelations
   * all logical relation
   * @param tableName
   * table name
   */
  def matchTable(logicalRelations: Seq[LogicalRelation], tableName: String) {
    assert(logicalRelations.exists {
      case l:LogicalRelation  if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.getTableName.
          equalsIgnoreCase(tableName)
    })
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("DROP TABLE IF EXISTS mainTable1")
  }

}
