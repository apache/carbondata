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
    sql("drop table if exists lineitem")
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
    sql("create table if not exists lineitem(L_SHIPDATE string,L_SHIPMODE string,L_SHIPINSTRUCT string,L_RETURNFLAG string,L_RECEIPTDATE string,L_ORDERKEY string,L_PARTKEY string,L_SUPPKEY string,L_LINENUMBER int,L_QUANTITY double,L_EXTENDEDPRICE double,L_DISCOUNT double,L_TAX double,L_LINESTATUS string,L_COMMITDATE string,L_COMMENT string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('table_blocksize'='128','NO_INVERTED_INDEX'='L_SHIPDATE,L_SHIPMODE,L_SHIPINSTRUCT,L_RETURNFLAG,L_RECEIPTDATE,L_ORDERKEY,L_PARTKEY,L_SUPPKEY','sort_columns'='')")
    sql("create datamap agr_lineitem ON TABLE lineitem USING 'preaggregate' as select L_RETURNFLAG,L_LINESTATUS,sum (L_QUANTITY),sum(L_EXTENDEDPRICE) from lineitem group by L_RETURNFLAG, L_LINESTATUS")
  }

  test("test sum and avg on same column should give proper results") {
    val df = sql("select name, sum(id), avg(id) from maintable group by name")
    checkAnswer(df, Seq(Row("david",1,1.0), Row("jarry",6,3.0), Row("kunal",4,4.0), Row("eason",2,2.0),Row("vishal",4,4.0)))
  }


  test("test PreAggregate table selection 1") {
    val df = sql("select name from mainTable group by name")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  ignore("test PreAggregate table selection 2") {
    val df = sql("select name from mainTable where name in (select name from mainTable) group by name")
    preAggTableValidator(df.queryExecution.analyzed, "mainTable")
  }

  ignore("test PreAggregate table selection 3") {
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

  test("test PreAggregate table selection 19") {
    val df = sql("select L_RETURNFLAG,L_LINESTATUS,sum(L_QUANTITY),sum(L_EXTENDEDPRICE) from lineitem group by L_RETURNFLAG, L_LINESTATUS")
    preAggTableValidator(df.queryExecution.analyzed, "lineitem_agr_lineitem")
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


  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit ={
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is from create preaTable1regate table class so no need to transform the query plan
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
    sql("drop table if exists agg0")
    sql("drop table if exists agg1")
    sql("drop table if exists agg2")
    sql("drop table if exists agg3")
    sql("drop table if exists agg4")
    sql("drop table if exists agg5")
    sql("drop table if exists agg6")
    sql("drop table if exists agg7")
  }

}
