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

package org.apache.spark.carbondata.query

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.scalatest.BeforeAndAfterAll

class SubQueryTestSuite extends Spark2QueryTest with BeforeAndAfterAll {

  val tempDirPath = s"$resourcesPath/temp"

  override def beforeAll(){
    sql("drop table if exists subquery")
    sql("create table subquery(id int, name string, rating float) STORED AS carbondata")
    sql(s"load data local inpath '$tempDirPath/data1.csv' into table subquery")
  }

  test("test to check if 2nd level subquery gives correct result") {
    checkAnswer(sql(
      "select * from subquery where id in(select id from subquery where name in(select name from" +
      " subquery where rating=2.0))"),
      Seq(Row(2,"ghj",2.0), Row(3,"ghj",3.0)))
  }

  test("test to check Broad cast filter works") {
    sql("drop table if exists anothertable")
    sql("create table anothertable(id int, name string, rating float) STORED AS carbondata")
    sql(s"load data local inpath '$tempDirPath/data1.csv' into table anothertable")

    val executedPlan =
      sql("select * from subquery t1, anothertable t2 where t1.id=t2.id").
        queryExecution.executedPlan
    var broadCastExists = false
    executedPlan.collect {
      case s: BroadcastHashJoinExec => broadCastExists = true
    }
    assert(broadCastExists, "Broad cast join does not exist on small table")
    sql("drop table if exists anothertable")
  }

  test("tupleId") {
    checkExistence(sql("select getTupleId() as tupleId from subquery"), true, "0/0/0-0_batchno0-0-")
  }

  override def afterAll() {
    sql("drop table if exists subquery")
  }
}
