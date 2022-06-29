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

package org.apache.carbondata.view.rewrite

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVFilterAndJoinTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    defaultConfig()
    drop
    sql("create table main_table (name string,age int,height int) STORED AS carbondata")
    sql("create table dim_table (name string,age int,height int) STORED AS carbondata")
    sql("create table sdr_table (name varchar(20),score int) STORED AS carbondata")
    sql("set carbon.enable.mv = true")
  }

  def drop() {
    sql("drop table if exists main_table")
    sql("drop materialized view if exists main_table_mv")
    sql("drop table if exists dim_table")
    sql("drop table if exists sdr_table")
    sql("drop materialized view if exists main_table_mv1")
  }

  test("test mv no filter and query with filter") {
    val querySQL = "select sum(age),name from main_table where name = 'tom' group by name"
    sql("insert into main_table select 'tom',20,170")
    sql("insert into main_table select 'lily',30,160")
    sql("create materialized view main_table_mv as select sum(age),name " +
        "from main_table group by name")
    sql("refresh materialized view main_table_mv")
    assert(TestUtil.verifyMVHit(sql(querySQL).queryExecution.optimizedPlan, "main_table_mv"))
    checkAnswer(sql(querySQL), Seq(Row(20, "tom")))
  }

  test("test mv rebuild twice and varchar string") {
    val querySQL = "select A.sum_score,A.name from (select sum(score) as sum_score,age,sdr.name " +
                   "as name from sdr_table sdr left join dim_table dim on sdr.name = dim.name " +
                   "where sdr.name in ('tom','lily') group by sdr.name,age) A where name = 'tom'"
    sql("insert into dim_table select 'tom',20,170")
    sql("insert into dim_table select 'lily',30,160")
    sql(
      "create materialized view main_table_mv1 as select count(score),sum(score),count(dim.name)," +
      "age,sdr.name from sdr_table sdr left join dim_table dim on sdr.name = dim.name group by " +
      "sdr.name,age")
    sql("refresh materialized view main_table_mv1")
    sql("insert into sdr_table select 'tom',70")
    sql("insert into sdr_table select 'tom',50")
    sql("insert into sdr_table select 'lily',80")
    sql("refresh materialized view main_table_mv1")
    assert(TestUtil.verifyMVHit(sql(querySQL).queryExecution.optimizedPlan, "main_table_mv1"))
    checkAnswer(sql(querySQL), Seq(Row(120, "tom")))
  }

  override def afterAll(): Unit = {
    drop
    sql("set carbon.enable.mv = false")
  }

}
