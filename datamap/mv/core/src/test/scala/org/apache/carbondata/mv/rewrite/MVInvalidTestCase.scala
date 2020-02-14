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
package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVInvalidTestCase  extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    drop
    sql("create table main_table (name string,age int,height int) STORED AS carbondata")
  }

  def drop {
    sql("drop materialized view if exists main_table_mv")
    sql("drop table if exists main_table")
  }

  test("test mv different filter") {
    val querySQL = "select age,name from main_table where name = 'lily' order by name limit 10"
    sql("insert into main_table select 'tom',20,170")
    sql("insert into main_table select 'lily',30,160")
    sql("create materialized view main_table_mv as select age,name,height from main_table where name = 'tom'")
    sql("refresh materialized view main_table_mv")

    assert(!TestUtil.verifyMVDataMap(sql(querySQL).queryExecution.optimizedPlan, "main_table_mv"))
  }

  override def afterAll(): Unit = {
    drop
  }
}
