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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVPredicateAliasTestCase  extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    drop

    sql("create table test_main(id int,name string,height int) using carbondata")
    sql("insert into test_main select 1,'tom',170")
    sql("insert into test_main select 2,'lily',160")
    sql("create datamap test_main_mv using 'mv' as select cast(id + 1 as bigint) as cast_id ,count(name) from test_main group by cast_id")
    sql("rebuild datamap test_main_mv")
  }

  def drop(): Unit = {
    sql("drop datamap if exists test_main_mv")
    sql("drop table if exists test_main")
  }

  test("test mv predicate alias having match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id < 3")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(2, 1)))
  }

  test("test mv predicate alias where match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main where cast(id + 1 as bigint) < 3 group by cast_id")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(2, 1)))
  }

  test("test mv predicate alias having in match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id in (3,4,5)")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(3, 1)))
  }

  test("test mv predicate alias having not in match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id not in (3,4,5)")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(2, 1)))
  }

  test("test mv predicate alias having not between match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id not between 3 and 5")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(2, 1)))
  }

  test("test mv predicate alias having subquery match") {
    val frame = sql("select A.cast_id+1 from ( select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id not between 3 and 5)A")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq(Row(3)))
  }

  test("test mv predicate alias having equal match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main group by cast_id having cast_id < 3 and cast_id > 1 and cast_id <> 2")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq())
  }

  test("test mv predicate alias having not match") {
    val frame = sql("select cast(id + 1 as bigint) as cast_id,count(name) from test_main where id > 0 group by cast_id having cast_id < 3 and cast_id > 1 and cast_id <> 2")
    assert(!verifyMVDataMap(frame.queryExecution.analyzed, "test_main_mv"))
    checkAnswer(frame, Seq())
  }

  override def afterAll(): Unit = {
    drop
  }

  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName+"_table"))
  }
}
