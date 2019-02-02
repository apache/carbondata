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

class MVCoalesceTestCase  extends QueryTest with BeforeAndAfterAll  {
  override def beforeAll(): Unit = {
    drop()
    sql("create table coalesce_test_main(id int,name string,height int,weight int) " +
      "using carbondata")
    sql("insert into coalesce_test_main select 1,'tom',170,130")
    sql("insert into coalesce_test_main select 2,'tom',170,130")
    sql("insert into coalesce_test_main select 3,'lily',160,100")
  }

  def drop(): Unit = {
    sql("drop table if exists coalesce_test_main")
  }

  test("test mv table with coalesce expression") {
    sql("drop datamap if exists coalesce_test_main_mv")
    sql("create datamap coalesce_test_main_mv using 'mv' as " +
      "select sum(id) as sum_id,sum(height) as sum_height,name as myname from coalesce_test_main group by name")
    sql("rebuild datamap coalesce_test_main_mv")

    var frame = sql("select coalesce(sum(id),12) as sumid,sum(height) as sum_height from coalesce_test_main group by name")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "coalesce_test_main_mv"))
    checkAnswer(frame, Seq(Row(3, 340), Row(3, 160)))

    frame = sql("select coalesce(sum(id),12) as sumid,sum(height) as sum_height from coalesce_test_main " +
      "where name = 'tom' group by name")
    assert(verifyMVDataMap(frame.queryExecution.analyzed, "coalesce_test_main_mv"))
    checkAnswer(frame, Seq(Row(3, 340)))

    sql("drop datamap if exists coalesce_test_main_mv")
  }

  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName+"_table"))
  }

  override def afterAll(): Unit ={
    drop
  }
}
