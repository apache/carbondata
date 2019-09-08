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
import org.apache.spark.sql.test.util.CarbonQueryTest

class SelectAllColumnsSuite extends CarbonQueryTest {

  test ("table select all columns mv") {
    sql("drop datamap if exists all_table_mv")
    sql("drop table if exists all_table")
    sql("create table all_table(name string, age int, height int)  stored by 'carbondata'")
    sql("insert into all_table select 'tom',20,175")
    sql("insert into all_table select 'tom',32,180")
    sql("create datamap all_table_mv on table all_table using 'mv' as select avg(age),avg(height),name from all_table group by name")
    sql("rebuild datamap all_table_mv")
    checkAnswer(
      sql("select avg(age),avg(height),name from all_table group by name"),
      Seq(Row(26.0, 177.5, "tom")))
    val frame = sql("select avg(age),avg(height),name from all_table group by name")
    val analyzed = frame.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "all_table_mv"))
    sql("drop table if exists all_table")
  }

  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName+"_table"))
  }

}
