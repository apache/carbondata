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

package org.apache.spark.sql.execution.command.mutation

import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata

class CarbonTruncateCommandTest extends QueryTest with BeforeAndAfterAll {
  test("test truncate table") {
    val index: Long = System.currentTimeMillis()
    sql(s"create table dli_stored$index(id int, name string) stored as carbondata")
    sql(s"insert into table dli_stored$index select 2,'aa'")
    sql(s"truncate table dli_stored$index")
    assert(getTableSize("default", s"dli_stored$index") == 0)
  }

  test("test truncate partition table") {
    val index: Long = System.currentTimeMillis()
    sql(s"create table dli_stored$index(id int) stored as carbondata " +
        s"partitioned by (name string)")
    sql(s"insert into table dli_stored$index select 2,'aa'")
    sql(s"truncate table dli_stored$index")
    assert(getTableSize("default", s"dli_stored$index") == 0)
  }

  private def getTableSize(databaseName: String, tableName:String) :Long={
    val table = CarbonMetadata.getInstance.getCarbonTable(databaseName, tableName)
    val relation = CarbonRelation(databaseName, tableName, table)
    relation.sizeInBytes
  }
}
