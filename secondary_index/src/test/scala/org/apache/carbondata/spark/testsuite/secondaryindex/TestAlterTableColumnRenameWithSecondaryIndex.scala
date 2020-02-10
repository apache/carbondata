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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestAlterTableColumnRenameWithSecondaryIndex extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    dropTable()
  }

  test("test direct rename on SI table") {
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table index1 change c test string")
    }
    assert(ex.getMessage.contains("Alter table column rename is not allowed on index table"))
  }

  test("test column rename with SI table") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    sql("alter table si_rename change c test string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "index1")
    assert(null != carbonTable.getColumnByName("test"))
    assert(null == carbonTable.getColumnByName("c"))
  }

  test("test column rename with multiple SI table table") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    sql("create index index2 on table si_rename(c,d) AS 'carbondata' ")
    sql("alter table si_rename change c test string")
    sql("alter table si_rename change d testSI string")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("default", "index1")
    assert(null != carbonTable1.getColumnByName("test"))
    assert(null == carbonTable1.getColumnByName("c"))
    val carbonTable2 = CarbonMetadata.getInstance().getCarbonTable("default", "index2")
    assert(null != carbonTable2.getColumnByName("testSI"))
    assert(null == carbonTable2.getColumnByName("d"))
  }

  test("test column rename with SI tables load and query") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata'")
    sql("create index index2 on table si_rename(c,d) AS 'carbondata'")
    sql("insert into si_rename select 'abc',3,'def','mno'")
    sql("insert into si_rename select 'def',4,'xyz','pqr'")
    val query1 = sql("select c,d from si_rename where d = 'pqr' or c = 'def'").count()
    sql("alter table si_rename change c test string")
    sql("alter table si_rename change d testSI string")
    sql("show indexes on si_rename").collect
    val query2 = sql("select test,testsi from si_rename where testsi = 'pqr' or test = 'def'").count()
    assert(query1 == query2)
    val df = sql("select test,testsi from si_rename where testsi = 'pqr' or test = 'def'").queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override protected def afterAll(): Unit = {
    dropTable()
  }

  private def dropTable(): Unit = {
    sql("drop table if exists si_rename")
  }

  private def createTable(): Unit = {
    sql("create table si_rename (a string,b int, c string, d string) STORED AS carbondata")
  }

  /**
    * Method to check whether the filter is push down to SI table or not
    *
    * @param sparkPlan
    * @return
    */
  private def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }
}
