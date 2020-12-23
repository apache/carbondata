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

import java.util

import mockit.{Mock, MockUp}
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

class TestCarbonInternalMetastore extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val dbName: String = "test"
  val tableName: String = "table1"
  val indexName: String = "index1"
  var tableIdentifier: TableIdentifier = _
  var parentCarbonTable: CarbonTable = _
  var indexTable: CarbonTable = _

  override def beforeAll(): Unit = {
    sql("drop database if exists test cascade");
    sql("create database test")
    sql("use test")
  }

  override def beforeEach(): Unit = {
    sql("drop table if exists table1")
    sql("create table table1(a string, b string, c string) stored as carbondata")
    sql("create index index1 on table1(b) as 'carbondata'")
    sql("insert into table1 values('ab','bc','cd')")
  }

  def setVariables(indexName: String): Unit = {
    tableIdentifier = new TableIdentifier(indexName, Some("test"))
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    indexTable = CarbonEnv.getCarbonTable(Some("test"), "index1")(sqlContext.sparkSession)
  }

  test("test delete index silent") {
    setVariables("index1")
    CarbonInternalMetastore.deleteIndexSilent(tableIdentifier, "",
      parentCarbonTable)(sqlContext.sparkSession)
    checkExistence(sql("show indexes on table1"), false, "index1")
  }

  test("test delete index table silently when exception occur") {
    setVariables("unknown")
    CarbonInternalMetastore.deleteIndexSilent(tableIdentifier, "",
      parentCarbonTable)(sqlContext.sparkSession)
    checkExistence(sql("show indexes on table1"), true, "index1")
    setVariables("index1")
    CarbonInternalMetastore.deleteIndexSilent(tableIdentifier, "",
      parentCarbonTable)(sqlContext.sparkSession)
    checkExistence(sql("show indexes on table1"), false, "index1")

    sql("drop index if exists index1 on table1")
    sql("create index index1 on table1(b) as 'carbondata'")
    setVariables("index1")
    // delete will fail as we are giving indexTable as parentTable.
    CarbonInternalMetastore.deleteIndexSilent(tableIdentifier, "",
      indexTable)(sqlContext.sparkSession)
    checkExistence(sql("show indexes on table1"), true, "index1")
  }

  test("test show index when SI were created before the change CARBONDATA-3765") {
    val mock: MockUp[IndexTableInfo] = new MockUp[IndexTableInfo]() {
      @Mock
      def getIndexProperties(): util.Map[String, String] = {
        null
      }
    }
    checkExistence(sql("show indexes on table1"), true, "index1")
    mock.tearDown()
  }

  test("test refresh index with different value of isIndexTableExists") {
    setVariables("index1")
    sql("create index index2 on table1(b) as 'bloomfilter'")
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("true"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("true"))
    CarbonIndexUtil.addOrModifyTableProperty(parentCarbonTable, Map("indexExists" -> "false",
      "indextableexists" -> "false"))(sqlContext.sparkSession)
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("false"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("false"))
    parentCarbonTable.getTableInfo.getFactTable.getTableProperties.remove("indextableexists")
    CarbonInternalMetastore.refreshIndexInfo(dbName, tableName,
      parentCarbonTable)(sqlContext.sparkSession)
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("true"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("true"))
  }

  test("test refresh index with indexExists as false and empty index table") {
    setVariables("index1")
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("false"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("true"))
    parentCarbonTable.getTableInfo.getFactTable.getTableProperties.remove("indextableexists")
    val mock: MockUp[CarbonIndexUtil.type ] = new MockUp[CarbonIndexUtil.type]() {
      @Mock
      def getSecondaryIndexes(carbonTable: CarbonTable): java.util.List[String] = {
        new java.util.ArrayList[String]
      }
    }
    CarbonInternalMetastore.refreshIndexInfo(dbName, tableName,
      parentCarbonTable)(sqlContext.sparkSession)
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("true"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("false"))
    mock.tearDown()
  }

  test("test refresh index with indexExists as null") {
    setVariables("index1")
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("false"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("true"))
    parentCarbonTable.getTableInfo.getFactTable.getTableProperties.remove("indextableexists")
    parentCarbonTable.getTableInfo.getFactTable.getTableProperties.remove("indexexists")
    CarbonInternalMetastore.refreshIndexInfo(dbName, tableName,
      parentCarbonTable)(sqlContext.sparkSession)
    parentCarbonTable = CarbonEnv.getCarbonTable(Some("test"), "table1")(sqlContext.sparkSession)
    assert(CarbonIndexUtil.isIndexExists(parentCarbonTable).equalsIgnoreCase("false"))
    assert(CarbonIndexUtil.isIndexTableExists(parentCarbonTable).equalsIgnoreCase("true"))
  }

  override def afterAll(): Unit = {
    sql("drop database if exists test cascade")
    sql("use default")
  }
}
