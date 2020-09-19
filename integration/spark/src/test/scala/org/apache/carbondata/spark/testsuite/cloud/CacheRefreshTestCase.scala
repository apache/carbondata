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

package org.apache.carbondata.spark.testsuite.cloud

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonHiveIndexMetadataUtil
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CacheRefreshTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop database if exists cachedb cascade")
    sql("create database cachedb")
    sql("use cachedb")
  }

  override protected def afterAll(): Unit = {
    sql("use default")
    sql("drop database if exists cachedb cascade")
  }

  test("test cache refresh") {
    sql("create table tbl_cache1(col1 string, col2 int, col3 int) using carbondata")
    sql("insert into tbl_cache1 select 'a', 123, 345")
    CarbonHiveIndexMetadataUtil.invalidateAndDropTable(
      "cachedb", "tbl_cache1", sqlContext.sparkSession)
    // discard cached table info in cachedDataSourceTables
    val tableIdentifier = TableIdentifier("tbl_cache1", Option("cachedb"))
    sqlContext.sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    sql("create table tbl_cache1(col1 string, col2 int, col3 int) using carbondata")
    sql("delete from tbl_cache1")
    sql("insert into tbl_cache1 select 'b', 123, 345")
    checkAnswer(sql("select * from tbl_cache1"),
      Seq(Row("b", 123, 345)))
  }
}
