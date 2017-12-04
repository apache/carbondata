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

package org.apache.carbondata.spark.testsuite.concurrent

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadTableConcurrentScenario extends QueryTest with BeforeAndAfterAll {

  var carbonTable: CarbonTable = _
  var metaPath: String = _

  override def beforeAll {
    sql("use default")
    sql("drop table if exists drop_concur")
    sql("drop table if exists rename_concur")
  }

  test("do not allow drop table when load is in progress") {
    sql("create table drop_concur(id int, name string) stored by 'carbondata'")
    sql("insert into drop_concur select 1,'abc'")
    sql("insert into drop_concur select 1,'abc'")
    sql("insert into drop_concur select 1,'abc'")

    carbonTable = CarbonEnv.getCarbonTable(Option("default"), "drop_concur")(sqlContext.sparkSession)
    metaPath = carbonTable.getMetaDataFilepath
    val listOfLoadFolderDetailsArray = SegmentStatusManager.readLoadMetadata(metaPath)
    listOfLoadFolderDetailsArray(1).setSegmentStatus(SegmentStatus.INSERT_IN_PROGRESS)

    try {
      sql("drop table drop_concur")
    } catch {
      case ex: Throwable => assert(ex.getMessage.contains("Cannot drop table, load or insert overwrite is in progress"))
    }
  }

  test("do not allow rename table when load is in progress") {
    sql("create table rename_concur(id int, name string) stored by 'carbondata'")
    sql("insert into rename_concur select 1,'abc'")
    sql("insert into rename_concur select 1,'abc'")

    carbonTable = CarbonEnv.getCarbonTable(Option("default"), "rename_concur")(sqlContext.sparkSession)
    metaPath = carbonTable.getMetaDataFilepath
    val listOfLoadFolderDetailsArray = SegmentStatusManager.readLoadMetadata(metaPath)
    listOfLoadFolderDetailsArray(1).setSegmentStatus(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS)

    try {
      sql("alter table rename_concur rename to rename_concur1")
    } catch {
      case ex: Throwable => assert(ex.getMessage.contains("alter rename failed, load, insert or insert overwrite " +
        "is in progress for the table"))
    }
  }

  override def afterAll: Unit = {
    sql("use default")
    sql("drop table if exists drop_concur")
    sql("drop table if exists rename_concur")
  }
}
