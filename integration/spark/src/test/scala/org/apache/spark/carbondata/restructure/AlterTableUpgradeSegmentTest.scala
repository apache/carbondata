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

package org.apache.spark.carbondata.restructure

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath

class AlterTableUpgradeSegmentTest extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    sql("drop table if exists altertest")
    sql("create table altertest(a string) STORED AS carbondata")
    sql("insert into altertest select 'k'")
    sql("insert into altertest select 'tttttt'")
  }

  private def removeDataAndIndexSizeFromTableStatus(table: CarbonTable): Unit = {
    val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(table.getTablePath))
    loadMetaDataDetails.foreach { loadMetaDataDetail =>
      loadMetaDataDetail.setIndexSize("0")
      loadMetaDataDetail.setDataSize("0")
    }
    SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
      .getTableStatusFilePath(table.getTablePath), loadMetaDataDetails)
  }

  test("test alter table upgrade segment test") {
    val carbonTable =
      CarbonEnv.getCarbonTable(TableIdentifier("altertest"))(sqlContext.sparkSession)
    removeDataAndIndexSizeFromTableStatus(carbonTable)
    val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(carbonTable.getTablePath))
    loadMetaDataDetails.foreach(detail => assert(detail.getIndexSize.toInt + detail.getDataSize
      .toInt == 0))
    sql("alter table altertest compact 'upgrade_segment'")
    val loadMetaDataDetailsNew = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(carbonTable.getTablePath))
    loadMetaDataDetailsNew.foreach{detail =>
      assert(detail.getIndexSize.toInt != 0)
      assert(detail.getDataSize.toInt != 0)}
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists altertest")
  }
}
