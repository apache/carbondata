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

package org.apache.spark.carbondata

import java.io.IOException

import mockit.{Mock, MockUp}
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class TableStatusBackupTest extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_TABLE_STATUS_BACKUP, "true")
    sql("drop table if exists source")
    sql("create table source(a string) stored as carbondata")
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists source")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_TABLE_STATUS_BACKUP, "false")
  }

  test("backup table status file") {
    sql("insert into source values ('A'), ('B')")
    val tablePath = CarbonEnv.getCarbonTable(None, "source")(sqlContext.sparkSession).getTablePath
    val tableStatusFilePath = CarbonTablePath.getTableStatusFilePath(tablePath)
    val oldTableStatus = SegmentStatusManager.readTableStatusFile(tableStatusFilePath)

    var mock = new MockUp[SegmentStatusManager]() {
      @Mock
      @throws[IOException]
      def mockForTest(): Unit = {
        throw new IOException("thrown in mock")
      }
    }

    val exception = intercept[IOException] {
      sql("insert into source values ('A'), ('B')")
    }
    assert(exception.getMessage.contains("thrown in mock"))
    val backupPath = tableStatusFilePath + ".backup"
    assert(FileFactory.isFileExist(backupPath))
    val backupTableStatus = SegmentStatusManager.readTableStatusFile(backupPath)
    assertResult(oldTableStatus)(backupTableStatus)

    mock = new MockUp[SegmentStatusManager]() {
      @Mock
      def mockForTest(): Unit = {
      }
    }
  }
}