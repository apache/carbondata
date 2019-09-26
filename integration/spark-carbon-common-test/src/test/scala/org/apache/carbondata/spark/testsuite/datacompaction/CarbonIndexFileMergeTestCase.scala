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

package org.apache.carbondata.spark.testsuite.datacompaction

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

import org.apache.spark.sql.test.util.CarbonQueryTest

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CarbonIndexFileMergeTestCase
  extends CarbonQueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
  }

  override protected def afterAll(): Unit = {
    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql("DROP TABLE IF EXISTS merge_index_cache")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)
  }

  test("Verify index merge for pre-aggregate table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS preAggTable")
    sql(
      """
        | CREATE TABLE preAggTable(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE preAggTable OPTIONS('header'='false')")
    assert(getIndexFileCount("default_preAggTable", "0") == 0)
    sql("create datamap preAggSum on table preAggTable using 'preaggregate' as " +
        "select city,sum(age) as sum from preAggTable group by city")
    assert(getIndexFileCount("default_preAggTable_preAggSum", "0") == 0)
    sql("DROP TABLE IF EXISTS partitionTable")
  }

  private def getIndexFileCount(tableName: String, segment: String): Int = {
    val table = CarbonMetadata.getInstance().getCarbonTable(tableName)
    val path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    val carbonFiles = if (table.isHivePartitionTable) {
      FileFactory.getCarbonFile(table.getAbsoluteTableIdentifier.getTablePath)
        .listFiles(true, new CarbonFileFilter {
          override def accept(file: CarbonFile): Boolean = {
            file.getName.endsWith(CarbonTablePath
              .INDEX_FILE_EXT)
          }
        })
    } else {
      FileFactory.getCarbonFile(path).listFiles(true, new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          file.getName.endsWith(CarbonTablePath
            .INDEX_FILE_EXT)
        }
      })
    }
    if (carbonFiles != null) {
      carbonFiles.size()
    } else {
      0
    }
  }
}
