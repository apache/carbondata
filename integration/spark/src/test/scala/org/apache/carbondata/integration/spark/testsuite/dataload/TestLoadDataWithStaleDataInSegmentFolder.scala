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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.util

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
/**
 * Test class of creating and loading for carbon table with staledata in segment folder
 *
 */
class TestLoadDataWithStaleDataInSegmentFolder extends QueryTest with BeforeAndAfterAll {

  val tableName = "staleDataInSegmentFolder"
  val siName = "si_StaleDataInSegmentFolder"
  val testData = s"$resourcesPath/sample.csv"
  val sortcolumns = "id"

  test("test load with staledata in segmentfolder, " +
      "carbon.merge.index.in.segment = false") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    testIUDWithStaleData
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT)
  }

  test("test load with staledata in segmentfolder, " +
      "carbon.merge.index.in.segment = true") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    testIUDWithStaleData
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT)
  }

  private def testIUDWithStaleData(): Unit = {
    List("NO_SORT", "LOCAL_SORT", "GLOBAL_SORT").foreach(sort => {
      List(true, false).foreach(isPartition => {
        testLoadWithStaleData(sort, sortcolumns, isPartition)
        testSIWithStaleData(sort, sortcolumns, isPartition)
        testInsertIntoWithStaleData(sort, sortcolumns, isPartition)
        testUpdateWithStaleData(sort, sortcolumns, isPartition)
        testDeleteWithStaleData(sort, sortcolumns, isPartition)
        testCompactWithStaleData(sort, sortcolumns, isPartition)
      })
    })
  }

  private def testLoadWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    mockStaleDataByRemoveTablestatus(tableName)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    verifyThereIsNoSameContentInDifferentIndexes(tableName, "0")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(6)))
    sql(s"clean files for table $tableName")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(6)))
  }

  private def testCompactWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    mockStaleDataByRemoveTablestatus(tableName)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    sql(s"ALTER TABLE $tableName COMPACT 'MINOR'")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(24)))
    sql(s"clean files for table $tableName")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(24)))
  }

  private def testSIWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    createSI()
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    mockStaleDataByRemoveTablestatus(tableName)
    mockStaleDataByRemoveTablestatus(siName)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $tableName")
    verifyThereIsNoSameContentInDifferentIndexes(tableName, "0")
    verifyThereIsNoSameContentInDifferentIndexes(siName, "0")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(6)))
    if (isPartition) {
      checkAnswer(sql(s"select count(1) from $siName"), Seq(Row(6)))
    } else {
      checkAnswer(sql(s"select count(1) from $siName"), Seq(Row(4)))
    }
    sql(s"clean files for table $tableName")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(6)))
    if (isPartition) {
      checkAnswer(sql(s"select count(1) from $siName"), Seq(Row(6)))
    } else {
      checkAnswer(sql(s"select count(1) from $siName"), Seq(Row(4)))
    }
  }

  private def testInsertIntoWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    sql(s"INSERT INTO $tableName values(1, 'a', 'b', 2)")
    mockStaleDataByRemoveTablestatus(tableName)
    sql(s"INSERT INTO $tableName values(1, 'a', 'c', 2)")
    verifyThereIsNoSameContentInDifferentIndexes(tableName, "0")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(1)))
    sql(s"clean files for table $tableName")
    checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(1)))
  }

  private def testUpdateWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    sql(s"INSERT INTO $tableName values(1, 'a', 'b', 2)")
    mockStaleDataByRemoveTablestatus(tableName)
    sql(s"INSERT INTO $tableName values(1, 'a', 'c', 2)")
    sql("""update staleDataInSegmentFolder d  set
        | (d.id) = (d.id + 1) where d.name = 'a'""".stripMargin).collect()
    checkAnswer(sql(s"select * from $tableName"),
      Seq(Row(2, "a", "c", 2)))
  }

  private def testDeleteWithStaleData(sortscope: String, sortcolumns: String,
      isPartition: Boolean): Unit = {
    createTable(sortscope, sortcolumns, isPartition)
    sql(s"INSERT INTO $tableName values(1, 'a', 'b', 2)")
    mockStaleDataByRemoveTablestatus(tableName)
    sql(s"INSERT INTO $tableName values(1, 'a', 'c', 2)")
    sql("""delete from staleDataInSegmentFolder d where d.city = 'c'""".stripMargin).collect()
    checkAnswer(sql(s"select * from $tableName"), Seq())
  }

  override def afterAll: Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  private def createTable(sortscope: String, sortcolumns: String, isPartition: Boolean): Unit = {
    if (!isPartition) {
      createNonPartitionTable(sortscope, sortcolumns)
    } else {
      createPartitionTable(sortscope, sortcolumns)
    }
  }

  private def createNonPartitionTable(sortscope: String, sortcolumns: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         CREATE TABLE $tableName(id int, name string, city string, age int)
         STORED AS carbondata
         TBLPROPERTIES('sort_scope'='$sortscope','sort_columns'='$sortcolumns')
      """)
  }

  private def createPartitionTable(sortscope: String, sortcolumns: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         CREATE TABLE $tableName(id int, name string, city string)
         PARTITIONED BY(age int)
         STORED AS carbondata
         TBLPROPERTIES('sort_scope'='$sortscope','sort_columns'='$sortcolumns')
      """)
  }

  private def createSI(): Unit = {
    sql(s"create index $siName on table $tableName (city) AS 'carbondata'")
  }

  private def mockStaleDataByRemoveTablestatus(tableName: String): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", tableName)
    val tableStatusFile = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath)
    FileFactory.getCarbonFile(tableStatusFile).delete()
  }

  private def verifyThereIsNoSameContentInDifferentIndexes(tableName: String,
      segment: String): Unit = {
    val table = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    var path = CarbonTablePath.
      getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    if (table.isHivePartitionTable) {
      path = table.getAbsoluteTableIdentifier.getTablePath
    }

    val allIndexFilesSet = new util.HashSet[String]()
    val allIndexFilesList = new util.ArrayList[String]()

    FileFactory.getCarbonFile(path).listFiles(true, new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT) ||
          file.getName.endsWith(CarbonTablePath.INDEX_FILE_EXT)
      }
    }).asScala.map(indexFile => {
      val ssim = new SegmentIndexFileStore()
      if (indexFile.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        ssim.readMergeFile(indexFile.getAbsolutePath)
      } else {
        ssim.readIndexFile(indexFile)
      }
      allIndexFilesSet.addAll(ssim.getCarbonIndexMapWithFullPath.keySet())
      allIndexFilesList.addAll(ssim.getCarbonIndexMapWithFullPath.keySet())
    })
    assert(allIndexFilesList.size() == allIndexFilesSet.size())
  }
}
