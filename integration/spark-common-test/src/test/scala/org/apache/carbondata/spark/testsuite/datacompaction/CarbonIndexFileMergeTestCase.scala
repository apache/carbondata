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

import org.junit.Assert

import scala.collection.JavaConverters._

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter

class CarbonIndexFileMergeTestCase
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
  }

  override protected def afterAll(): Unit = {
    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
  }

  test("Verify correctness of index merge") {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "indexmerge")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    assert(getIndexFileCount("default_indexmerge", "0") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""),
      sql("""Select count(*) from indexmerge"""))
  }

  test("Verify command of index merge") {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "nonindexmerge")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("1", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify command of index merge without enabling property") {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "nonindexmerge")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("1", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge with compaction") {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "nonindexmerge")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0.1", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge for compacted segments") {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "nonindexmerge")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0.1", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Query should not fail after iud operation on a table having merge indexes") {
    sql("drop table if exists mitable")
    sql("create table mitable(id int, issue date) stored by 'carbondata'")
    sql("insert into table mitable select '1','2000-02-01'")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "mitable")
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    sql("update mitable set(id)=(2) where issue = '2000-02-01'").show()
    sql("clean files for table mitable")
    sql("select * from mitable").show()
  }

  // CARBONDATA-2704, test the index file size after merge
  test("Verify the size of the index file after merge") {
    sql("DROP TABLE IF EXISTS fileSize")
    sql(
      """
        | CREATE TABLE fileSize(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE fileSize OPTIONS('header'='false')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "fileSize")
    var loadMetadataDetails = SegmentStatusManager
      .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(table.getTablePath))
    var segment0 = loadMetadataDetails.filter(x=> x.getLoadName.equalsIgnoreCase("0"))
    Assert
      .assertEquals(getIndexOrMergeIndexFileSize(table, "0", CarbonTablePath.INDEX_FILE_EXT),
        segment0.head.getIndexSize.toLong)
    new CarbonIndexFileMergeWriter(table)
      .mergeCarbonIndexFilesOfSegment("0", table.getTablePath, false, String.valueOf(System.currentTimeMillis()))
    loadMetadataDetails = SegmentStatusManager
      .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(table.getTablePath))
    segment0 = loadMetadataDetails.filter(x=> x.getLoadName.equalsIgnoreCase("0"))
    Assert
      .assertEquals(getIndexOrMergeIndexFileSize(table, "0", CarbonTablePath.MERGE_INDEX_FILE_EXT),
        segment0.head.getIndexSize.toLong)
    sql("DROP TABLE IF EXISTS fileSize")
  }

  private def getIndexFileCount(tableName: String, segmentNo: String): Int = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableName)
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentNo)
    if (FileFactory.isFileExist(segmentDir)) {
      val indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir)
      indexFiles.asScala.map { f =>
        if (f._2 == null) {
          1
        } else {
          0
        }
      }.sum
    } else {
      val segment = Segment.getSegment(segmentNo, carbonTable.getTablePath)
      if (segment != null) {
        val store = new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName)
        store.getSegmentFile.getLocationMap.values().asScala.map { f =>
          if (f.getMergeFileName == null) {
            f.getFiles.size()
          } else {
            0
          }
        }.sum
      } else {
        0
      }
    }
  }

  private def getIndexOrMergeIndexFileSize(carbonTable: CarbonTable,
      segmentId: String,
      fileExtension: String): Long = {
    var size = 0L;
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentId)
    val segmentFile = FileFactory.getCarbonFile(segmentPath)
    val carbonFiles = segmentFile.listFiles(new CarbonFileFilter() {
      override def accept(file: CarbonFile): Boolean = {
        (file.getName.endsWith(fileExtension))
      }
    })
    carbonFiles.toList.foreach(carbonFile => {
      size += FileFactory.getCarbonFile(carbonFile.getPath).getSize
    })
    size
  }

}
