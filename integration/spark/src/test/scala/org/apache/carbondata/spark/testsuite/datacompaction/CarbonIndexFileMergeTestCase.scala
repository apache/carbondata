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

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import mockit.{Mock, MockUp}
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexFactory
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTestUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class CarbonIndexFileMergeTestCase
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 15000
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

  test("Verify correctness of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    assert(CarbonTestUtil.getIndexFileCount("default_indexmerge", "0") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""),
      sql("""Select count(*) from indexmerge"""))
  }

  test("Verify command of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getSegmentFileCount("default_nonindexmerge") == 2)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    // creates new segment file instead of updating
    assert(CarbonTestUtil.getSegmentFileCount("default_nonindexmerge") == 4)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify command of index merge without enabling property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Verify index merge for compacted segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,3")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("ALTER TABLE nonindexmerge COMPACT 'segment_index'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Query should not fail after iud operation on a table having merge indexes") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("drop table if exists mitable")
    sql("create table mitable(id int, issue date) STORED AS carbondata")
    sql("insert into table mitable select '1','2000-02-01'")
    sql("update mitable set(id)=(2) where issue = '2000-02-01'").collect()
    sql("clean files for table mitable")
    sql("select * from mitable")collect()
}

  test("Verify index merge for compacted segments MINOR") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,3")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  // CARBONDATA-2704, test the index file size after merge
  test("Verify the size of the index file after merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS fileSize")
    sql(
      """
        | CREATE TABLE fileSize(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE fileSize OPTIONS('header'='false')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "fileSize")
    var loadMetadataDetails = SegmentStatusManager
      .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(table.getTablePath))
    var segment0 = loadMetadataDetails.filter(x => x.getLoadName.equalsIgnoreCase("0"))
    Assert
      .assertEquals(getIndexOrMergeIndexFileSize(table, "0", CarbonTablePath.INDEX_FILE_EXT),
        segment0.head.getIndexSize.toLong)
    sql("Alter table fileSize compact 'segment_index'")
    loadMetadataDetails = SegmentStatusManager
      .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(table.getTablePath))
    segment0 = loadMetadataDetails.filter(x => x.getLoadName.equalsIgnoreCase("0"))
    Assert
      .assertEquals(getIndexOrMergeIndexFileSize(table, "0", CarbonTablePath.MERGE_INDEX_FILE_EXT),
        segment0.head.getIndexSize.toLong)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS fileSize")
  }

  test("Verify index merge for compacted segments MINOR - level 2") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index merge for compacted segments Auto Compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,3")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')"
    )
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "4") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(300000)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("Verify index merge for compacted segments Auto Compaction - level 2") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='20')"
    )
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "4") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(300000)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }


  test("Verify index merge for partition table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS partitionTable")
    sql(
      """
        | CREATE TABLE partitionTable(id INT, name STRING, city STRING)
        | PARTITIONED BY(age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE partitionTable OPTIONS('header'='false')")
    assert(CarbonTestUtil.getIndexFileCount("default_partitionTable", "0") == 0)
    sql("DROP TABLE IF EXISTS partitionTable")
  }

  test("Verify command of index merge for partition table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING)
        | PARTITIONED BY(age INT)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge " +
      s"partition(age='20') OPTIONS('header'='false')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 1)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0",
      CarbonTablePath.MERGE_INDEX_FILE_EXT) == 1)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index merge for streaming table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS streamingTable")
    sql(
      """
        | CREATE TABLE streamingTable(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'streaming'='true')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE streamingTable OPTIONS('header'='false')")
    // check for one merge index file
    assert(
      CarbonTestUtil.getIndexFileCount("default_streamingTable",
        "0", CarbonTablePath.MERGE_INDEX_FILE_EXT) == 1)
    sql("DROP TABLE IF EXISTS streamingTable")
  }

  test("Verify alter table index merge for streaming table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS streamingTable")
    sql(
      """
        | CREATE TABLE streamingTable(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'streaming'='true')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE streamingTable OPTIONS('header'='false')")
    // check for zero merge index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable",
      "0", CarbonTablePath.MERGE_INDEX_FILE_EXT) == 0)
    // check for one index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable", "0") == 1)
    sql("alter table streamingTable compact 'segment_index'")
    sql("alter table streamingTable compact 'segment_index' where segment.id in (0)")
    // check for one merge index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable",
      "0", CarbonTablePath.MERGE_INDEX_FILE_EXT) == 1)
    sql("DROP TABLE IF EXISTS streamingTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
  }

  test("Verify alter table index merge for streaming table with custom segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS streamingTable")
    sql(
      """
        | CREATE TABLE streamingTable(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'streaming'='true')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE streamingTable OPTIONS('header'='false')")
    // check for zero merge index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable",
      "0", CarbonTablePath.MERGE_INDEX_FILE_EXT) == 0)
    // check for one index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable", "0") == 1)
    sql("alter table streamingTable compact 'segment_index' where segment.id in (0)")
    // check for one merge index file
    assert(CarbonTestUtil.getIndexFileCount("default_streamingTable",
      "0", CarbonTablePath.MERGE_INDEX_FILE_EXT) == 1)
    sql("DROP TABLE IF EXISTS streamingTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
  }

  test("verify driver cache gets updated after creating merge Index file") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS merge_index_cache")
    sql(
      """
        | CREATE TABLE merge_index_cache(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE merge_index_cache OPTIONS('header'='false')")
    sql("""Select count(*) from merge_index_cache""").collect()
    // merge Index fileName should be null as merge Index file is not created
    assert(mergeFileNameIsNull("0", "default", "merge_index_cache"))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE merge_index_cache COMPACT 'SEGMENT_INDEX'")
    sql("""Select count(*) from merge_index_cache""").collect()
    // once merge file is created cache should be refreshed in the same session and identifiers
    // should contain mergeIndex file name
    assert(!mergeFileNameIsNull("0", "default", "merge_index_cache"))
  }

  test("verify load when merge index fails") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    val mockMethod = new MockUp[CarbonIndexFileMergeWriter]() {
      @Mock
      def writeMergeIndexFileBasedOnSegmentFolder
      (indexFileNamesTobeAdded: util.List[String], isOldStoreIndexFilesPresent: Boolean,
       segmentPath: String, segmentId: String, uuid: String, readBasedOnUUID: Boolean): String = {
        throw new IOException("mock failure reason")
      }
    }
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    intercept[RuntimeException] {
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false')")
    }
    checkAnswer(sql("Select count(*) from indexmerge"), Seq(Row(0)))
    sql("DROP TABLE indexmerge")
    mockMethod.tearDown()
  }

  test("verify load when merge index fails for partition table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    val mockMethod = new MockUp[CarbonLoaderUtil]() {
      @Mock
      def mergeIndexFilesInPartitionedTempSegment
      (table: CarbonTable, segmentId: String, partitionPath: String,
          partitionInfo: util.List[String], uuid: String, tempFolderPath: String,
          currPartitionSpec: String): SegmentFileStore.FolderDetails = {
        throw new IOException("mock failure reason")
      }
    }
    sql("DROP TABLE IF EXISTS indexmergePartition")
    sql(
      """
        | CREATE TABLE indexmergePartition(id INT, name STRING, city STRING)
        | STORED AS carbondata partitioned by(age INT)
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    intercept[RuntimeException] {
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmergePartition " +
          s"OPTIONS('header'='false')")
    }
    checkAnswer(sql("Select count(*) from indexmergePartition"), Seq(Row(0)))
    sql("DROP TABLE indexmergePartition")
    mockMethod.tearDown()
  }

  private def mergeFileNameIsNull(segmentId: String, dbName: String, tableName: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    val indexFactory = IndexStoreManager.getInstance().getDefaultIndex(carbonTable)
      .getIndexFactory
    val method = classOf[BlockletIndexFactory]
      .getDeclaredMethod("getTableBlockIndexUniqueIdentifiers", classOf[Segment])
    method.setAccessible(true)
    val segment = new Segment(segmentId)
    val identifiers = method.invoke(indexFactory, segment)
      .asInstanceOf[util.Set[TableBlockIndexUniqueIdentifier]].asScala
    assert(identifiers.size == 1)
    identifiers.forall(identifier => identifier.getMergeIndexFileName == null)
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
