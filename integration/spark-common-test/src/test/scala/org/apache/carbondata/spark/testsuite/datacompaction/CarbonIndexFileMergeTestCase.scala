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

import java.util

import scala.collection.JavaConverters._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

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
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_indexmerge", "0") == 0)
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify command of index merge without enabling property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
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
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
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
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("ALTER TABLE nonindexmerge COMPACT 'segment_index'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Query should not fail after iud operation on a table having merge indexes") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("drop table if exists mitable")
    sql("create table mitable(id int, issue date) stored by 'carbondata'")
    sql("insert into table mitable select '1','2000-02-01'")
    sql("update mitable set(id)=(2) where issue = '2000-02-01'").show()
    sql("clean files for table mitable")
    sql("select * from mitable").show()
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  // CARBONDATA-2704, test the index file size after merge
  test("Verify the size of the index file after merge") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
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
    sql("Alter table fileSize compact 'segment_index'")
    loadMetadataDetails = SegmentStatusManager
      .readTableStatusFile(CarbonTablePath.getTableStatusFilePath(table.getTablePath))
    segment0 = loadMetadataDetails.filter(x=> x.getLoadName.equalsIgnoreCase("0"))
    Assert
      .assertEquals(getIndexOrMergeIndexFileSize(table, "0", CarbonTablePath.MERGE_INDEX_FILE_EXT),
        segment0.head.getIndexSize.toLong)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.2") == 0)
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
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='100')"
    )
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(3000000)))
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
    s"'GLOBAL_SORT_PARTITIONS'='100')"
    )
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "2.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(3000000)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "false")
    CarbonProperties.getInstance()
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
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE partitionTable OPTIONS('header'='false')")
    assert(getIndexFileCount("default_partitionTable", "0") == 0)
    sql("DROP TABLE IF EXISTS partitionTable")
  }

  ignore("Verify index merge for pre-aggregate table") {
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

  test("Verify index merge for streaming table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS streamingTable")
    sql(
      """
        | CREATE TABLE streamingTable(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'streaming'='true')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE streamingTable OPTIONS('header'='false')")
    assert(getIndexFileCount("default_streamingTable", "0") >= 1)
    val exceptionMessage = intercept[Exception] {
      sql("alter table streamingTable compact 'segment_index'")
    }.getMessage
    assert(exceptionMessage.contains("Unsupported alter operation on carbon table: Merge index is not supported on streaming table"))
    sql("DROP TABLE IF EXISTS streamingTable")
  }

  test("verify driver cache gets updated after creating merge Index file") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS merge_index_cache")
    sql(
      """
        | CREATE TABLE merge_index_cache(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
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

  private def mergeFileNameIsNull(segmentId: String, dbName: String, tableName: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    val dataMapFactory = DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable)
      .getDataMapFactory
    val method = classOf[BlockletDataMapFactory]
      .getDeclaredMethod("getTableBlockIndexUniqueIdentifiers", classOf[Segment])
    method.setAccessible(true)
    val segment = new Segment(segmentId)
    val identifiers = method.invoke(dataMapFactory, segment)
      .asInstanceOf[util.Set[TableBlockIndexUniqueIdentifier]].asScala
    assert(identifiers.size == 1)
    identifiers.forall(identifier => identifier.getMergeIndexFileName == null)
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
