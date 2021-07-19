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
package org.apache.carbondata.spark.testsuite.mergeindex

import java.io.{File, PrintWriter}

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTestUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath

class CarbonIndexFileMergeTestCaseWithSI
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 15000
    createFile(file2, n * 4, n)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("use default")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
  }

  override protected def afterAll(): Unit = {
    deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE,
        CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE_DEFAULT)
  }

  test("Verify correctness of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql("CREATE INDEX nonindexmerge_index on table nonindexmerge (name) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index", "0") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql("CREATE INDEX indexmerge_index1 on table indexmerge (name) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    assert(CarbonTestUtil.getIndexFileCount("default_indexmerge", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_indexmerge_index1", "0") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""),
      sql("""Select count(*) from indexmerge"""))
  }

  ignore("Verify command of index merge") {
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
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index1", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index1", "1") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index1", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index1", "1") == 0)
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
    sql("CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index2", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index2", "1") == 20)
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index2", "0") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index2", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
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
    sql("CREATE INDEX nonindexmerge_index3 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index3", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index3", "1") == 20)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index3", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Verify index index merge for compacted segments") {
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
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    sql("CREATE INDEX nonindexmerge_index4 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "3") == 20)
    sql("alter table nonindexmerge set tblproperties('global_sort_partitions'='20')")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("ALTER TABLE nonindexmerge COMPACT 'segment_index'").collect()
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "2.1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "0") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "2") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "3") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "0.1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "2.1") == 20)
    assert(CarbonTestUtil.getIndexFileCount("default_nonindexmerge_index4", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(fileName);
      for (i <- start until (start + line)) {
        // scalastyle:off println
        write
          .println(i + "," + "n" + i + "," + "c" + Random.nextInt(line) + "," + Random.nextInt(80))
        // scalastyle:on println
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  private def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }

}
