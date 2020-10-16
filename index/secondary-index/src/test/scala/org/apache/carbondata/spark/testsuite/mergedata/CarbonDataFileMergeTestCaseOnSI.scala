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
package org.apache.carbondata.spark.testsuite.mergedata

import java.io.{File, IOException, PrintWriter}
import java.util

import mockit.{Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{AnalysisException, CarbonEnv}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import scala.util.Random

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.merger.{CarbonCompactionExecutor, CarbonCompactionUtil}
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

class CarbonDataFileMergeTestCaseOnSI
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 16000
    createFile(file2, n * 4, n)
    sql("drop database if exists dataFileMerge cascade")
    sql("create database dataFileMerge")
    sql("use dataFileMerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
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
    sql("use default")
    sql("drop database if exists dataFileMerge cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE,
        CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE_DEFAULT)
  }

  test("Verify correctness of data file merge") {
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      "CREATE INDEX indexmerge_index1 on table indexmerge (name) AS 'carbondata' properties" +
      "('table_blocksize'='1')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='20')")
    val rows = sql("""Select count(*) from indexmerge where name='n164419'""").collect()
    checkAnswer(sql("""Select count(*) from indexmerge where name='n164419'"""), rows)
    assert(getDataFileCount("indexmerge_index1", "0") < 7)
  }

  test("Verify command of data file merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("20", 2)
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
      "CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata' " +
      "properties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    assert(getDataFileCount("nonindexmerge_index1", "0") > 7)
    assert(getDataFileCount("nonindexmerge_index1", "1") > 7)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
  }

  test("Verify command of data file merge on segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("20", 2)
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata' " +
    "properties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("REFRESH INDEX nonindexmerge_index2 ON TABLE nonindexmerge WHERE SEGMENT.ID IN(0)")
      .collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    assert(getDataFileCount("nonindexmerge_index2", "0") > 7)
    assert(getDataFileCount("nonindexmerge_index2", "1") == 20)
    sql("REFRESH INDEX nonindexmerge_index2 ON TABLE nonindexmerge WHERE SEGMENT.ID IN(1)")
      .collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    assert(getDataFileCount("nonindexmerge_index2", "1") > 7)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
  }

  test("Verify command of REFRESH INDEX command with invalid segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("20", 1)
    sql(
      "CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata' " +
      "properties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    val exceptionMessage = intercept[RuntimeException] {
      sql("REFRESH INDEX nonindexmerge_index2 ON TABLE nonindexmerge WHERE SEGMENT.ID IN(1,2)")
        .collect()
    }.getMessage
    assert(exceptionMessage.contains("Refresh index by segment id is failed. Invalid ID:"))
  }

  test("Verify index data file merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("20", 2)
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index3 on table nonindexmerge (name) AS 'carbondata' " +
    "properties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getDataFileCount("nonindexmerge_index3", "0.1") < 11)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
        .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
  }

  test("Verify index data file merge for compacted segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    createTableAndLoadData("20", 4)
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index4 on table nonindexmerge (name) AS 'carbondata' " +
    "properties('table_blocksize'='1')")
    assert(getDataFileCount("nonindexmerge_index4", "0.2") < 15)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("Verify data file merge in SI segments with sort scope as gloabl sort and" +
    "CARBON_SI_SEGMENT_MERGE property is enabled") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    createTableAndLoadData("100", 2)
    val rows = sql(" select count(*) from nonindexmerge").collect()
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata' " +
        "properties('table_blocksize'='1', 'SORT_SCOPE'='GLOBAL_SORT')")
    // number of rows in main table and SI should be same
    checkAnswer(sql(" select count(*) from nonindexmerge_index1"), rows)
   val df1 = sql("""Select * from nonindexmerge where name='n16000'""")
     .queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    assert(getDataFileCount("nonindexmerge_index1", "0") < 15)
    assert(getDataFileCount("nonindexmerge_index1", "1") < 15)
  }

  test("Verify REFRESH INDEX command with sort scope as global sort") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("100", 2)
    val rows = sql(" select count(*) from nonindexmerge").collect()
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata' " +
      "properties('table_blocksize'='1', 'SORT_SCOPE'='GLOBAL_SORT')")
    val result = sql(" select positionReference from nonindexmerge_index1 where name = 'n16010'")
      .collect()
    sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    // value of positionReference column should be same before and after merge
    checkAnswer(sql(" select positionReference from nonindexmerge_index1 where name = 'n16010'"),
      result)
    // number of rows in main table and SI should be same
    checkAnswer(sql(" select count(*) from nonindexmerge_index1"), rows)
    val df1 = sql("""Select * from nonindexmerge where name='n16000'""")
      .queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    assert(getDataFileCount("nonindexmerge_index1", "0") > 15)
    assert(getDataFileCount("nonindexmerge_index1", "1") > 15)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants
        .CARBON_SI_SEGMENT_MERGE, "true")
  }

  def createTableAndLoadData(globalSortPartition: String, loadTimes: Int): Unit = {
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    for (_ <- 0 until loadTimes) {
      sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
          s"'GLOBAL_SORT_PARTITIONS'='$globalSortPartition')")
    }
  }

  test("test verify data file merge when exception occurred in rebuild segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("100", 2)
    val rows = sql(" select count(*) from nonindexmerge").collect()
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata'")
    // when merge data file will throw the exception
    val mock1 = TestSecondaryIndexUtils.mockDataFileMerge()
    val ex = intercept[RuntimeException] {
      sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    }
    mock1.tearDown()
    assert(ex.getMessage.contains("An exception occurred while merging data files in SI"))
    var df1 = sql("""Select * from nonindexmerge where name='n16000'""")
      .queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    assert(getDataFileCount("nonindexmerge_index1", "0") == 100)
    assert(getDataFileCount("nonindexmerge_index1", "1") == 100)
    // not able to acquire lock on table
    val mock2 = TestSecondaryIndexUtils.mockTableLock()
    val exception = intercept[AnalysisException] {
      sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    }
    mock2.tearDown()
    assert(exception.getMessage.contains("Table is already locked for compaction. " +
      "Please try after some time."))
    df1 = sql("""Select * from nonindexmerge where name='n16000'""")
      .queryExecution.sparkPlan
    assert(getDataFileCount("nonindexmerge_index1", "0") == 100)
    assert(getDataFileCount("nonindexmerge_index1", "1") == 100)

    // exception is thrown by compaction executor
    val mock3: MockUp[CarbonCompactionExecutor] = new MockUp[CarbonCompactionExecutor]() {
      @Mock
      def processTableBlocks(configuration: Configuration, filterExpr: Expression):
      util.Map[String, util.List[RawResultIterator]] = {
        throw new IOException("An exception occurred while compaction executor.")
      }
    }
    val exception2 = intercept[Exception] {
      sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    }
    mock3.tearDown()
    assert(exception2.getMessage.contains("Merge data files Failure in Merger Rdd."))
    df1 = sql("""Select * from nonindexmerge where name='n16000'""")
        .queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    assert(getDataFileCount("nonindexmerge_index1", "0") == 100)
    assert(getDataFileCount("nonindexmerge_index1", "1") == 100)
    checkAnswer(sql(" select count(*) from nonindexmerge_index1"), rows)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants
        .CARBON_SI_SEGMENT_MERGE, "true")
  }

  test("test refresh index command when block need to be sorted") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    createTableAndLoadData("100", 2)
    val rows = sql(" select count(*) from nonindexmerge").collect()
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata'")
    val mock: MockUp[CarbonCompactionUtil] = new MockUp[CarbonCompactionUtil]() {
      @Mock
      def isSortedByCurrentSortColumns(table: CarbonTable, footer: DataFileFooter): Boolean = {
        false
      }
    }
    sql("REFRESH INDEX nonindexmerge_index1 ON TABLE nonindexmerge").collect()
    mock.tearDown()
    val df1 = sql("""Select * from nonindexmerge where name='n16000'""")
        .queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df1))
    assert(getDataFileCount("nonindexmerge_index1", "0") > 15)
    assert(getDataFileCount("nonindexmerge_index1", "1") > 15)
    checkAnswer(sql(" select count(*) from nonindexmerge_index1"), rows)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants
        .CARBON_SI_SEGMENT_MERGE, "true")
  }

  private def getDataFileCount(tableName: String, segment: String): Int = {
    val table = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    val path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    val carbonFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(CarbonTablePath
        .CARBON_DATA_EXT)
    })
    if (carbonFiles != null) {
      carbonFiles.length
    } else {
      0
    }
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
