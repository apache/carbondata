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

package org.apache.carbondata.spark.testsuite.cleanfiles

import java.io.{File, PrintWriter}

import scala.io.Source

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.util.SparkSQLUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties, CarbonTestUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil

class TestCleanFileCommand extends QueryTest with BeforeAndAfterAll {

  var count = 0

  test("clean up table and test trash folder with IN PROGRESS segments with trash time = 0") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS, "0")
    // do not send the segment folders to trash
    createTable()
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    editTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))

    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == 4)
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest " +
      s"OPTIONS('stale_inprogress'='true','dryrun'='true')").collect()
    val cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest" +
      s" OPTIONS('stale_inprogress'='true')").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    assert(4 == segmentNumber2)
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assert(getFileCountInTrashFolder(trashFolderPath) == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("clean up table and test trash folder with IN PROGRESS segments") {
    // do not send the segment folders to trash
    createTable()
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    editTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))

    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == 4)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS" +
      s"('stale_inprogress'='true','force'='true','dryrun'='true')").collect()
    val cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS" +
      s"('stale_inprogress'='true','force'='true')").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    assert(0 == segmentNumber2)
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assert(getFileCountInTrashFolder(trashFolderPath) == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS)
  }

  test("clean up table and test trash folder with Marked For Delete and Compacted segments") {
    // do not send MFD folders to trash
    createTable()
    loadData()
    sql(s"""ALTER TABLE CLEANTEST COMPACT "MINOR" """)
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"""Delete from table cleantest where segment.id in(4)""")

    var dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true')").collect()
    var cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest").collect()
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('statistics'='false')").show()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true','force'='true')")
        .collect()
    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    sql(s"""show segments for table cleantest""").show()
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == segmentNumber2 + 5)
    assert(!FileFactory.isFileExist(trashFolderPath))
    count = 0
    // no carbondata file is added to the trash
    assert(getFileCountInTrashFolder(trashFolderPath) == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("clean up table and test trash folder with stale segments") {
    createTable()
    loadData()
    sql(s"""alter table cleantest compact 'minor'""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 2, "name"""")
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(5)))
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "4")
    assert(!FileFactory.isFileExist(trashFolderPath))
    var dryRunRes = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true')").collect()
    var cleanFilesRes = sql(s"CLEAN FILES FOR TABLE cleantest").collect()
    assert(cleanFilesRes(0).get(0) == dryRunRes(0).get(0))
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)
    val timeStamp = getTimestampFolderName(trashFolderPath)
    // recovering data from trash folder
    val segment4Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '4'

    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment4Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    // test after recovering data from trash
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(5)))

    dryRunRes = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true')").collect()
    cleanFilesRes = sql(s"CLEAN FILES FOR TABLE cleantest").collect()
    assert(cleanFilesRes(0).get(0) == dryRunRes(0).get(0))
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    dryRunRes = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true', 'dryrun'='true')")
        .collect()
    cleanFilesRes = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").collect()
    assert(cleanFilesRes(0).get(0) == dryRunRes(0).get(0))
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)

    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("clean up maintable table and test trash folder with SI with stale segments") {
    createTable()
    loadData()
    sql(s"""CREATE INDEX SI_CLEANTEST on cleantest(add) as 'carbondata' """)

    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""),
      Seq(Row(4)))

    val mainTablePath = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext
      .sparkSession).getTablePath
    // deleteTableStatusFile(mainTablePath)
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "1")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "2")
    val mainTableTrashFolderPath = CarbonTablePath.getTrashFolderPath(mainTablePath)

    assert(!FileFactory.isFileExist(mainTableTrashFolderPath))
    sql(s"CLEAN FILES FOR TABLE CLEANTEST").show()
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(2)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""), Seq(Row(4)))

    assert(FileFactory.isFileExist(mainTableTrashFolderPath))

    count = 0
    var listMainTable = getFileCountInTrashFolder(mainTableTrashFolderPath)
    assert(listMainTable == 4)

    // recovering data from trash folder
    val timeStamp = getTimestampFolderName(mainTableTrashFolderPath)
    val segment1Path = mainTableTrashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
      timeStamp + CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '1'
    val segment2Path = mainTableTrashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
      timeStamp + CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '2'


    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment1Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment2Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    assert(intercept[RuntimeException] {
      sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    }.getMessage.contains("Clean files with force operation not permitted by default"))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    // no files in trash anymore
    count = 0
    listMainTable = getFileCountInTrashFolder(mainTableTrashFolderPath)
    assert(listMainTable == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("test trash folder with 2 segments with same segment number") {
    createTable()
    loadData()

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "3")

    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)

    sql(s"""INSERT INTO CLEANTEST SELECT "1", 2, "name"""")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "3")

    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 4)

    assert(intercept[RuntimeException] {
      sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    }.getMessage.contains("Clean files with force operation not permitted by default"))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("test carbon.trash.retenion.property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS, "0")
    createTable()
    loadData()
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "1")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "2")
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(2)))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 4)
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 0)

    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS)
  }

  test("Test clean files on segments after compaction and deletion of segments on" +
       " partition table with mixed formats") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("DROP TABLE IF EXISTS partition_carbon_table")
    sql("DROP TABLE IF EXISTS partition_parquet_table")
    sql("""CREATE TABLE partition_carbon_table (id Int, vin String, logdate Date,phonenumber Long,
         area String, salary Int) PARTITIONED BY (country String)
          STORED AS carbondata""".stripMargin)
    for (i <- 0 until 5) {
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/partition_data_example.csv'
             | into table partition_carbon_table""".stripMargin)
    }
    sql("""CREATE TABLE partition_parquet_table (id Int, vin String, logdate Date,phonenumber Long,
         country String, area String, salary Int)
         using parquet PARTITIONED BY (country)""".stripMargin)
    sql(s"""insert into partition_parquet_table select * from partition_carbon_table""")
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("partition_parquet_table")).location
    sql(s"alter table partition_carbon_table add segment options ('path'='$parquetRootPath', " +
        "'format'='parquet', 'partition'='country:string')")
    sql("alter table partition_carbon_table compact 'minor'").collect()
    sql("delete from table partition_carbon_table where segment.id in (7,8)")
    sql("clean files for table partition_carbon_table OPTIONS('force'='true')")
    val table = CarbonEnv
      .getCarbonTable(None, "partition_carbon_table") (sqlContext.sparkSession)
    val segmentsFilePath = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
    val files = new File(segmentsFilePath).listFiles()
    assert(files.length == 6)
    val segmentIds = files.map(file => getSegmentIdFromSegmentFilePath(file.getAbsolutePath))
    assert(segmentIds.contains("0.1"))
    assert(!segmentIds.contains("7"))
    assert(!segmentIds.contains("8"))
    sql("DROP TABLE IF EXISTS partition_carbon_table")
    sql("DROP TABLE IF EXISTS partition_parquet_table")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
  }

  test("Test clean files on segments(MFD/Compacted/inProgress) after compaction and" +
       " deletion of segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    for (i <- 0 until 6) {
      FileFactory.deleteAllFilesOfDir(new File(newPath + i))
      CarbonTestUtil.copy(path, newPath + i)
    }
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    for (i <- 0 until 6) {
      sql(s"alter table addsegment1 add segment " +
          s"options('path'='${ newPath + i }', 'format'='carbon')").collect()
    }

    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(80)))
    sql("alter table addsegment1 compact 'minor'").collect()
    for (i <- 0 until 2) {
      assert(CarbonTestUtil.getIndexFileCount("default_addsegment1", i.toString,
        CarbonTablePath.MERGE_INDEX_FILE_EXT) == 1)
    }
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(80)))
    sql("clean files for table addsegment1 OPTIONS('force'='true')")
    for (i <- 0 until 2) {
      assert(CarbonTestUtil.getIndexFileCount("default_addsegment1", i.toString,
        CarbonTablePath.MERGE_INDEX_FILE_EXT) == 0)
    }
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(80)))
    sql(s"alter table addsegment1 add segment " +
        s"options('path'='${ newPath + 0 }', 'format'='carbon')").collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(90)))
    sql("delete from table addsegment1 where segment.id in (8)")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(80)))
    sql("clean files for table addsegment1 OPTIONS('force'='true')")
    sql(s"alter table addsegment1 add segment " +
        s"options('path'='${ newPath + 0 }', 'format'='carbon')").collect()
    // testing for in progress segments
    val tableStatusPath = CarbonTablePath.getTableStatusFilePath(table.getTablePath)
    val segments = SegmentStatusManager.readTableStatusFile(tableStatusPath)
    segments.foreach(segment => if (segment.getLoadName.equals("9")) {
      segment.setSegmentStatus(SegmentStatus.INSERT_IN_PROGRESS)
    })
    SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, segments)
    sql("clean files for table addsegment1 OPTIONS('force'='true','stale_inprogress'='true')")
    val segmentsFilePath = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
    val files = new File(segmentsFilePath).listFiles()
    assert(files.length == 2)
    val segmentIds = files.map(file => getSegmentIdFromSegmentFilePath(file.getAbsolutePath))
    assert(segmentIds.contains("0.1"))
    assert(segmentIds.contains("4.1"))
    assert(!segmentIds.contains("8"))
    assert(!segmentIds.contains("9"))
    for (i <- 0 until 6) {
      val oldFolder = FileFactory.getCarbonFile(newPath + i)
      assert(oldFolder.listFiles.length == 2,
        "Older data present at external location should not be deleted")
      FileFactory.deleteAllFilesOfDir(new File(newPath + i))
    }
    sql("drop table if exists addsegment1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
  }

  test("Test clean files on segments(MFD/Compacted/inProgress) after deletion of segments" +
       " present inside table path") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = table.getTablePath + "/internalPath"
    CarbonTestUtil.copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    sql(s"alter table addsegment1 add segment " +
          s"options('path'='$newPath', 'format'='carbon')").collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    sql("delete from table addsegment1 where segment.id in (1)")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    sql("clean files for table addsegment1 OPTIONS('force'='true')")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    val segmentsFilePath = CarbonTablePath.getSegmentFilesLocation(table.getTablePath)
    val files = new File(segmentsFilePath).listFiles()
    assert(files.length == 1)
    val segmentIds = files.map(file => getSegmentIdFromSegmentFilePath(file.getAbsolutePath))
    assert(segmentIds.contains("0"))
    assert(!segmentIds.contains("1"))
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 2,
        "Older data present at external location should not be deleted")
    sql("drop table if exists addsegment1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    }

  test("Test clean files after delete command") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("drop table if exists cleantest")
    sql(
      """
        | CREATE TABLE cleantest (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = CarbonEnv.getCarbonTable(None, "cleantest") (sqlContext.sparkSession)
    sql("delete from cleantest where deptno='10'")
    sql(s"""Delete from table cleantest where segment.id in(0)""")
    val segmentSize = FileFactory.getDirectorySize(CarbonTablePath.getSegmentPath(table
        .getTablePath, "0")) + FileFactory.getDirectorySize(CarbonTablePath
        .getSegmentFilesLocation(table.getTablePath))
    var dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true')").collect()
    var cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('dryrun'='true','force'='true')")
      .collect()
    cleanFiles = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").collect()
    assert(cleanFiles(0).get(0) == dryRun(0).get(0))
    assert(ByteUtil.convertByteToReadable(segmentSize) == cleanFiles(0).get(0))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    sql("drop table if exists cleantest")
  }

  test("Test clean files after horizontal compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")

    sql("drop table if exists cleantest")
    sql(
      """
        | CREATE TABLE cleantest (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE cleantest OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("delete from cleantest where deptno='10'").show()
    sql("delete from cleantest where deptno='11'").show()
    val table = CarbonEnv.getCarbonTable(None, "cleantest") (sqlContext.sparkSession)
    val segment0Path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val allSegmentFilesPreCleanFiles = FileFactory.getCarbonFile(segment0Path).listFiles()
      .filter(a => a.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT))
    assert(allSegmentFilesPreCleanFiles.length == 3)

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").collect()
    val allSegmentFilesPostCleanFiles = FileFactory.getCarbonFile(segment0Path).listFiles()
      .filter(a => a.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT))
    assert(allSegmentFilesPostCleanFiles.length == 1)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
        CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    sql("drop table if exists cleantest")
  }

  def editTableStatusFile(carbonTablePath: String) : Unit = {
    // original table status file
    val f1 = new File(CarbonTablePath.getTableStatusFilePath(carbonTablePath))
    val f2 = new File(CarbonTablePath.getMetadataPath(carbonTablePath) + CarbonCommonConstants
        .FILE_SEPARATOR + CarbonCommonConstants.FILE_SEPARATOR + "tmp")
    val w = new PrintWriter(f2)
    val bufferedSource = Source.fromFile(f1)
      bufferedSource.getLines
      .map { x =>
        x.replaceAll("Success", "In Progress")
      }
      // scalastyle:off println
      .foreach(x => w.println(x))
    // scalastyle:on println
    bufferedSource.close()
    w.close()
    f2.renameTo(f1)
  }

  def getFileCountInTrashFolder(dirPath: String) : Int = {
    val fileName = new File(dirPath)
    val files = fileName.listFiles()
    if (files != null) {
      files.foreach(file => {
        if (file.isFile) {
          count = count + 1
        }
        if (file.isDirectory()) {
          getFileCountInTrashFolder(file.getAbsolutePath())
        }
      })
    }
    count
  }

  def getTimestampFolderName(trashPath: String) : String = {
    val timeStampList = FileFactory.getFolderList(trashPath)
    timeStampList.get(0).getName
  }

  def createTable() : Unit = {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int, add String)
        | STORED AS carbondata
      """.stripMargin)
  }

  /**
   * gets segment id from given absolute segment file path
   */
  def getSegmentIdFromSegmentFilePath(segmentFilePath: String): String = {
    val tempSegmentFileAbsolutePath = segmentFilePath.replace(CarbonCommonConstants
      .WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR)
    if (!StringUtils.isBlank(tempSegmentFileAbsolutePath)) {
      val pathElements = tempSegmentFileAbsolutePath.split(CarbonCommonConstants
        .FILE_SEPARATOR)
      if (pathElements != null && pathElements.nonEmpty) {
        val fileName = pathElements(pathElements.length - 1)
        return DataFileUtil.getSegmentNoFromSegmentFile(fileName)
      }
    }
    CarbonCommonConstants.INVALID_SEGMENT_ID
  }

  def loadData() : Unit = {
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
  }
}
