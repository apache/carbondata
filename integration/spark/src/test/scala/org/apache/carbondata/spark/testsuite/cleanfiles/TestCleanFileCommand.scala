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

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

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
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('stale_inprogress'='true')").show
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
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('stale_inprogress'='true','force'='true')").show
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
    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
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
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
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

    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)

    intercept[RuntimeException] {
      sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    }
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
    intercept[RuntimeException] {
      sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    }
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

    intercept[RuntimeException] {
      sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    }
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

  def loadData() : Unit = {
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
  }
}
