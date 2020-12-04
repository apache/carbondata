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

  test("clean up table and test trash folder with IN PROGRESS segments") {
    // do not send the segment folders to trash
    createTable()
    loadData()
    val (path, trashFolderPath) = getTableAndTrashPath
    editTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    assertResult(4)(sql("show segments for table cleantest").count())
    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(4)(sql("show segments for table cleantest").count())
    // clean in progress segment immediately
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('FORCE'='TRUE')")
    assertResult(0)(sql("show segments for table cleantest").count())
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assert(getFileCountInTrashFolder(trashFolderPath) == 0)
  }

  test("clean up table and test trash folder with Marked For Delete and Compacted segments") {
    // do not send MFD folders to trash
    createTable()
    loadData()
    sql(s"""ALTER TABLE CLEANTEST COMPACT "MINOR" """)
    loadData()
    val (_, trashFolderPath) = getTableAndTrashPath
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"""Delete from table cleantest where segment.id in(4)""")
    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    sql(s"CLEAN FILES FOR TABLE cleantest")
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    // clean in MFD segment immediately
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('FORCE'='TRUE')")
    val segmentNumber3 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == segmentNumber2)
    assert(segmentNumber2 == segmentNumber3 + 5)
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assert(getFileCountInTrashFolder(trashFolderPath) == 0)
  }

  test("clean up table and test trash folder with stale segments") {
    createTable()
    loadData()
    sql(s"""alter table cleantest compact 'minor'""")
    sql(s"CLEAN FILES FOR TABLE cleantest")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 2, "name"""")
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(5)))
    val (path, trashFolderPath) = getTableAndTrashPath
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4 segments are made as stale segments and should be moved to trash
    deleteTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest")
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(0)))
    assertResult(12)(getFileCountInTrashFolder(trashFolderPath))
    val timeStamp = getTimestampFolderName(trashFolderPath)
    // recovering data from trash folder
    recoverSegment(trashFolderPath, timeStamp, "0.1")
    recoverSegment(trashFolderPath, timeStamp, "4")
    // test after recovering data from trash
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(5)))

    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(12)(getFileCountInTrashFolder(trashFolderPath))

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')")

    // no carbondata file is added to the trash
    assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
  }

  test("clean up maintable table and test trash folder with SI with stale segments") {
    createTable()
    loadData()
    sql(s"""CREATE INDEX SI_CLEANTEST on cleantest(add) as 'carbondata' """)

    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(4)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""), Seq(Row(4)))

    val (mainTablePath, mainTableTrashFolderPath) = getTableAndTrashPath
    deleteTableStatusFile(mainTablePath)
    assert(!FileFactory.isFileExist(mainTableTrashFolderPath))
    sql(s"CLEAN FILES FOR TABLE CLEANTEST").show()
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(0)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""), Seq(Row(4)))

    assert(FileFactory.isFileExist(mainTableTrashFolderPath))
    assertResult(8)(getFileCountInTrashFolder(mainTableTrashFolderPath))

    // recovering data from trash folder
    val timeStamp = getTimestampFolderName(mainTableTrashFolderPath)
    recoverSegment(mainTableTrashFolderPath, timeStamp, "0")
    recoverSegment(mainTableTrashFolderPath, timeStamp, "1")
    recoverSegment(mainTableTrashFolderPath, timeStamp, "2")
    recoverSegment(mainTableTrashFolderPath, timeStamp, "3")

    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(4)))
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')")
    // no files in trash anymore
    assertResult(0)(getFileCountInTrashFolder(mainTableTrashFolderPath))
  }

  test("test trash folder with 2 segments with same segment number") {
    createTable()
    sql(s"""INSERT INTO CLEANTEST SELECT "1", 2, "name"""")

    val (path, trashFolderPath) = getTableAndTrashPath
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    deleteTableStatusFile(path)

    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(2)(getFileCountInTrashFolder(trashFolderPath))

    sql(s"""INSERT INTO CLEANTEST SELECT "1", 2, "name"""")
    deleteTableStatusFile(path)

    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(4)(getFileCountInTrashFolder(trashFolderPath))

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()

    // no carbondata file is added to the trash
    assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
  }

  test("test carbon.trash.retenion.property") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS, "0")
      createTable()
      loadData()
      checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(4)))
      val (path, trashFolderPath) = getTableAndTrashPath
      assert(!FileFactory.isFileExist(trashFolderPath))
      // All 4 segments are made as stale segments and should be moved to trash
      deleteTableStatusFile(path)
      assert(!FileFactory.isFileExist(trashFolderPath))
      sql(s"CLEAN FILES FOR TABLE cleantest")
      checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(0)))
      assertResult(8)(getFileCountInTrashFolder(trashFolderPath))
      sql(s"CLEAN FILES FOR TABLE cleantest")
      assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS,
          CarbonCommonConstants.CARBON_TRASH_RETENTION_DAYS_DEFAULT + "")
    }
  }

  test("clean files not allowed force option by default") {
    try {
      createTable()
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
          CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
      val exception = intercept[RuntimeException] {
        sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')")
      }
      assertResult("Clean files with force operation not permitted by default")(
        exception.getMessage)
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    }
  }

  private def editTableStatusFile(carbonTablePath: String) : Unit = {
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

  private def getFileCountInTrashFolder(dirPath: String) : Int = {
    var count = 0
    val fileName = new File(dirPath)
    val files = fileName.listFiles()
    if (files != null) {
      files.foreach(file => {
        if (file.isDirectory()) {
          count = count + getFileCountInTrashFolder(file.getAbsolutePath())
        } else {
          count = count + 1
        }
      })
    }
    count
  }

  private def getTimestampFolderName(trashPath: String) : String = {
    val timeStampList = FileFactory.getFolderList(trashPath)
    timeStampList.get(0).getName
  }

  private def deleteTableStatusFile(carbonTablePath: String) : Unit = {
    val f1 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tablestatus")  // Original File
    f1.delete()
  }

  private def createTable() : Unit = {
    dropTable()
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int, add String)
        | STORED AS carbondata
      """.stripMargin)
  }

  private def loadData() : Unit = {
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1, "name"""")
  }

  private def getTableAndTrashPath: (String, String) = {
    val path =
      CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession).getTablePath
    (path, CarbonTablePath.getTrashFolderPath(path))
  }

  private def getTrashSegmentPath(
      trashFolderPath: String,
      timeStamp: String,
      segmentNo: String): String = {
    trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + segmentNo
  }

  private def recoverDataFromTrash(path: String): Unit = {
    sql("drop table if exists c1")
    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$path'")
    sql("INSERT INTO cleantest select * from c1")
    sql("drop table c1")
  }

  private def recoverSegment(trashFolderPath: String,
      timeStamp: String,
      segmentNo: String): Unit = {
    recoverDataFromTrash(getTrashSegmentPath(trashFolderPath, timeStamp, segmentNo))
  }

  override protected def afterAll(): Unit = {
    dropTable()
  }
  private def dropTable(): Unit = {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql("drop table if exists c1")
  }
}
