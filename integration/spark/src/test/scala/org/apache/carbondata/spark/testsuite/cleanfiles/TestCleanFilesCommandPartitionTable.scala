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

class TestCleanFilesCommandPartitionTable extends QueryTest with BeforeAndAfterAll {

  test("clean up table and test trash folder with IN PROGRESS segments") {
    // do not send the segment folders to trash
    createPartitionTable()
    loadData()
    val (path, trashFolderPath) = getTableAndTrashPath
    editTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    assertResult(4)(sql(s"""show segments for table cleantest""").count())
    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(4)(sql(s"""show segments for table cleantest""").count())
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('FORCE'='TRUE')")
    assertResult(0)(sql(s"""show segments for table cleantest""").count())
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
  }

  test("clean up table and test trash folder with Marked For Delete and Compacted segments") {
    // do not send MFD folders to trash
    createPartitionTable()
    loadData()
    sql(s"""ALTER TABLE CLEANTEST COMPACT "MINOR" """)
    loadData()
    val (_, trashFolderPath) = getTableAndTrashPath
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"""Delete from table cleantest where segment.id in(4)""")
    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    sql(s"CLEAN FILES FOR TABLE cleantest")
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('FORCE'='TRUE')")
    val segmentNumber3 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == segmentNumber2)
    assert(segmentNumber2 == segmentNumber3 + 5)
    assert(!FileFactory.isFileExist(trashFolderPath))
    // no carbondata file is added to the trash
    assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
  }

  test("test trash folder with 2 segments with same segment number") {
    createPartitionTable()
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"hello","abc"""")
    val (path, trashFolderPath) = getTableAndTrashPath
    assert(!FileFactory.isFileExist(trashFolderPath))
    deleteTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(2)(getFileCountInTrashFolder(trashFolderPath))
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"hello","abc"""")
    deleteTableStatusFile(path)
    sql(s"CLEAN FILES FOR TABLE cleantest")
    assertResult(4)(getFileCountInTrashFolder(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')")
    // no carbondata file is added to the trash
    assertResult(0)(getFileCountInTrashFolder(trashFolderPath))
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("clean up table and test trash folder with stale segments") {
    createPartitionTable()
    loadData()
    val (path, trashFolderPath) = getTableAndTrashPath
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    deleteTableStatusFile(path)
    sql(s"CLEAN FILES FOR TABLE CLEANTEST")
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(0)))
    val timeStamp = getTimestampFolderName(trashFolderPath)
    // test recovery from partition table
    recoverSegment(trashFolderPath, timeStamp, "0")
    recoverSegment(trashFolderPath, timeStamp, "1")
    recoverSegment(trashFolderPath, timeStamp, "2")
    recoverSegment(trashFolderPath, timeStamp, "3")
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(4)))
  }

  test("clean up table and test trash folder with stale segments part 2") {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("create table cleantest(" +
      "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into cleantest values (30, 'amy', 12), (40, 'bob', 13)")
    sql("insert into cleantest values (30, 'amy', 20), (10, 'bob', 13)")
    sql("insert into cleantest values (30, 'cat', 12), (40, 'dog', 13)")
    val (path, trashFolderPath) = getTableAndTrashPath
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    // createStaleSegments(path)
    deleteTableStatusFile(path)
    sql(s"CLEAN FILES FOR TABLE CLEANTEST")
    val timeStamp = getTimestampFolderName(trashFolderPath)
    // test recovery from partition table
    recoverSegment(trashFolderPath, timeStamp, "0")
    recoverSegment(trashFolderPath, timeStamp, "1")
    recoverSegment(trashFolderPath, timeStamp, "2")
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(6)))
    checkAnswer(sql(s"""select count(*) from cleantest where age=13"""), Seq(Row(3)))
  }

  test("clean up maintable table and test trash folder with SI with stale segments") {
    createPartitionTable()
    loadData()
    sql(s"""CREATE INDEX SI_CLEANTEST on cleantest(name) as 'carbondata' """)
    checkAnswer(sql(s"""select count(*) from cleantest"""), Seq(Row(4)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""), Seq(Row(4)))
    val (mainTablePath, mainTableTrashFolderPath) = getTableAndTrashPath
    deleteTableStatusFile(mainTablePath)
    assert(!FileFactory.isFileExist(mainTableTrashFolderPath))
    sql(s"CLEAN FILES FOR TABLE CLEANTEST")
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

  test("clean files not allowed force option by default") {
    try {
      createPartitionTable()
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

  def editTableStatusFile(carbonTablePath: String) : Unit = {
    // Original Table status file
    val f1 = new File(CarbonTablePath.getTableStatusFilePath(carbonTablePath))
    // duplicate
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
    bufferedSource.close
    w.close()
    f2.renameTo(f1)
  }

  def getFileCountInTrashFolder(dirPath: String): Int = {
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

  def getTimestampFolderName(trashPath: String) : String = {
    val timeStampList = FileFactory.getFolderList(trashPath)
    timeStampList.get(0).getName
  }

  def deleteTableStatusFile(carbonTablePath: String) : Unit = {
    val f1 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tablestatus")  // Original File
    f1.delete()
  }

  def createPartitionTable() : Unit = {
    dropTable
    sql(
      """
        | CREATE TABLE CLEANTEST (id Int, id1 INT, name STRING ) PARTITIONED BY (add String)
        | STORED AS carbondata
      """.stripMargin)
  }

  def loadData() : Unit = {
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"bob","abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"jack","abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"johnny","adc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"Reddit","adc"""")
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

  private def recoverDataFromTrash(path: String) = {
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
    sql("drop table if exists c1")
  }
}
