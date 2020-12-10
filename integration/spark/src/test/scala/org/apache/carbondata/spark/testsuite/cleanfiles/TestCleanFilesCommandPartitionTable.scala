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

  var count = 0

  test("clean up table and test trash folder with IN PROGRESS segments") {
    // do not send the segment folders to trash
    createParitionTable()
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath.TRASH_DIR
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
    val list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("clean up table and test trash folder with Marked For Delete and Compacted segments") {
    // do not send MFD folders to trash
    createParitionTable()
    loadData()
    sql(s"""ALTER TABLE CLEANTEST COMPACT "MINOR" """)
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath.TRASH_DIR
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"""Delete from table cleantest where segment.id in(4)""")
    val segmentNumber1 = sql(s"""show segments for table cleantest""").count()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    val segmentNumber2 = sql(s"""show segments for table cleantest""").count()
    assert(segmentNumber1 == segmentNumber2 + 5)
    assert(!FileFactory.isFileExist(trashFolderPath))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("test trash folder with 2 segments with same segment number") {
    createParitionTable()
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath.TRASH_DIR
    assert(!FileFactory.isFileExist(trashFolderPath))
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "3")

    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)

    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"hello","abc"""")
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
  }

  test("clean up table and test trash folder with stale segments") {
    sql("""DROP TABLE IF EXISTS C1""")
    createParitionTable()
    loadData()
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    addRandomFiles(path)
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
      sqlContext.sparkSession), "1")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
      sqlContext.sparkSession), "2")

    sql(s"CLEAN FILES FOR TABLE CLEANTEST").show()
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(2)))

    val timeStamp = getTimestampFolderName(trashFolderPath)
    // test recovery from partition table

    val segment1Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      "/Segment_1"
    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment1Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    val segment2Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      "/Segment_2"
    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment2Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))

    sql("""DROP TABLE IF EXISTS C1""")
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }


  test("clean up table and test trash folder with stale segments part 2") {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS C1""")

    sql("create table cleantest(" +
      "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into cleantest values (30, 'amy', 12), (40, 'bob', 13)")
    sql("insert into cleantest values (30, 'amy', 20), (10, 'bob', 13)")
    sql("insert into cleantest values (30, 'cat', 12), (40, 'dog', 13)")


    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    addRandomFiles(path)
    val trashFolderPath = CarbonTablePath.getTrashFolderPath(path)
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
      sqlContext.sparkSession), "1")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
      sqlContext.sparkSession), "2")

    sql(s"CLEAN FILES FOR TABLE CLEANTEST").show()

    val timeStamp = getTimestampFolderName(trashFolderPath)
    // test recovery from partition table

    val segment1Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      "/Segment_1"
    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment1Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    val segment2Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      "/Segment_2"
    sql(s"CREATE TABLE c1 USING CARBON LOCATION '$segment2Path'")
    sql("INSERT INTO cleantest select * from c1").show()
    sql("drop table c1")

    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(6)))
    checkAnswer(sql(s"""select count(*) from cleantest where age=13"""),
      Seq(Row(3)))

    sql("""DROP TABLE IF EXISTS C1""")
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
  }

  test("clean up maintable table and test trash folder with SI with stale segments") {
    createParitionTable()
    loadData()
    sql(s"""CREATE INDEX SI_CLEANTEST on cleantest(name) as 'carbondata' """)
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""),
      Seq(Row(4)))

    val mainTablePath = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext
      .sparkSession).getTablePath
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "1")
    removeSegmentEntryFromTableStatusFile(CarbonEnv.getCarbonTable(Some("default"), "cleantest")(
        sqlContext.sparkSession), "2")
    val mainTableTrashFolderPath = mainTablePath + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.TRASH_DIR

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
    sql("show segments for table cleantest").show()
    sql("show segments for table si_cleantest").show()
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
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

  def createParitionTable() : Unit = {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
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

  def addRandomFiles(carbonTablePath: String) : Unit = {
    val f1 = CarbonTablePath.getSegmentFilesLocation(carbonTablePath) +
      CarbonCommonConstants.FILE_SEPARATOR  + "_.tmp"
    val f2 = CarbonTablePath.getSegmentFilesLocation(carbonTablePath) +
      CarbonCommonConstants.FILE_SEPARATOR  + "1_.tmp"
      FileFactory.createNewFile(f1)
      FileFactory.createNewFile(f2)
  }

}
