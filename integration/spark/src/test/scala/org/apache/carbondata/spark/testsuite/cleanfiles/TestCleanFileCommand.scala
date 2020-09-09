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
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestCleanFileCommand extends QueryTest with BeforeAndAfterAll {

  var count = 0

  test("clean up table and test trash folder with IN PROGRESS segments") {
    // do not send the segment folders to trash
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    editTableStatusFile(path)
    assert(!FileFactory.isFileExist(trashFolderPath))
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    assert(dryRun == 2)
    sql(s"CLEAN FILES FOR TABLE cleantest").show
    assert(!FileFactory.isFileExist(trashFolderPath))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }


  test("clean up table and test trash folder with Marked For Delete segments") {
    // do not send MFD folders to trash
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"""Delete from table cleantest where segment.id in(1)""")
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    // dry run shows 1 Marked Fro DElete segments to be deleted
    assert(dryRun == 1)
    sql(s"CLEAN FILES FOR TABLE cleantest").show
    assert(!FileFactory.isFileExist(trashFolderPath))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("clean up table and test trash folder with compaction") {
    // do not send compacted folders to trash
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""ALTER TABLE CLEANTEST COMPACT "MINOR" """)

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    assert(!FileFactory.isFileExist(trashFolderPath))
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    // dry run shows 4 compacted segments to be deleted
    assert(dryRun == 4)
    sql(s"CLEAN FILES FOR TABLE cleantest").show
    assert(!FileFactory.isFileExist(trashFolderPath))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("clean up table and test trash folder with stale segments") {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql(
      """
        | CREATE TABLE cleantest (name String, id Int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 2""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 1""")
    sql(s"""INSERT INTO CLEANTEST SELECT "abc", 2""")
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))
    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4 segments are made as stale segments and should be moved to trash
    deleteTableStatusFile(path)
    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    assert(dryRun == 4)
    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(0)))
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 8)

    val timeStamp = getTimestampFolderName(trashFolderPath)

    // recovering data from trash folder
    sql(
      """
        | CREATE TABLE cleantest1 (name String, id Int)
        | STORED AS carbondata
      """.stripMargin)

    val segment0Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '0'
    val segment1Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '1'
    val segment2Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '2'
    val segment3Path = trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR + timeStamp +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '3'

    sql(s"alter table cleantest1 add segment options('path'='$segment0Path'," +
      s"'format'='carbon')").show()
    sql(s"alter table cleantest1 add segment options('path'='$segment1Path'," +
      s"'format'='carbon')").show()
    sql(s"alter table cleantest1 add segment options('path'='$segment2Path'," +
      s"'format'='carbon')").show()
    sql(s"alter table cleantest1 add segment options('path'='$segment3Path'," +
      s"'format'='carbon')").show()
    sql(s"""INSERT INTO CLEANTEST SELECT * from cleantest1""")

    // test after recovering data from trash
    checkAnswer(sql(s"""select count(*) from cleantest"""),
      Seq(Row(4)))

    val dryRun1 = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    assert(dryRun1 == 4)

    sql(s"CLEAN FILES FOR TABLE cleantest").show()

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("clean up maintable table and test trash folder with SI with stale segments") {
    sql("""DROP TABLE IF EXISTS CLEANTEST_WITHSI""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
    sql(
      """
        | CREATE TABLE CLEANTEST_WITHSI (id Int, name String, add String )
        | STORED AS carbondata
      """.stripMargin)
    sql(s"""INSERT INTO CLEANTEST_WITHSI SELECT 1,"abc","def"""")
    sql(s"""INSERT INTO CLEANTEST_WITHSI SELECT 2, "abc","def"""")
    sql(s"""INSERT INTO CLEANTEST_WITHSI SELECT 3, "abc","def"""")

    sql(s"""CREATE INDEX SI_CLEANTEST on cleantest_withSI(add) as 'carbondata' """)

    checkAnswer(sql(s"""select count(*) from cleantest_withSI"""),
      Seq(Row(3)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""),
      Seq(Row(3)))

    val mainTablePath = CarbonEnv.getCarbonTable(Some("default"), "cleantest_withsi")(sqlContext
      .sparkSession).getTablePath
    deleteTableStatusFile(mainTablePath)
    val mainTableTrashFolderPath = mainTablePath + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME

    val siTablePath = CarbonEnv.getCarbonTable(Some("default"), "si_cleantest")(sqlContext
      .sparkSession).getTablePath
    deleteTableStatusFile(siTablePath)
    val siTableTrashFolderPath = siTablePath + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME

    assert(!FileFactory.isFileExist(mainTableTrashFolderPath))
    assert(!FileFactory.isFileExist(siTableTrashFolderPath))

    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest_withsi OPTIONS('isDryRun'='true')").count()
    // dry run shows 6 segments to move to trash. 3 for main table, 3 for si table
    assert(dryRun == 6)

    sql(s"CLEAN FILES FOR TABLE CLEANTEST_WITHSI").show()

    checkAnswer(sql(s"""select count(*) from cleantest_withSI"""), Seq(Row(0)))
    checkAnswer(sql(s"""select count(*) from si_cleantest"""), Seq(Row(0)))

    assert(FileFactory.isFileExist(mainTableTrashFolderPath))
    assert(FileFactory.isFileExist(siTableTrashFolderPath))

    count = 0
    var listMainTable = getFileCountInTrashFolder(mainTableTrashFolderPath)
    assert(listMainTable == 6)

    count = 0
    var listSITable = getFileCountInTrashFolder(siTableTrashFolderPath)
    assert(listSITable == 6)

    val dryRun1 = sql(s"CLEAN FILES FOR TABLE cleantest_withsi OPTIONS('isDryRun'='true')").count()
    // dry run shows 6 segments to move to trash. 3 for main table, 3 for si table
    assert(dryRun1 == 6)
    // recovering data from trash folder
    val timeStamp = getTimestampFolderName(mainTableTrashFolderPath)
    sql(
      """
        | CREATE TABLE cleantest1 (id Int, name String, add String )
        | STORED AS carbondata
      """.stripMargin)

    val segment0Path = mainTableTrashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
      timeStamp + CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '0'
    val segment1Path = mainTableTrashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
      timeStamp + CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '1'
    val segment2Path = mainTableTrashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
      timeStamp + CarbonCommonConstants.FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + '2'

    sql(s"alter table cleantest1 add segment options('path'='$segment0Path'," +
      s"'format'='carbon')").show()
    sql(s"alter table cleantest1 add segment options('path'='$segment1Path'," +
      s"'format'='carbon')").show()
    sql(s"alter table cleantest1 add segment options('path'='$segment2Path'," +
      s"'format'='carbon')").show()
    sql(s"""INSERT INTO CLEANTEST_withsi SELECT * from cleantest1""")

    checkAnswer(sql(s"""select count(*) from cleantest_withSI"""),
      Seq(Row(3)))
    sql(s"CLEAN FILES FOR TABLE cleantest_withsi options('force'='true')").show
    // no files in trash anymore
    count = 0
    listMainTable = getFileCountInTrashFolder(mainTableTrashFolderPath)
    assert(listMainTable == 0)
    count = 0
    listSITable = getFileCountInTrashFolder(siTableTrashFolderPath)
    assert(listSITable == 0)
    sql("show segments for table cleantest_withsi").show()
    sql("show segments for table si_cleantest").show()
    sql("""DROP TABLE IF EXISTS CLEANTEST_WITHSI""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")

  }

  test("clean up table and test trash folder with stale segments in partition table") {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")

    sql(
      """
        | CREATE TABLE CLEANTEST (id Int, id1 INT ) PARTITIONED BY (add String)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"abc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"adc"""")
    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"adc"""")

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    createStaleSegments(path)

    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    // dry run shows 3 segments to move to trash
    assert(dryRun == 3)

    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 8)

    val dryRun1 = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    // dry run shows 3 segments to move to trash
    assert(dryRun1 == 4)

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  test("test trash folder with 2 segments with same segment number") {
    sql("""DROP TABLE IF EXISTS CLEANTEST""")

    sql(
      """
        | CREATE TABLE CLEANTEST (id Int, id1 INT , add String)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"abc"""")

    val path = CarbonEnv.getCarbonTable(Some("default"), "cleantest")(sqlContext.sparkSession)
      .getTablePath
    val trashFolderPath = path + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.CARBON_TRASH_FOLDER_NAME
    assert(!FileFactory.isFileExist(trashFolderPath))
    // All 4  segments are made as stale segments, they should be moved to the trash folder
    deleteTableStatusFile(path)

    val dryRun = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    // dry run shows 3 segments to move to trash
    assert(dryRun == 1)

    assert(!FileFactory.isFileExist(trashFolderPath))
    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    var list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 2)

    sql(s"""INSERT INTO CLEANTEST SELECT 1, 2,"abc"""")
    deleteTableStatusFile(path)

    val dryRun1 = sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('isDryRun'='true')").count()
    assert(dryRun1 == 2)

    sql(s"CLEAN FILES FOR TABLE cleantest").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    assert(list == 4)

    sql(s"CLEAN FILES FOR TABLE cleantest OPTIONS('force'='true')").show()
    count = 0
    list = getFileCountInTrashFolder(trashFolderPath)
    // no carbondata file is added to the trash
    assert(list == 0)
    sql("""DROP TABLE IF EXISTS CLEANTEST""")
    sql("""DROP TABLE IF EXISTS CLEANTEST1""")
  }

  def editTableStatusFile(carbonTablePath: String) : Unit = {
    val f1 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tablestatus")  // Original File
    val f2 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tmp") // Temporary File
    val w = new PrintWriter(f2)
    Source.fromFile(f1).getLines
      .map { x =>
        x.replaceAll("Success", "In Progress")
      }
      // scalastyle:off println
      .foreach(x => w.println(x))
    // scalastyle:on println
    w.close()
    f2.renameTo(f1)
  }


  def getFileCountInTrashFolder(dirPath: String) : Int = {
    val f = new File(dirPath)
    val files = f.listFiles()
    if (files != null) {
      files.foreach( f =>
      {
        if (f.isFile) {
          count = count + 1
        }
          if (f.isDirectory()) {
          getFileCountInTrashFolder(f.getAbsolutePath())
        }
      })
    }
    count
  }

  def createStaleSegments(carbonTablePath: String) : Unit = {
    val f1 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tablestatus")  // Original File
    val f2 = new File(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
      CarbonCommonConstants.FILE_SEPARATOR + "tmp") // Temporary File
    val w = new PrintWriter(f2)
    // scalastyle:off println
    w.println("[{\"timestamp\":\"1601538062303\",\"loadStatus\":\"Success\",\"loadName\":\"3\"," +
      "\"dataSize\":\"620\",\"indexSize\":\"423\",\"loadStartTime\":\"1601538047755\"," +
      "\"segmentFile\":\"3_1601538047755.segment\"}]")
    // scalastyle:on println
    w.close()
    f2.renameTo(f1)
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

}
