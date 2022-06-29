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

package org.apache.carbondata.integration.spark.testsuite.recovery

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.recovery.tablestatus.TableStatusRecovery
import org.apache.carbondata.sdk.file.CarbonWriter

class TableStatusRecoveryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS, "true")
    sql("DROP TABLE IF EXISTS table1")
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
      .removeProperty(CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS)
    sql("DROP TABLE IF EXISTS table1")
  }

  test("test table status recovery if file is lost after first insert") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    sql("insert into table1 values('abc',1, 1)")
    checkAnswer(sql("select * from table1"), Seq(Row("abc", 1, 1)))
    val version = deleteTableStatusVersionFile("table1")
    var err = intercept[RuntimeException] {
      sql("select count(*) from table1").show()
    }
    err = intercept[RuntimeException] {
      sql("insert into table1 values('abc',1, 1)")
    }
    assertException(version, err)
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"), Seq(Row("abc", 1, 1)))
  }

  test("test table status recovery for table with global sort") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata " +
        "tblproperties('sort_scope'='global_sort', 'sort_columns'='c3')")
    insertData()
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    sql("alter table table1 compact 'major'")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  test("test table status recovery on secondary index table") {
    def checkResults(): Unit = {
      checkAnswer(sql("select * from table1 where c1='abc'"),
        Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
      checkAnswer(sql("select count(*) from si_index "), Seq(Row(3)))
    }
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    sql("DROP INDEX IF EXISTS si_index on table1")
    sql("CREATE INDEX si_index on table table1 (c1) AS 'carbondata' ")
    insertData()
    checkResults()
    val version = deleteTableStatusVersionFile("si_index")
    var err = intercept[RuntimeException] {
      sql("SHOW SEGMENTS FOR TABLE si_index").show()
    }
    assertException(version, err)
    err = intercept[RuntimeException] {
      sql("select * from si_index").show()
    }
    assertException(version, err)
    val args = "default si_index"
    TableStatusRecovery.main(args.split(" "))
    checkResults()
  }

  test("test table status recovery on mv table -- not supported") {
    def checkResults(): Unit = {
      checkAnswer(sql("select c2, c3 from table1"),
        Seq(Row(1, 1), Row(2, 1), Row(3, 2)))
      checkAnswer(sql("select count(*) from view1"), Seq(Row(3)))
    }
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    sql("drop MATERIALIZED VIEW if exists view1")
    sql("CREATE MATERIALIZED VIEW view1 AS SELECT c2, c3 FROM table1")
    insertData()
    checkResults()
    val version = deleteTableStatusVersionFile("view1")
    var err = intercept[RuntimeException] {
      sql("select * from view1").show()
    }
    assertException(version, err)
    val args = "default view1"
    err = intercept[UnsupportedOperationException] {
      TableStatusRecovery.main(args.split(" "))
    }
    assert(err.getMessage.contains("Unsupported operation on Materialized view table"))
    sql("refresh materialized view view1")
    checkResults()
  }

  private def assertException(version: String,
      err: RuntimeException) = {
    assert(err.getMessage
      .contains(s"Table Status Version file {tablestatus_$version} not found. Try running " +
                "TableStatusRecovery tool to recover lost file "))
  }

  test("test table status recovery if file is lost after insert and disable versioning") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_Insert()
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS, "false")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
    // will write to tablestatus file
    sql("insert into table1 values('abcd',11, 12)")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2), Row("abcd", 11, 12)))
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_ENABLE_MULTI_VERSION_TABLE_STATUS, "true")
    sql("insert into table1 values('abcd',14, 15)")
    checkAnswer(sql("select * from table1"), Seq(Row("abc", 1, 1), Row("abc", 2, 1),
      Row("abc", 3, 2), Row("abcd", 11, 12), Row("abcd", 14, 15)))
  }

  test("verify clean table status old version files") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    insertData()
    insertData()
    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
    var tableStatusFiles = getTableStatusFiles(carbonTable)
    assert(tableStatusFiles.length == 6)
    sql("clean files for table table1 options('force'='true')")
    tableStatusFiles = getTableStatusFiles(carbonTable)
    assert(tableStatusFiles.length == 2)
  }

  private def getTableStatusFiles(carbonTable: CarbonTable) = {
    FileFactory.getCarbonFile(CarbonTablePath.getMetadataPath(carbonTable
      .getTablePath)).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.startsWith(CarbonTablePath
          .TABLE_STATUS_FILE)
      }
    })
  }

  test("test table status recovery if file is lost after delete segment") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_Delete_Segment()
  }

  test("test table status recovery if file is lost after insert") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_Insert()
  }

  test("test table status recovery if file is lost after update & delete") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_IUD()
  }

  test("test table status recovery if file is lost after compaction") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_Compaction()
  }

  test("test table status recovery if file is lost after compaction & clean files") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    verifyScenario_Compaction_CleanFiles()
  }

  test("test table status recovery if file is lost after insert - partition table") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int) partitioned by (c3 int)" +
        "STORED AS carbondata")
    verifyScenario_Insert()
  }

  test("test table status recovery if file is lost after update & delete - partition table") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int) partitioned by (c3 int)" +
        "STORED AS carbondata")
    verifyScenario_IUD()
  }

  test("test table status recovery if file is lost after compaction - partition table") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int) partitioned by (c3 int)" +
        "STORED AS carbondata")
    verifyScenario_Compaction()
  }

  test("test table status recovery if file is lost after compaction & clean files " +
       "- partition table") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int) partitioned by (c3 int)" +
        "STORED AS carbondata")
    verifyScenario_Compaction_CleanFiles()
  }

  test("test table status recovery if file is lost after add & drop partition") {
    sql("drop table if exists table1")
    sql("create table table1 (id int,name String) partitioned by(email string) " +
        "stored as carbondata")
    sql("insert into table1 select 1,'blue','abc'")
    val schemaFile =
      CarbonTablePath.getSchemaFilePath(
        CarbonEnv.getCarbonTable(None, "table1")(sqlContext.sparkSession).getTablePath)
    val sdkWritePath = target + "/" + "def"
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(sdkWritePath))
    val writer = CarbonWriter.builder()
      .outputPath(sdkWritePath)
      .writtenBy("test")
      .withSchemaFile(schemaFile)
      .withCsvInput()
      .build()
    writer.write(Seq("2", "red", "def").toArray)
    writer.write(Seq("3", "black", "def").toArray)
    writer.close()
    sql(s"alter table table1 add partition (email='def') location '$sdkWritePath'")
    val version = deleteTableStatusVersionFile("table1")
    val err = intercept[RuntimeException] {
      sql("select count(*) from table1").show()
    }
    assertException(version, err)
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select name from table1"), Seq(Row("blue"), Row("red"), Row("black")))
    sql("alter table table1 drop partition(email='def')")
    checkAnswer(sql("select name from table1"), Seq(Row("blue")))
    deleteTableStatusVersionFile("table1")
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select name from table1"), Seq(Row("blue")))
  }

  private def deleteTableStatusVersionFile(tblName: String): String = {
    val table = CarbonEnv.getCarbonTable(Some("default"), tblName)(sqlContext.sparkSession)
    val currVersion = table.getTableStatusVersion
    val status = FileFactory.getCarbonFile(CarbonTablePath.getTableStatusFilePath(
      table.getTablePath, currVersion)).deleteFile()
    assert(status.equals(true))
    currVersion
  }

  private def verifyScenario_Insert(): Unit = {
    insertDataAndCheckResult()
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def insertDataAndCheckResult(): Unit = {
    insertData()
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def insertData(): Unit = {
    sql("insert into table1 values('abc',1, 1)")
    sql("insert into table1 values('abc', 2, 1)")
    sql("insert into table1 values('abc', 3, 2)")
  }

  def verifyScenario_IUD(): Unit = {
    insertDataAndCheckResult()
    sql("update table1 set(c2)=(5) where c2=3").show()
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 5, 2)))
    sql("update table1 set(c2)=(6) where c2=5").show()

    var table = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
    var currVersion = table.getTableStatusVersion
    var status = FileFactory.getCarbonFile(CarbonTablePath.getTableStatusFilePath(
      table.getTablePath, currVersion)).deleteFile()
    assert(status.equals(true))
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 6, 2)))

    sql("delete from table1 where c2=6").show()
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1)))
    table = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
    currVersion = table.getTableStatusVersion
    status = FileFactory.getCarbonFile(CarbonTablePath.getTableStatusFilePath(
      table.getTablePath, currVersion)).deleteFile()
    assert(status.equals(true))
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1)))
  }

  private def verifyScenario_Compaction(): Unit = {
    insertData()
    sql("alter table table1 compact 'major'")
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    assert(!sql("Show segments for table table1").collect().map(_.get(0)).contains("0.1"))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def verifyScenario_Compaction_CleanFiles(): Unit = {
    insertDataAndCheckResult()
    sql("alter table table1 compact 'major'")
    sql("clean files for table table1 options('force'='true')")
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    assert(sql("Show segments for table table1").collect().map(_.get(0)).contains("0.1"))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def verifyScenario_Delete_Segment(): Unit = {
    insertDataAndCheckResult()
    sql("DELETE FROM TABLE table1 WHERE SEGMENT.ID IN(0)")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 2, 1), Row("abc", 3, 2)))
    deleteTableStatusVersionFile("table1")
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    // cannot recover deleted segment, as the delete info exists only in table status file
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }
}
