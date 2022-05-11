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

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.recovery.tablestatus.TableStatusRecovery
import org.apache.carbondata.sdk.file.CarbonWriter

class TableStatusRecoveryTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS table1")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS table1")
  }

  test("test table status recovery if file is lost after first insert") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (c1 string,c2 int, c3 int) STORED AS carbondata")
    sql("insert into table1 values('abc',1, 1)")
    checkAnswer(sql("select * from table1"), Seq(Row("abc", 1, 1)))
    deleteTableStatusVersionFile
    checkAnswer(sql("select count(*) from table1"), Seq(Row(0)))
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"), Seq(Row("abc", 1, 1)))
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
    deleteTableStatusVersionFile
    checkAnswer(sql("select count(*) from table1"), Seq(Row(0)))
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select name from table1"), Seq(Row("blue"), Row("red"), Row("black")))
    sql("alter table table1 drop partition(email='def')")
    checkAnswer(sql("select name from table1"), Seq(Row("blue")))
    deleteTableStatusVersionFile
    TableStatusRecovery.main(args.split(" "))
    sql("select name from table1").show(false)
  }

  private def deleteTableStatusVersionFile: Unit = {
    val table = CarbonEnv.getCarbonTable(Some("default"), "table1")(sqlContext.sparkSession)
    val currVersion = table.getTableStatusVersion
    val status = FileFactory.getCarbonFile(CarbonTablePath.getTableStatusFilePath(
      table.getTablePath, currVersion)).deleteFile()
    assert(status.equals(true))
  }

  private def verifyScenario_Insert(): Unit = {
    insertData()
    deleteTableStatusVersionFile
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def insertData(): Unit = {
    sql("insert into table1 values('abc',1, 1)")
    sql("insert into table1 values('abc', 2, 1)")
    sql("insert into table1 values('abc', 3, 2)")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  def verifyScenario_IUD(): Unit = {
    insertData()
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
    deleteTableStatusVersionFile
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }

  private def verifyScenario_Delete_Segment(): Unit = {
    insertData()
    sql("DELETE FROM TABLE table1 WHERE SEGMENT.ID IN(0)")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 2, 1), Row("abc", 3, 2)))
    deleteTableStatusVersionFile
    val args = "default table1"
    TableStatusRecovery.main(args.split(" "))
    // cannot recover deleted segment, as the delete info exists only in table status file
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc", 1, 1), Row("abc", 2, 1), Row("abc", 3, 2)))
  }
}
