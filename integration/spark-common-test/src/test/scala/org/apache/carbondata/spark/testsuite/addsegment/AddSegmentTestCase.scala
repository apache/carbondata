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
package org.apache.carbondata.spark.testsuite.addsegment

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, DataFrame, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.rdd.CarbonScanRDD

class AddSegmentTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

  }

  test("Test add segment ") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')").show()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test added segment drop") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')").show()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("delete from table addsegment1 where segment.id in (2)")
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 0, "Added segment path should be deleted when clean files are called")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test compact on added segment") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')").show()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("alter table addsegment1 compact 'major'").show()
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 0, "Added segment path should be deleted when clean files are called")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test compact on multiple added segments") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    for (i <- 0 until 10) {
      copy(path, newPath+i)
    }

    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    for (i <- 0 until 10) {
      sql(s"alter table addsegment1 add segment options('path'='${newPath+i}', 'format'='carbon')").show()

    }
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(110)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(110)))
    sql("alter table addsegment1 compact 'minor'").show()
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(110)))
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 0, "Added segment path should be deleted when clean files are called")
    for (i <- 0 until 10) {
      FileFactory.deleteAllFilesOfDir(new File(newPath+i))
    }
  }

  test("Test add segment and drop table should deletes all segments") {
    val tableName = "add_segment_test"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (empno int, empname string, designation String, doj Timestamp,
         | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
         | utilization int,salary int)
         | STORED AS carbondata
      """.stripMargin)

    sql(
      s"""
         |LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE $tableName
         |OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')
      """.stripMargin)
    var count = 10

    // write 3 external segments
    (1 to 3).foreach { i =>
      val externalSegmentPath = s"$storeLocation/external_segment$i"
      FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))

      val writer = CarbonWriter.builder
        .outputPath(externalSegmentPath)
        .withSchemaFile(s"$storeLocation/$tableName/Metadata/schema")
        .writtenBy("AddSegmentTestCase")
        .withCsvInput()
        .build()
      val source = Source.fromFile(s"$resourcesPath/data.csv")
      for (line <- source.getLines()) {
        if (count != 0) {
          writer.write(line.split(","))
        }
        count = count + 1
      }
      writer.close()
      sql(s"alter table $tableName add segment options('path'='$externalSegmentPath', 'format'='carbon')").show()
    }
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(count)))
    sql(s"drop table $tableName")

    // drop table should have deleted all external segment folders
    (1 to 3).foreach { i =>
      assertResult(false)(new File(s"$storeLocation/external_segment$i").exists())
    }
  }

  test("Test update on added segment ") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')").show()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("""update addsegment1 d  set (d.empname) = ('ravi') where d.empname = 'arvind'""").show()
    checkAnswer(sql("select count(*) from addsegment1 where empname='ravi'"), Seq(Row(2)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test validation on added segment") {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql(
      """
        | CREATE TABLE addsegment2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    sql("select count(*) from addsegment1").show()
    val table = CarbonEnv.getCarbonTable(None, "addsegment2") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    val ex = intercept[Exception] {
      sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')").show()
    }
    assert(ex.getMessage.contains("Schema is not same"))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  def copy(oldLoc: String, newLoc: String): Unit = {
    val oldFolder = FileFactory.getCarbonFile(oldLoc)
    FileFactory.mkdirs(newLoc, FileFactory.getConfiguration)
    val oldFiles = oldFolder.listFiles
    for (file <- oldFiles) {
      Files.copy(Paths.get(file.getParentFile.getPath, file.getName), Paths.get(newLoc, file.getName))
    }
  }


  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
  }

}
