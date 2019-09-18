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

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, DataFrame, Row}
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport
import org.apache.carbondata.sdk.file.{CarbonReader, CarbonWriter}
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.junit.Assert

import scala.io.Source

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


  test("Test update on added segment") {
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


  test("Test added segment with different format") {
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
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")

    sql("select * from addsegment2").show()
    val table = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier( "addsegment2"))
    val path = table.location.getPath
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='parquet')").show()
    assert(sql("select empname, designation, doj, workgroupcategory , workgroupcategoryname   from addsegment1").collect().length == 20)
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind"),Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test added segment with different format more than two") {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
    sql("drop table if exists addsegment3")
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
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")

    sql(
      """
        | CREATE TABLE addsegment3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int) using orc
      """.stripMargin)

    sql(s"""insert into addsegment3 select * from addsegment1""")

    val newPath1 = copyseg("addsegment2", "addsegtest1")
    val newPath2 = copyseg("addsegment3", "addsegtest2")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath1', 'format'='parquet')").show()
    sql(s"alter table addsegment1 add segment options('path'='$newPath2', 'format'='orc')").show()
    assert(sql("select empname, designation, doj, workgroupcategory , workgroupcategoryname   from addsegment1").collect().length == 30)
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind"),Row("arvind"),Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(30)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    FileFactory.deleteAllFilesOfDir(new File(newPath2))
  }

  test("Test added segment with different format more than two and use set segment") {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
    sql("drop table if exists addsegment3")
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
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")

    sql(
      """
        | CREATE TABLE addsegment3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int) using orc
      """.stripMargin)

    sql(s"""insert into addsegment3 select * from addsegment1""")

    val newPath1 = copyseg("addsegment2", "addsegtest1")
    val newPath2 = copyseg("addsegment3", "addsegtest2")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath1', 'format'='parquet')").show()
    sql(s"alter table addsegment1 add segment options('path'='$newPath2', 'format'='orc')").show()

    assert(sql("select empname, designation, doj, workgroupcategory , workgroupcategoryname   from addsegment1").collect().length == 30)

    sql("SET carbon.input.segments.default.addsegment1 = 0")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql("SET carbon.input.segments.default.addsegment1 = 0,1")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind"),Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))

    sql("SET carbon.input.segments.default.addsegment1 = *")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind"),Row("arvind"),Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(30)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    FileFactory.deleteAllFilesOfDir(new File(newPath2))
  }

  test("Test added segment with different format and test compaction") {
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
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val table = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier( "addsegment2"))
    val path = table.location.getPath
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='parquet')").show()
    sql("alter table addsegment1 compact 'major'")
    assert(sql("select empname, designation, doj, workgroupcategory , workgroupcategoryname   from addsegment1").collect().length == 40)
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(40)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(40)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test filter queries on mixed formats table") {
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

    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql(
      """
        | CREATE TABLE addsegment2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)
    sql(s"""insert into addsegment2 select * from addsegment1""")

    val table = sqlContext.sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location.getPath
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    copy(path, newPath)

    val res1 = sql("select empname, deptname from addsegment1 where deptno=10")

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='parquet')")

    val res2 = sql("select * from addsegment1 where deptno=10")
    assert(res1.collect().length == 6)
    assert(res2.collect().length == 6)
    assert(sql("select empname, deptname, deptno from addsegment1 where empname = 'arvind'")
      .collect().length == 2)

    // For testing filter columns not in projection list
    assert(sql("select deptname, deptno from addsegment1 where empname = 'arvind'")
             .collect().length == 2)

    assert(sql("select deptname, sum(salary) from addsegment1 where empname = 'arvind' group by deptname").collect().length == 1)
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }


  test("Test show segments for added segment with different format") {
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
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val table = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier( "addsegment2"))
    val path = table.location.getPath
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='parquet')").show()
    checkExistence(sql(s"show segments for table addsegment1"), true, "spark-common/target/warehouse/addsegtest")
    checkExistence(sql(s"show history segments for table addsegment1"), true, "spark-common/target/warehouse/addsegtest")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test parquet table") {
    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")

    sql("create table addSegCar(a int, b string) stored by 'carbondata'")
    sql("create table addSegPar(a int, b string) using parquet")
    sql("create table addSegParless(a int) using parquet")
    sql("create table addSegParmore(a int, b string, c string) using parquet")

    sql("insert into addSegCar values (1,'a')")
    sql("insert into addSegPar values (2,'b')")
    sql("insert into addSegParless values (3)")
    sql("insert into addSegParmore values (4,'c', 'x')")

    val table1 = sqlContext.sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier("addSegPar"))
    val table2 = sqlContext.sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier("addSegParless"))
    val table3 = sqlContext.sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier("addSegParmore"))

    sql(s"alter table addSegCar add segment options('path'='${table1.location.getPath}', 'format'='parquet')")
    intercept[Exception] {
      sql(s"alter table addSegCar add segment options('path'='${table2.location.getPath}', 'format'='parquet')")
    }
    sql(s"alter table addSegCar add segment options('path'='${table3.location.getPath}', 'format'='parquet')")

    assert(sql("select * from addSegCar").collect().length == 3)

    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")
  }

  private def copyseg(tableName: String, pathName: String): String = {
    val table1 = sqlContext.sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val path1 = table1.location.getPath
    val newPath1 = storeLocation + "/" + pathName
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    copy(path1, newPath1)
    newPath1
  }

  test("Test add segment by carbon written by sdk") {
    val tableName = "add_segment_test"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
        | CREATE TABLE $tableName (empno int, empname string, designation String, doj Timestamp,
        | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        | utilization int,salary int)
        | STORED AS carbondata
        |""".stripMargin)

    val externalSegmentPath = storeLocation + "/" + "external_segment"
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))

    // write into external segment folder
    val schemaFilePath = s"$storeLocation/$tableName/Metadata/schema"
    val writer = CarbonWriter.builder
      .outputPath(externalSegmentPath)
      .withSchemaFile(schemaFilePath)
      .writtenBy("AddSegmentTestCase")
      .withCsvInput()
      .build()
    val source = Source.fromFile(s"$resourcesPath/data.csv")
    var count = 0
    for (line <- source.getLines()) {
      if (count != 0) {
        writer.write(line.split(","))
      }
      count = count + 1
    }
    writer.close()

    sql(s"alter table $tableName add segment options('path'='$externalSegmentPath', 'format'='carbon')").show()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(10)))
    sql(s"select * from $tableName").show()

    expectSameResultBySchema(externalSegmentPath, schemaFilePath, tableName)
    expectSameResultInferSchema(externalSegmentPath, tableName)

    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
  }

  /**
   * use sdk to read the specified path using specified schema file
   * and compare result with select * from tableName
   */
  def expectSameResultBySchema(pathToRead: String, schemaFilePath: String, tableName: String): Unit = {
    val tableRows = sql(s"select * from $tableName").collectAsList()
    val projection = Seq("empno", "empname", "designation", "doj",
      "workgroupcategory", "workgroupcategoryname", "deptno", "deptname",
      "projectcode", "projectjoindate", "projectenddate", "attendance",
      "utilization", "salary").toArray
    val reader = CarbonReader.builder(pathToRead)
      .withRowRecordReader()
      .withReadSupport(classOf[CarbonRowReadSupport])
      .projection(projection)
      .build()

    var count = 0
    while (reader.hasNext) {
      val row = reader.readNextRow.asInstanceOf[CarbonRow]
      val tableRow = tableRows.get(count)
      var columnIndex = 0
      for (column <- row.getData) {
        val tableRowColumn = tableRow.get(columnIndex)
        Assert.assertEquals(s"cell[$count, $columnIndex] not equal", tableRowColumn.toString, column.toString)
        columnIndex = columnIndex + 1
      }
      count += 1
    }
    reader.close()
  }

  /**
   * use sdk to read the specified path by inferring schema
   * and compare result with select * from tableName
   */
  def expectSameResultInferSchema(pathToRead: String, tableName: String): Unit = {
    val tableRows = sql(s"select * from $tableName").collectAsList()
    val projection = Seq("empno", "empname", "designation", "doj",
      "workgroupcategory", "workgroupcategoryname", "deptno", "deptname",
      "projectcode", "projectjoindate", "projectenddate", "attendance",
      "utilization", "salary").toArray
    val reader = CarbonReader.builder(pathToRead)
      .withRowRecordReader()
      .withReadSupport(classOf[CarbonRowReadSupport])
      .projection(projection)
      .build()

    var count = 0
    while (reader.hasNext) {
      val row = reader.readNextRow.asInstanceOf[CarbonRow]
      val tableRow = tableRows.get(count)
      var columnIndex = 0
      for (column <- row.getData) {
        val tableRowColumn = tableRow.get(columnIndex)
        Assert.assertEquals(s"cell[$count, $columnIndex] not equal", tableRowColumn.toString, column.toString)
        columnIndex = columnIndex + 1
      }
      count += 1
    }
    reader.close()
  }

  test("Test add segment by carbon written by sdk, and 1 load") {
    val tableName = "add_segment_test"
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (empno int, empname string, designation String, doj Timestamp,
         | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
         | utilization int,salary int)
         | STORED AS carbondata
         |""".stripMargin)

    sql(
      s"""
        |LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE $tableName
        |OPTIONS('DELIMITER'=',', 'QUOTECHAR'='"')
        |""".stripMargin)

    val externalSegmentPath = storeLocation + "/" + "external_segment"
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))

    // write into external segment folder
    val writer = CarbonWriter.builder
      .outputPath(externalSegmentPath)
      .withSchemaFile(s"$storeLocation/$tableName/Metadata/schema")
      .writtenBy("AddSegmentTestCase")
      .withCsvInput()
      .build()
    val source = Source.fromFile(s"$resourcesPath/data.csv")
    var count = 0
    for (line <- source.getLines()) {
      if (count != 0) {
        writer.write(line.split(","))
      }
      count = count + 1
    }
    writer.close()

    sql(s"alter table $tableName add segment options('path'='$externalSegmentPath', 'format'='carbon')").show()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(20)))
    checkAnswer(sql(s"select sum(empno) from $tableName where empname = 'arvind' "), Seq(Row(22)))
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
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
    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")
  }

}
