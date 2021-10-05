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

import scala.io.Source

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.util.SparkSQLUtil
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.datatype.{DataTypes, Field}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTestUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.readsupport.impl.CarbonRowReadSupport
import org.apache.carbondata.sdk.file.{CarbonIUD, CarbonReader, CarbonWriter}

class AddSegmentTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable

    for (i <- 0 until 10) {
      FileFactory.deleteAllFilesOfDir(new File(storeLocation + "/" + "addsegtest" + i))
    }
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

  }

  test("Test add segment ") {

    createCarbonTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    val newPathWithLineSeparator = storeLocation + "/" + "addsegtest/"
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))

    // cannot add segment from same path file again so deleting previously added segment
    sql("delete from table addsegment1 where segment.id in (2)")
    sql(s"alter table addsegment1 add segment options('path'='$newPathWithLineSeparator', " +
        s"'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test update/delete operation on added segment which required horizontal compaction") {
    sql("drop table if exists uniqdata")
    sql("""CREATE TABLE  uniqdata(empname String, designation String, doj Timestamp,
          |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
          |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
          |  utilization int,salary int, empno int)
          | STORED AS carbondata""".stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE uniqdata
           | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    val table = CarbonEnv.getCarbonTable(None, "uniqdata")(sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path, newPath)

    sql(s"Alter table uniqdata add segment options ('path'='$newPath','format'='carbon')")
    // perform update/delete operation after add of new segment
    sql("delete from uniqdata where empno=11")
    sql("update uniqdata set (empname)=('nihal') where empno=12")

    checkAnswer(sql("select empname from uniqdata where empno=12"), Seq(Row("nihal"), Row("nihal")))
    checkAnswer(sql("select count(*) from uniqdata where empno=11"), Seq(Row(0)))

    sql("set carbon.input.segments.default.uniqdata=1")
    // after update new data will be present in new segment
    checkAnswer(sql("select count(*) from uniqdata where empno=12"), Seq(Row(0)))
    checkAnswer(sql("select count(*) from uniqdata where empno=11"), Seq(Row(0)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    sql("drop table if exists uniqdata")
  }

  test("test add segment with SI when parent and SI segments are not in sunc") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    try {
      createCarbonTable()
      sql(
        s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1
           | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
      val table = CarbonEnv.getCarbonTable(None, "addsegment1")(sqlContext.sparkSession)
      val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
      val newPath = storeLocation + "/" + "addsegtest"
      FileFactory.deleteAllFilesOfDir(new File(newPath))
      CarbonTestUtil.copy(path, newPath)
      sql("delete from table addsegment1 where segment.id in (0)")
      sql("clean files for table addsegment1 options('force'='true')")
      sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
          .collect()
      sql("DROP INDEX IF EXISTS addsegment_si on addsegment1")
      sql("create index addsegment_si on addsegment1(workgroupcategoryname) as 'carbondata'")
      assert(sql("show segments on addsegment_si").collect().length == 1)
      checkAnswer(sql("select count(*) from addsegment1 where workgroupcategoryname='developer'"),
        Seq(Row(10)))
      sql("delete from table addsegment_si where segment.id in (1)")
      sql("clean files for table addsegment_si options('force'='true')")
      assert(sql("show segments on addsegment_si").collect().length == 0)
      checkAnswer(sql("select count(*) from addsegment1 where workgroupcategoryname='developer'"),
        Seq(Row(10)))
      sql("drop table if exists addsegment1")
    }
    finally {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
    }
  }

  test("Test add segment for the segment having delete delta files") {
    createCarbonTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1
         | OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(s"delete from addsegment1 where empno = 12").collect()
    sql(s"delete from addsegment1 where empno = 13").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(16)))
    checkAnswer(sql("select count(*) from addsegment1 where empno = 12"), Seq(Row(0)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    sql("drop table if exists addsegment1")
  }

  test("Test add segment for the segment having multiple blocks and delete delta files") {
    sql("drop table if exists addsegment1")
    sql(
      """
        | CREATE TABLE addsegment1 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='empno', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='5')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='5')""".stripMargin)
    sql(s"delete from addsegment1 where empno = 12").collect()
    sql(s"delete from addsegment1 where empno = 17").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    sql(s"alter table addsegment1 add segment" +
      s" options('path'='$newPath', 'format'='carbon')").collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(16)))
    checkAnswer(sql("select count(*) from addsegment1 where empno = 12"), Seq(Row(0)))
    checkAnswer(sql("select count(*) from addsegment1 where empno = 17"), Seq(Row(0)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    sql("drop table if exists addsegment1")
  }

  test("Test add segment by carbon written by sdk having delete delta files") {
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
    CarbonIUD.getInstance().delete(externalSegmentPath, "empno", "12").commit()
    CarbonIUD.getInstance().delete(externalSegmentPath, "empno", "13").commit()
    sql(s"alter table $tableName add segment " +
      s"options('path'='$externalSegmentPath', 'format'='carbon')").collect()
    // To clear the indexes from cache, which were cached after delete operation on SDK segments.
    CacheProvider.getInstance().getCarbonCache.getCacheMap.clear()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(8)))
    checkAnswer(sql(s"select count(*) from $tableName where empno = 12"), Seq(Row(0)))
    checkAnswer(sql(s"select count(*) from $tableName where empno = 13"), Seq(Row(0)))
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
  }

  test("Test added segment drop") {

    createCarbonTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("delete from table addsegment1 where segment.id in (2)")
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 2,
      "Added segment path should not be deleted physically when clean files are called")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test compact on added segment") {

    createCarbonTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("alter table addsegment1 compact 'major'").collect()
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 2,
      "Added segment path should not be deleted physically when clean files are called")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test compact on multiple added segments") {

    createCarbonTable()

    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    for (i <- 0 until 10) {
      CarbonTestUtil.copy(path, newPath + i)
    }

    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    for (i <- 0 until 10) {
      sql(s"alter table addsegment1 add segment " +
          s"options('path'='${ newPath + i }', 'format'='carbon')").collect()

    }
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(110)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(110)))
    sql("alter table addsegment1 compact 'minor'").collect()
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(110)))
    sql("clean files for table addsegment1")
    val oldFolder = FileFactory.getCarbonFile(newPath)
    assert(oldFolder.listFiles.length == 0,
      "Added segment path should be deleted when clean files are called")
    for (i <- 0 until 10) {
      FileFactory.deleteAllFilesOfDir(new File(newPath + i))
    }
    dropTable
  }


  test("Test update on added segment") {

    createCarbonTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val newPath = storeLocation + "/" + "addsegtest"
    CarbonTestUtil.copy(path, newPath)
    sql("delete from table addsegment1 where segment.id in (1)")
    sql("clean files for table addsegment1")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='carbon')")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    sql("""update addsegment1 d  set (d.empname) = ('ravi') where d.empname = 'arvind'""")
      .collect()
    checkAnswer(sql("select count(*) from addsegment1 where empname='ravi'"), Seq(Row(2)))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test validation on added segment") {
    sql("drop table if exists addsegment2")
    createCarbonTable()

    sql(
      """
        | CREATE TABLE addsegment2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    sql("select count(*) from addsegment1").collect()
    val table = CarbonEnv.getCarbonTable(None, "addsegment2") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "0")
    val newPath = storeLocation + "/" + "addsegtest"
    CarbonTestUtil.copy(path, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    val ex = intercept[Exception] {
      sql("alter table addsegment1 add segment " +
          s"options('path'='$newPath', 'format'='carbon')").collect()
    }
    assert(ex.getMessage.contains("Schema is not same"))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test added segment with different format") {
    createCarbonTable()
    createParquetTable()

    sql("select * from addsegment2").collect()
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment " +
        s"options('path'='$newPath', 'format'='parquet')").collect()
    assert(sql("select empname, designation, doj, workgroupcategory, " +
               "workgroupcategoryname from addsegment1").collect().length == 20)
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"),
      Seq(Row("arvind"), Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    val showSeg = sql("show segments for table addsegment1").collectAsList()
    val descFormattedSize = sql("desc formatted addsegment1")
      .collect()
      .filter(_.get(0).toString.startsWith("Table Data Size"))
      .head
      .get(1)
      .toString
    val size = getDataSize(newPath)
    assert(showSeg.get(0).getString(7).equalsIgnoreCase("parquet"))
    assert(descFormattedSize.split("KB")(0).toDouble > 0.0d)
    assert(showSeg.get(0).get(5).toString.equalsIgnoreCase(size))
    assert(showSeg.get(0).get(6).toString.equalsIgnoreCase("NA"))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test add segment with different formats and vector reader disabled") {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    createCarbonTable()
    createParquetTable()
    createOrcTable()

    val newPath1 = copyseg("addsegment2", "addsegtest1")
    val newPath2 = copyseg("addsegment3", "addsegtest2")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql("alter table addsegment1 add segment " +
        s"options('path'='$newPath1', 'format'='parquet')").collect()
    sql(s"alter table addsegment1 add segment options('path'='$newPath2', 'format'='orc')")
      .collect()
    assert(sql("select empname, designation, doj, workgroupcategory, " +
               "workgroupcategoryname   from addsegment1").collect().length == 30)
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"),
      Seq(Row("arvind"), Row("arvind"), Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(30)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    assert(sql("select deptname, deptno from addsegment1 where empname = 'arvind'")
             .collect().length == 3)
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    FileFactory.deleteAllFilesOfDir(new File(newPath2))
    sqlContext.setConf("carbon.enable.vector.reader",
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("Test update/delete blocking on mixed format segments") {
    createCarbonTable()
    createParquetTable()

    sql("select * from addsegment2").collect()
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)

    sql(s"alter table addsegment1 add segment options('path'='$newPath', 'format'='parquet')")
    val exception1 = intercept[MalformedCarbonCommandException](sql(
      """update addsegment1 d  set (d.empname) = ('ravi') where d.empname = 'arvind'""").collect())
    assertResult("Unsupported update operation on table containing mixed format segments")(
      exception1.getMessage())
    val exception2 = intercept[MalformedCarbonCommandException](sql(
      "delete from addsegment1 where deptno = 10"))
    assertResult("Unsupported delete operation on table containing mixed format segments")(
      exception2.getMessage())
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }


  test("Test delete by id for added parquet segment") {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
    sql("drop table if exists addsegment3")
    createCarbonTable()
    createParquetTable
    sql("select * from addsegment2").collect()
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))
    sql(
      """
        | CREATE TABLE addsegment3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)
    sql("create index one_one on table addsegment3(designation) as 'carbondata'")
    sql("alter table addsegment3 add segment " +
        s"options('path'='$newPath', 'format'='parquet')").collect()
    sql("show segments for table addsegment3").collect()
    sql("delete from table addsegment1 where segment.id in(0)")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(0)))
    sql("clean files for table addsegment1")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }


  test("Test delete by id for added segment") {
    createCarbonTable()
    createParquetTable

    sql("select * from addsegment2").collect()
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment " +
        s"options('path'='$newPath', 'format'='parquet')").collect()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)

    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(40)))
    sql("show segments for table addsegment1").collect()
    sql("delete from table addsegment1 where segment.id in(3)")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    sql("show segments for table addsegment1").collect()
    sql("delete from table addsegment1 where segment.id in(2)")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))
    sql("show segments for table addsegment1").collect()
    sql("delete from table addsegment1 where segment.id in(0,1)")
    sql("show segments for table addsegment1").collect()

    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(0)))
    sql("clean files for table addsegment1")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("Test added segment with different format more than two") {
    createCarbonTable()
    createParquetTable()
    createOrcTable()

    val newPath1 = copyseg("addsegment2", "addsegtest1")
    val newPath2 = copyseg("addsegment3", "addsegtest2")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql("alter table addsegment1 add segment " +
        s"options('path'='$newPath1', 'format'='parquet')").collect()
    sql(s"alter table addsegment1 add segment options('path'='$newPath2', 'format'='orc')")
      .collect()
    assert(sql("select empname, designation, doj, workgroupcategory, " +
               "workgroupcategoryname   from addsegment1").collect().length == 30)
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"),
      Seq(Row("arvind"), Row("arvind"), Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(30)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    assert(sql("select deptname, deptno from addsegment1 where empname = 'arvind'")
             .collect().length == 3)
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    FileFactory.deleteAllFilesOfDir(new File(newPath2))
  }

  test("Test added segment with different format more than two and use set segment") {
    createCarbonTable()
    createParquetTable()
    createOrcTable()

    val newPath1 = copyseg("addsegment2", "addsegtest1")
    val newPath2 = copyseg("addsegment3", "addsegtest2")
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql(s"alter table addsegment1 add segment " +
        s"options('path'='$newPath1', 'format'='parquet')").collect()
    sql(s"alter table addsegment1 add segment options('path'='$newPath2', 'format'='orc')")
      .collect()

    assert(sql("select empname, designation, doj, workgroupcategory, " +
               "workgroupcategoryname   from addsegment1").collect().length == 30)

    sql("SET carbon.input.segments.default.addsegment1 = 0")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"), Seq(Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(10)))

    sql("SET carbon.input.segments.default.addsegment1 = 0,1")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"),
      Seq(Row("arvind"), Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(20)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(20)))

    sql("SET carbon.input.segments.default.addsegment1 = *")
    checkAnswer(sql("select empname from addsegment1 where empname='arvind'"),
      Seq(Row("arvind"), Row("arvind"), Row("arvind")))
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(30)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    FileFactory.deleteAllFilesOfDir(new File(newPath2))
  }

  test("Test added segment with different format and test compaction") {
    createCarbonTable()
    createParquetTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))

    sql(s"alter table addsegment1 add segment " +
        s"options('path'='$newPath', 'format'='parquet')").collect()
    sql("alter table addsegment1 compact 'major'")
    assert(sql("select empname, designation, doj, workgroupcategory, " +
               "workgroupcategoryname from addsegment1").collect().length == 40)
    checkAnswer(sql("select count(empname) from addsegment1"), Seq(Row(40)))
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(40)))
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test filter queries on mixed formats table") {
    createCarbonTable()
    createParquetTable()

    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)

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

    assert(sql("select deptname, sum(salary) from addsegment1 " +
               "where empname = 'arvind' group by deptname").collect().length == 1)
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }


  test("Test show segments for added segment with different format") {
    createCarbonTable()
    createParquetTable()
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv'
         | INTO TABLE addsegment1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addsegment2"))
    val path = table.location
    val newPath = storeLocation + "/" + "addsegtest"
    FileFactory.deleteAllFilesOfDir(new File(newPath))
    CarbonTestUtil.copy(path.toString, newPath)
    checkAnswer(sql("select count(*) from addsegment1"), Seq(Row(30)))

    sql("alter table addsegment1 add segment " +
        s"options('path'='$newPath', 'format'='PARQUET')").collect()
    checkExistence(sql("show segments for table addsegment1 " +
                       "as select * from addsegment1_segments"),
      true,
      "spark/target/warehouse/addsegtest")
    checkExistence(sql("show history segments for table addsegment1 " +
                       "as select * from addsegment1_segments"),
      true,
      "spark/target/warehouse/addsegtest")
    FileFactory.deleteAllFilesOfDir(new File(newPath))
  }

  test("test parquet table") {
    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")

    sql("create table addSegCar(a int, b string) STORED AS carbondata")
    sql("create table addSegPar(a int, b string) using parquet")
    sql("create table addSegParless(a int) using parquet")
    sql("create table addSegParmore(a int, b string, c string) using parquet")

    sql("insert into addSegCar values (1,'a')")
    sql("insert into addSegPar values (2,'b')")
    sql("insert into addSegParless values (3)")
    sql("insert into addSegParmore values (4,'c', 'x')")

    val table1 = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addSegPar"))
    val table2 = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addSegParless"))
    val table3 = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("addSegParmore"))

    sql("alter table addSegCar add segment " +
        s"options('path'='${table1.location}', 'format'='parquet')")
    assert(intercept[Exception] {
      sql("alter table addSegCar add segment " +
          s"options('path'='${table2.location}', 'format'='parquet')")
    }.getMessage
      .contains(
        "Schema is not same. Table schema is : StructType(StructField(a,IntegerType,true), " +
        "StructField(b,StringType,true)) and segment schema is : StructType(StructField(a," +
        "IntegerType,false))"))
    sql("alter table addSegCar add segment " +
        s"options('path'='${table3.location}', 'format'='parquet')")

    assert(sql("select * from addSegCar").collect().length == 3)

    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")
  }

  test("test add segment partition table") {
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
    sql("drop table if exists orc_table")

    sql("create table parquet_table(" +
        "value int, name string, age int) using parquet partitioned by (name, age)")
    sql("create table carbon_table(" +
        "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into parquet_table values (30, 'amy', 12), (40, 'bob', 13)")
    sql("insert into parquet_table values (30, 'amy', 20), (10, 'bob', 13)")
    sql("insert into parquet_table values (30, 'cat', 12), (40, 'dog', 13)")
    sql("select * from parquet_table").collect()
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("parquet_table")).location

    // add data from parquet table to carbon table
    sql(s"alter table carbon_table add segment options ('path'='$parquetRootPath', " +
        "'format'='parquet', 'partition'='name:string,age:int')")
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))

    // load new data into carbon table
    sql("insert into carbon_table select * from parquet_table")
    checkAnswer(sql("select * from carbon_table"),
      sql("select * from parquet_table union all select * from parquet_table"))

    // add another data from orc table to carbon table
    sql("create table orc_table(" +
        "value int, name string, age int) using orc partitioned by (name, age)")
    sql("insert into orc_table values (30, 'orc', 50), (40, 'orc', 13)")
    sql("insert into orc_table values (30, 'fast', 10), (10, 'fast', 13)")
    val orcRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("orc_table")).location
    sql("alter table carbon_table add segment " +
        s"options ('path'='$orcRootPath', 'format'='orc', 'partition'='name:string,age:int')")
    checkAnswer(sql("select * from carbon_table"),
      sql("select * from parquet_table " +
          "union all select * from parquet_table " +
          "union all select * from orc_table"))

    // filter query on partition column
    checkAnswer(sql("select count(*) from carbon_table where name = 'amy'"), Row(4))

    // do compaction
    sql("alter table carbon_table compact 'major'")
    checkAnswer(sql("select * from carbon_table"),
      sql("select * from parquet_table " +
          "union all select * from parquet_table " +
          "union all select * from orc_table"))

    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
    sql("drop table if exists orc_table")
  }

  test("show segment after add segment to partition table") {
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")

    sql("create table parquet_table(" +
        "value int, name string, age int) using parquet partitioned by (name, age)")
    sql("create table carbon_table(" +
        "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into parquet_table values (30, 'amy', 12), (40, 'bob', 13)")
    sql("insert into parquet_table values (30, 'amy', 20), (10, 'bob', 13)")
    sql("insert into parquet_table values (30, 'cat', 12), (40, 'dog', 13)")
    sql("select * from parquet_table").collect()
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("parquet_table")).location

    // add data from parquet table to carbon table
    sql(s"alter table carbon_table add segment options ('path'='$parquetRootPath', " +
        s"'format'='parquet', 'partition'='name:string,age:int')")
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))

    // test show segment
    checkExistence(sql("show segments for table carbon_table " +
                       "as select * from carbon_table_segments"),
      true,
      "spark/target/warehouse/parquet_table")
    checkExistence(sql("show history segments for table carbon_table " +
                       "as select * from carbon_table_segments"),
      true,
      "spark/target/warehouse/parquet_table")

    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
  }

  test("test add segment partition table, missing partition option") {
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")

    sql("create table parquet_table(" +
        "value int, name string, age int) using parquet partitioned by (name, age)")
    sql("create table carbon_table(" +
        "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into parquet_table values (30, 'amy', 12), (40, 'bob', 13)")
    sql("insert into parquet_table values (30, 'amy', 20), (10, 'bob', 13)")
    sql("insert into parquet_table values (30, 'cat', 12), (40, 'dog', 13)")
    sql("select * from parquet_table").collect()
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("parquet_table")).location

    // add data from parquet table to carbon table
    val exception = intercept[AnalysisException](
      sql("alter table carbon_table add segment " +
          s"options ('path'='$parquetRootPath', 'format'='parquet')")
    )
    assert(exception.message.contains("partition option is required"))

    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
  }

  test("test add segment partition table, unmatched partition") {
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")

    sql("create table parquet_table(" +
        "value int, name string, age int) using parquet partitioned by (name)")
    sql("create table carbon_table(" +
        "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into parquet_table values (30, 12, 'amy'), (40, 13, 'bob')")
    sql("insert into parquet_table values (30, 20, 'amy'), (10, 13, 'bob')")
    sql("insert into parquet_table values (30, 12, 'cat'), (40, 13, 'dog')")
    sql("select * from parquet_table").collect()
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("parquet_table")).location

    // add data from parquet table to carbon table
    // unmatched partition
    var exception = intercept[AnalysisException](
      sql("alter table carbon_table add segment " +
          s"options ('path'='$parquetRootPath', 'format'='parquet', 'partition'='name:string')")
    )
    assert(exception.message.contains("Partition is not same"))

    // incorrect partition option
    exception = intercept[AnalysisException](
      sql(s"alter table carbon_table add segment options ('path'='$parquetRootPath', " +
          "'format'='parquet', 'partition'='name:string,age:int')")
    )
    assert(exception.message.contains(
      "input segment path does not comply to partitions in carbon table"))

    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
  }

  test("test add segment partition table, incorrect partition") {
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")

    sql("create table parquet_table(value int) using parquet")
    sql("create table carbon_table(" +
        "value int) partitioned by (name string, age int) stored as carbondata")
    sql("insert into parquet_table values (30), (40)")
    sql("select * from parquet_table").collect()
    val parquetRootPath = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier("parquet_table")).location

    // add data from parquet table to carbon table
    // incorrect partition option
    val exception = intercept[RuntimeException](
      sql(s"alter table carbon_table add segment options ('path'='$parquetRootPath', " +
          "'format'='parquet', 'partition'='name:string,age:int')")
    )
    assert(exception.getMessage.contains("invalid partition path"))

    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
  }

  private def copyseg(tableName: String, pathName: String): String = {
    val table1 = SparkSQLUtil.sessionState(sqlContext.sparkSession).catalog
      .getTableMetadata(TableIdentifier(tableName))
    val path1 = table1.location
    val newPath1 = storeLocation + "/" + pathName
    FileFactory.deleteAllFilesOfDir(new File(newPath1))
    CarbonTestUtil.copy(path1.toString, newPath1)
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

    sql(s"alter table $tableName add segment " +
        s"options('path'='$externalSegmentPath', 'format'='carbon')").collect()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(10)))
    sql(s"select * from $tableName").collect()

    expectSameResultBySchema(externalSegmentPath, schemaFilePath, tableName)
    expectSameResultInferSchema(externalSegmentPath, tableName)

    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
  }

  /**
   * use sdk to read the specified path using specified schema file
   * and compare result with select * from tableName
   */
  def expectSameResultBySchema(pathToRead: String,
      schemaFilePath: String,
      tableName: String): Unit = {
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
        Assert.assertEquals(s"cell[$count, $columnIndex] not equal",
          tableRowColumn.toString,
          column.toString)
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
        Assert.assertEquals(s"cell[$count, $columnIndex] not equal",
          tableRowColumn.toString,
          column.toString)
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

    var fields: Array[Field] = new Array[Field](14)
    fields(0) = new Field("empno", DataTypes.INT)
    fields(1) = new Field("empname", DataTypes.STRING)
    fields(2) = new Field("designation", DataTypes.STRING)
    fields(3) = new Field("doj", DataTypes.TIMESTAMP)
    fields(4) = new Field("workgroupcategory", DataTypes.INT)
    fields(5) = new Field("workgroupcategoryname", DataTypes.STRING)
    fields(6) = new Field("deptno", DataTypes.INT)
    fields(7) = new Field("deptname", DataTypes.STRING)
    fields(8) = new Field("projectcode", DataTypes.INT)
    fields(9) = new Field("projectjoindate", DataTypes.TIMESTAMP)
    fields(10) = new Field("projectenddate", DataTypes.DATE)
    fields(11) = new Field("attendance", DataTypes.INT)
    fields(12) = new Field("utilization", DataTypes.INT)
    fields(13) = new Field("salary", DataTypes.INT)


    // write into external segment folder
    val writer = CarbonWriter.builder
      .outputPath(externalSegmentPath)
      .writtenBy("AddSegmentTestCase")
      .withSchemaFile(CarbonTablePath.getSchemaFilePath(CarbonEnv.getCarbonTable(None,
        tableName)(sqlContext.sparkSession).getTablePath))
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

    sql(s"alter table $tableName add segment " +
        s"options('path'='$externalSegmentPath', 'format'='carbon')").collect()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(20)))
    checkAnswer(sql(s"select count(*) from $tableName where empno = 11"), Seq(Row(2)))
    checkAnswer(sql(s"select sum(empno) from $tableName where empname = 'arvind' "), Seq(Row(22)))
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
  }

  test("Test add segment by sdk written segment having timestamp in nanoseconds") {
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
      .uniqueIdentifier(System.nanoTime())
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

    sql(s"alter table $tableName add segment " +
      s"options('path'='$externalSegmentPath', 'format'='carbon')").collect()
    sql(s"delete from $tableName where empno = 12").collect()
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(9)))
    FileFactory.deleteAllFilesOfDir(new File(externalSegmentPath))
    sql(s"drop table $tableName")
  }

  test("Test Adding invalid segment which is already present inside the table") {
    createCarbonTable()
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE addsegment1 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
    val table = CarbonEnv.getCarbonTable(None, "addsegment1") (sqlContext.sparkSession)
    val path = CarbonTablePath.getSegmentPath(table.getTablePath, "1")
    val ex = intercept[Exception] {
      sql("alter table addsegment1 add segment " +
          s"options('path'='$path', 'format'='carbon')").collect()
    }
    assert(ex.getMessage.contains("can not add same segment path repeatedly"))
  }

  test("Test add segment with empty path") {
    createCarbonTable()
    val ex = intercept[Exception] {
      sql("alter table addsegment1 add segment " +
          s"options('path'='', 'format'='carbon')").collect()
    }
    assert(ex.getMessage.contains("PATH cannot be empty"))
  }

  def getDataSize(path: String): String = {
    val allFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonCommonConstants.PARQUET_FILE_EXT)
      }
    })
    var size: Long = 0
    for (file <- allFiles) {
      size += file.getSize
    }
    Strings.formatSize(size.toFloat)
  }

  private def createCarbonTable() = {
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
  }

  private def createParquetTable() = {
    sql("drop table if exists addsegment2")
    sql(
      """
        | CREATE TABLE addsegment2 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int) using parquet
      """.stripMargin)

    sql(s"""insert into addsegment2 select * from addsegment1""")
  }

  private def createOrcTable() = {
    sql("drop table if exists addsegment3")
    sql(
      """
        | CREATE TABLE addsegment3 (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int) using orc
      """.stripMargin)

    sql(s"""insert into addsegment3 select * from addsegment1""")
  }

  override def afterAll: Unit = {
    defaultConfig()
    sqlContext.sparkSession.conf.unset("carbon.input.segments.default.addsegment1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    dropTable
  }

  private def dropTable = {
    sql("drop table if exists addsegment1")
    sql("drop table if exists addsegment2")
    sql("drop table if exists addSegCar")
    sql("drop table if exists addSegPar")
    sql("drop table if exists addSegParless")
    sql("drop table if exists addSegParmore")
  }
}
