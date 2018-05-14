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

package org.apache.carbondata.spark.testsuite.createTable

import java.sql.Timestamp
import java.io.{File, FileFilter, IOException}
import java.util
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

import org.apache.avro
import org.apache.commons.lang.CharEncoding
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.sdk.file.{AvroCarbonWriter, CarbonWriter, CarbonWriterBuilder, Field, Schema}


class TestNonTransactionalCarbonTable extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
  writerPath = writerPath.replace("\\", "/")

  def buildTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, false, null)
  }

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(1000000, false, null)
  }

  def buildTestDataTwice(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, false, null)
    buildTestData(3, false, null)
  }

  def buildTestDataSameDirectory(): Any = {
    buildTestData(3, false, null)
  }

  def buildTestDataWithBadRecordForce(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FORCE").asJava
    buildTestData(3, false, options)
  }

  def buildTestDataWithBadRecordFail(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FAIL").asJava
    buildTestData(15001, false, options)
  }

  def buildTestDataWithBadRecordIgnore(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "IGNORE").asJava
    buildTestData(3, false, options)
  }

  def buildTestDataWithBadRecordRedirect(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "REDIRECT").asJava
    buildTestData(3, false, options)
  }

  def buildTestDataWithSortColumns(sortColumns: List[String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, false, null, sortColumns)
  }

  def buildTestData(rows: Int, persistSchema: Boolean, options: util.Map[String, String]): Any = {
    buildTestData(rows, persistSchema, options, List("name"))
  }

  // prepare sdk writer output
  def buildTestData(rows: Int,
      persistSchema: Boolean,
      options: util.Map[String, String],
      sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        if (persistSchema) {
          builder.persistSchemaFile(true)
          builder.withSchema(Schema.parseJson(schema))
            .sortBy(sortColumns.toArray)
            .outputPath(writerPath)
            .isTransactionalTable(false)
            .uniqueIdentifier(System.currentTimeMillis)
            .buildWriterForCSVInput()
        } else {
          if (options != null) {
            builder.withSchema(Schema.parseJson(schema)).outputPath(writerPath)
              .isTransactionalTable(false)
              .sortBy(sortColumns.toArray)
              .uniqueIdentifier(
                System.currentTimeMillis).withBlockSize(2).withLoadOptions(options)
              .buildWriterForCSVInput()
          } else {
            builder.withSchema(Schema.parseJson(schema)).outputPath(writerPath)
              .isTransactionalTable(false)
              .sortBy(sortColumns.toArray)
              .uniqueIdentifier(
                System.currentTimeMillis).withBlockSize(2)
              .buildWriterForCSVInput()
          }
        }
      var i = 0
      while (i < rows) {
        if ((options != null) && (i < 3)) {
          // writing a bad record
          writer.write(Array[String]( "robot" + i, String.valueOf(i.toDouble / 2), "robot"))
        } else {
          writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        }
        i += 1
      }
      if (options != null) {
        //Keep one valid record. else carbon data file will not generate
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)

      case _ => None
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int, sortColumns: Array[String]): Any = {
    val fields: Array[Field] = new Array[Field](3)
    // same column name, but name as boolean type
    fields(0) = new Field("name", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.withSchema(new Schema(fields)).outputPath(writerPath)
          .isTransactionalTable(false)
          .uniqueIdentifier(System.currentTimeMillis()).withBlockSize(2).sortBy(sortColumns)
          .buildWriterForCSVInput()

      var i = 0
      while (i < rows) {
        writer.write(Array[String]("true", String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _ => None
    }
  }

  // prepare sdk writer output
  def buildTestDataWithSameUUID(rows: Int,
      persistSchema: Boolean,
      options: util.Map[String, String],
      sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.withSchema(Schema.parseJson(schema)).outputPath(writerPath)
          .isTransactionalTable(false)
          .sortBy(sortColumns.toArray)
          .uniqueIdentifier(
            123).withBlockSize(2)
          .buildWriterForCSVInput()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)

      case _ => None
    }
  }

  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
  }

  def deleteFile(path: String, extension: String): Unit = {
    val file: CarbonFile = FileFactory
      .getCarbonFile(path, FileFactory.getFileType(path))

    for (eachDir <- file.listFiles) {
      if (!eachDir.isDirectory) {
        if (eachDir.getName.endsWith(extension)) {
          CarbonUtil.deleteFoldersAndFilesSilent(eachDir)
        }
      } else {
        deleteFile(eachDir.getPath, extension)
      }
    }
  }

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  test("concurrently insert operation"){
    cleanTestData()
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("drop table if exists t1")
    sql("create table if not exists t1 (name string, age int, height double) STORED BY 'org.apache.carbondata.format'")
    var i =0;
    while (i<50){
      sql (s"""insert into t1 values ("aaaaa", 12, 20)""").show(200,false)
      i = i+1;
    }
    checkAnswer(sql("select count(*) from t1"),Seq(Row(50)))
    val one = Future {
      sql("insert into sdkOutputTable select * from t1 ")
    }
    val two = Future {
      sql("insert into sdkOutputTable select * from t1 ")
    }

    Await.result(Future.sequence(Seq(one, two)), Duration(300, TimeUnit.SECONDS))

    checkAnswer(sql("select count(*) from sdkOutputTable"),Seq(Row(103)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test(
    "Read two sdk writer outputs before and after deleting the existing files and creating new " +
    "files with same schema and UUID") {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestDataWithSameUUID(3, false, null, List("name"))
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))
    new File(writerPath).listFiles().map(x => LOGGER.audit(x.getName +" : "+x.lastModified()))
    FileUtils.deleteDirectory(new File(writerPath))
    // Thread.sleep is required because it is possible sometime deletion
    // and creation of new file can happen at same timestamp.
    Thread.sleep(1000)
    assert(!new File(writerPath).exists())
    buildTestDataWithSameUUID(4, false, null, List("name"))
    new File(writerPath).listFiles().map(x => LOGGER.audit(x.getName +" : "+x.lastModified()))
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot3", 3, 1.5)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create external table with sort columns") {
    buildTestDataWithSortColumns(List("age","name"))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "age,name")

    checkExistence(sql("describe formatted sdkOutputTable"), true, writerPath)

    buildTestDataWithSortColumns(List("age"))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "age")

    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "name")

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create External Table with Schema with partition, should ignore schema and partition") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("test create External Table with insert into feature") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql("DROP TABLE IF EXISTS t1")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED BY 'org.apache.carbondata.format'")
    sql (s"""insert into t1 values ("aaaaa", 12, 20)""").show(200,false)
    sql("insert into sdkOutputTable select * from t1").show(200,false)

    checkAnswer(sql(s"""select * from sdkOutputTable where age = 12"""),
      Seq(Row("aaaaa", 12, 20.0)))

    sql("DROP TABLE sdkOutputTable")
    sql("drop table t1")

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create External Table with insert overwrite") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
    sql("DROP TABLE IF EXISTS t3")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED BY 'org.apache.carbondata.format'")
    sql (s"""insert into t1 values ("aaaaa", 12, 20)""").show(200,false)

    checkAnswer(sql(s"""select count(*) from sdkOutputTable where age = 1"""),
      Seq(Row(1)))

    sql("insert overwrite table sdkOutputTable select * from t1").show(200,false)

    checkAnswer(sql(s"""select count(*) from sdkOutputTable where age = 1"""),
      Seq(Row(0)))

    sql("DROP TABLE if exists sdkOutputTable")
    sql("drop table if exists t1")

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("test create External Table with Load") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
    sql("DROP TABLE IF EXISTS t3")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED BY 'org.apache.carbondata.format'")
    sql (s"""insert into t1 values ("aaaaa", 12, 20)""").show(200,false)

    checkAnswer(sql(s"""select count(*) from sdkOutputTable where age = 1"""),
      Seq(Row(1)))

    // scalastyle:off
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/nontransactional.csv'
         | INTO TABLE sdkOutputTable
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    checkAnswer(sql(s"""select count(*) from sdkOutputTable where height = 6.2"""),
      Seq(Row(1)))

    sql("DROP TABLE if exists sdkOutputTable")
    sql("drop table if exists t1")

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("read non transactional table, files written from sdk Writer Output)") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable1")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable1 STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable1"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    checkAnswer(sql("select name from sdkOutputTable1"), Seq(Row("robot0"),
      Row("robot1"),
      Row("robot2")))

    checkAnswer(sql("select age from sdkOutputTable1"), Seq(Row(0), Row(1), Row(2)))

    checkAnswer(sql("select * from sdkOutputTable1 where age > 1 and age < 8"),
      Seq(Row("robot2", 2, 1.0)))

    checkAnswer(sql("select * from sdkOutputTable1 where name = 'robot2'"),
      Seq(Row("robot2", 2, 1.0)))

    checkAnswer(sql("select * from sdkOutputTable1 where name like '%obot%' limit 2"),
      Seq(Row("robot0", 0, 0.0),
        Row("robot1", 1, 0.5)))

    checkAnswer(sql("select sum(age) from sdkOutputTable1 where name like 'robot%'"), Seq(Row(3)))

    checkAnswer(sql("select count(*) from sdkOutputTable1 where name like 'robot%' "), Seq(Row(3)))

    checkAnswer(sql("select count(*) from sdkOutputTable1"), Seq(Row(3)))

    sql("DROP TABLE sdkOutputTable1")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Test Blocked operations for non transactional table ") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    //1. alter datatype
    var exception = intercept[MalformedCarbonCommandException] {
      sql("Alter table sdkOutputTable change age age BIGINT")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //2. Datamap creation
    exception = intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE DATAMAP agg_sdkOutputTable ON TABLE sdkOutputTable USING \"preaggregate\" AS " +
        "SELECT name, sum(age) FROM sdkOutputTable GROUP BY name,age")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //3. compaction
    exception = intercept[MalformedCarbonCommandException] {
      sql("ALTER TABLE sdkOutputTable COMPACT 'MAJOR'")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //4. Show segments
    exception = intercept[MalformedCarbonCommandException] {
      sql("Show segments for table sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //5. Delete segment by ID
    exception = intercept[MalformedCarbonCommandException] {
      sql("DELETE FROM TABLE sdkOutputTable WHERE SEGMENT.ID IN (0)")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //6. Delete segment by date
    exception = intercept[MalformedCarbonCommandException] {
      sql("DELETE FROM TABLE sdkOutputTable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //7. Update Segment
    exception = intercept[MalformedCarbonCommandException] {
      sql("UPDATE sdkOutputTable SET (age) = (age + 9) ").show(false)
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //8. Delete Segment
    exception = intercept[MalformedCarbonCommandException] {
      sql("DELETE FROM sdkOutputTable where name='robot1'").show(false)
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //9. Show partition
    exception = intercept[MalformedCarbonCommandException] {
      sql("Show partitions sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //12. Streaming table creation
    // No need as External table don't accept table properties

    //13. Alter table rename command
    exception = intercept[MalformedCarbonCommandException] {
      sql("ALTER TABLE sdkOutputTable RENAME to newTable")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))

    //14. Block clean files
    exception = intercept[MalformedCarbonCommandException] {
      sql("clean files for table sdkOutputTable")
    }
    assert(exception.getMessage()
      .contains("Unsupported operation on non transactional table"))


    sql("DROP TABLE sdkOutputTable")
    //drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create External Table With Schema, should ignore the schema provided") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with schema
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file without Carbondata file should fail") {
    buildTestDataSingleFile()
    deleteFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //    data source file format
      sql(
        s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
           |'$writerPath' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("Operation not allowed: Invalid table path provided:"))

    cleanTestData()
  }


  test("Read sdk writer output file without any file should fail") {
    buildTestDataSingleFile()
    deleteFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    deleteFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //data source file format
      sql(
        s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
           |'$writerPath' """.stripMargin)

    }
    assert(exception.getMessage()
      .contains("Operation not allowed: Invalid table path provided:"))

    cleanTestData()
  }

  test("Read sdk writer output multiple files ") {
    buildTestDataMultipleFiles()
    assert(new File(writerPath).exists())
    val folder = new File(writerPath)
    val dataFiles = folder.listFiles(new FileFilter() {
      override def accept(pathname: File): Boolean = {
        pathname.getName
          .endsWith(CarbonCommonConstants.FACT_FILE_EXT)
      }
    })
    Assert.assertNotNull(dataFiles)
    Assert.assertNotEquals(1, dataFiles.length)

    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql("DROP TABLE IF EXISTS t1")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select count(*) from sdkOutputTable"), Seq(Row(1000000)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read two sdk writer outputs with same column name placed in same folder") {
    buildTestDataSingleFile()

    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    buildTestDataSameDirectory()

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    buildTestDataWithSameUUID(3, false, null, List("name"))

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    buildTestDataWithSameUUID(3, false, null, List("name"))

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    //test filter query
    checkAnswer(sql("select * from sdkOutputTable where age = 1"), Seq(
      Row("robot1", 1, 0.5),
      Row("robot1", 1, 0.5),
      Row("robot1", 1, 0.5),
      Row("robot1", 1, 0.5)))

    // test the default sort column behavior in Nontransactional table
    checkExistence(sql("describe formatted sdkOutputTable"), true,
      "SORT_COLUMNS                        name")

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test(
    "Read two sdk writer outputs before and after deleting the existing files and creating new " +
    "files with same schema") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(4, false, null)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot3", 3, 1.5)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test bad records form sdk writer") {

    //1. Action = FORCE
    buildTestDataWithBadRecordForce()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", null, null),
      Row("robot1", null, null),
      Row("robot2", null, null),
      Row("robot3", 3, 1.5)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())


    //2. Action = REDIRECT
    buildTestDataWithBadRecordRedirect()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot3", 3, 1.5)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())

    //3. Action = IGNORE
    buildTestDataWithBadRecordIgnore()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot3", 3, 1.5)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())

    cleanTestData()
  }

  test("test custom  format for date and timestamp in sdk") {

    cleanTestData()
    var options = Map("dateformat" -> "dd-MM-yyyy" ,"timestampformat" -> "dd-MM-yyyy HH:mm:ss").asJava

    val fields: Array[Field] = new Array[Field](4)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    fields(2) = new Field("mydate", DataTypes.DATE)
    fields(3) = new Field("mytime", DataTypes.TIMESTAMP)

    val builder: CarbonWriterBuilder = CarbonWriter.builder.withSchema(new Schema(fields))
      .outputPath(writerPath).isTransactionalTable(false).withLoadOptions(options)

    val writer: CarbonWriter = builder.buildWriterForCSVInput
    writer.write(Array("babu","1","02-01-2002","02-01-2002 01:01:00"));
    writer.close()

    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("babu", 1, java.sql.Date.valueOf("2002-01-02"),Timestamp.valueOf("2002-01-02 01:01:00.0"))))
    sql("DROP TABLE sdkOutputTable")
    cleanTestData()

  }

  test("test huge data write with one batch having bad record") {

    val exception =
      intercept[RuntimeException] {
        buildTestDataWithBadRecordFail()
      }
    assert(exception.getMessage()
      .contains("Data load failed due to bad record"))

  }

  test("Read sdk two writer output with same column name but different sort columns") {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestDataOtherDataType(3, Array[String]("name"))
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(true, 0, 0.0),
      Row(true, 1, 0.5),
      Row(true, 2, 1.0)))


    buildTestDataOtherDataType(3, Array[String]("age"))
    // put other sdk writer output to same path,
    // which has same column names but different sort column
    val exception =
    intercept[IOException] {
      sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("All the files doesn't have same schema"))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("Read sdk two writer output with same column name but different data type ") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    // put other sdk writer output to same path,
    // which has same column names but different data type
    buildTestDataOtherDataType(3, null)

    val exception =
      intercept[IOException] {
        sql("select * from sdkOutputTable").show(false)
      }
    assert(exception.getMessage()
      .contains("All the files doesn't have same schema"))


    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  private def WriteFilesWithAvroWriter(rows: Int,
      mySchema: String,
      json: String,
      fields: Array[Field]) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json.getBytes(CharEncoding.UTF_8), nn)

    try {
      val writer = CarbonWriter.builder.withSchema(new Schema(fields))
        .outputPath(writerPath).isTransactionalTable(false)
        .uniqueIdentifier(System.currentTimeMillis()).buildWriterForAvroInput
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  // struct type test
  def buildAvroTestDataStruct(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    val mySchema =
      """
        |{"name": "address",
        | "type": "record",
        | "fields": [
        |  { "name": "name", "type": "string"},
        |  { "name": "age", "type": "int"},
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "street", "type": "string"},
        |    {"name": "city", "type": "string"}]}}
        |]}
      """.stripMargin

    val json = """ {"name":"bob", "age":10, "address" : {"street":"abc", "city":"bang"}} """

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))
    fields(2) = new Field("address", "struct", fld)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataStructType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStruct(3, null)
  }

  // array type test
  def buildAvroTestDataArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |      "name": "address",
                     |      "type": "record",
                     |      "fields": [
                     |      {
                     |      "name": "name",
                     |      "type": "string"
                     |      },
                     |      {
                     |      "name": "age",
                     |      "type": "int"
                     |      },
                     |      {
                     |      "name": "address",
                     |      "type": {
                     |      "type": "array",
                     |      "items": {
                     |      "name": "street",
                     |      "type": "string"
                     |      }
                     |      }
                     |      }
                     |      ]
                     |  }
                   """.stripMargin

    val json: String = """ {"name": "bob","age": 10,"address": ["abc", "defg"]} """


    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    // fields[1] = new Field("age", DataTypes.INT);
    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fields(2) = new Field("address", "array", fld)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataSingleFileArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayType(3, null)
  }

  // struct with array type test
  def buildAvroTestDataStructWithArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """
                     {
                     |     "name": "address",
                     |     "type": "record",
                     |     "fields": [
                     |     { "name": "name", "type": "string"},
                     |     { "name": "age", "type": "int"},
                     |     {
                     |     "name": "address",
                     |     "type": {
                     |     "type" : "record",
                     |     "name" : "my_address",
                     |     "fields" : [
                     |     {"name": "street", "type": "string"},
                     |     {"name": "city", "type": "string"}
                     |     ]}
                     |     },
                     |     {"name" :"doorNum",
                     |     "type" : {
                     |     "type" :"array",
                     |     "items":{
                     |     "name" :"EachdoorNums",
                     |     "type" : "int",
                     |     "default":-1
                     |     }}
                     |     }]}
                     """.stripMargin

    val json =
      """ {"name":"bob", "age":10,
        |"address" : {"street":"abc", "city":"bang"},
        |"doorNum" : [1,2,3,4]}""".stripMargin

    val fields = new Array[Field](4)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))
    fields(2) = new Field("address", "struct", fld)
    val fld1 = new util.ArrayList[StructField]
    fld1.add(new StructField("eachDoorNum", DataTypes.INT))
    fields(3) = new Field("doorNum", "array", fld1)
    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataBothStructArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStructWithArrayType(3, null)
  }


  // ArrayOfStruct test
  def buildAvroTestDataArrayOfStruct(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "name",
                     |			"type": "string"
                     |		},
                     |		{
                     |			"name": "age",
                     |			"type": "int"
                     |		},
                     |		{
                     |			"name": "doorNum",
                     |			"type": {
                     |				"type": "array",
                     |				"items": {
                     |					"type": "record",
                     |					"name": "my_address",
                     |					"fields": [
                     |						{
                     |							"name": "street",
                     |							"type": "string"
                     |						},
                     |						{
                     |							"name": "city",
                     |							"type": "string"
                     |						}
                     |					]
                     |				}
                     |			}
                     |		}
                     |	]
                     |} """.stripMargin
    val json =
      """ {"name":"bob","age":10,"doorNum" :
        |[{"street":"abc","city":"city1"},
        |{"street":"def","city":"city2"},
        |{"street":"ghi","city":"city3"},
        |{"street":"jkl","city":"city4"}]} """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))

    val fld2 = new util.ArrayList[StructField]
    fld2.add(new StructField("my_address", DataTypes.createStructType(fld), fld))
    fields(2) = new Field("doorNum", DataTypes.createArrayType(fld2.get(0).getDataType), fld2)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataArrayOfStructType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayOfStruct(3, null)
  }


  // StructOfArray test
  def buildAvroTestDataStructOfArray(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "name",
                     |			"type": "string"
                     |		},
                     |		{
                     |			"name": "age",
                     |			"type": "int"
                     |		},
                     |		{
                     |			"name": "address",
                     |			"type": {
                     |				"type": "record",
                     |				"name": "my_address",
                     |				"fields": [
                     |					{
                     |						"name": "street",
                     |						"type": "string"
                     |					},
                     |					{
                     |						"name": "city",
                     |						"type": "string"
                     |					},
                     |					{
                     |						"name": "doorNum",
                     |						"type": {
                     |							"type": "array",
                     |							"items": {
                     |								"name": "EachdoorNums",
                     |								"type": "int",
                     |								"default": -1
                     |							}
                     |						}
                     |					}
                     |				]
                     |			}
                     |		}
                     |	]
                     |} """.stripMargin

    val json = """ {
                 |	"name": "bob",
                 |	"age": 10,
                 |	"address": {
                 |		"street": "abc",
                 |		"city": "bang",
                 |		"doorNum": [
                 |			1,
                 |			2,
                 |			3,
                 |			4
                 |		]
                 |	}
                 |} """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val fld2 = new util.ArrayList[StructField]
    fld2.add(new StructField("street", DataTypes.STRING))
    fld2.add(new StructField("city", DataTypes.STRING))

    val fld1 = new util.ArrayList[StructField]
    fld1.add(new StructField("eachDoorNum", DataTypes.INT))
    fld2.add(new StructField("doorNum", DataTypes.createArrayType(DataTypes.INT), fld1))

    fields(2) = new Field("address","struct",fld2)
    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataStructOfArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStructOfArray(3, null)
  }


  test("Read sdk writer Avro output Record Type") {
    buildAvroTestDataStructType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, Row("abc","bang")),
      Row("bob", 10, Row("abc","bang")),
      Row("bob", 10, Row("abc","bang"))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }

  test("Read sdk writer Avro output Array Type") {
    buildAvroTestDataSingleFileArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(200,false)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg")))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    cleanTestData()
  }

  test("Read sdk writer Avro output with both Array and Struct Type") {
    buildAvroTestDataBothStructArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    /*
    *-+----+---+----------+------------+
    |name|age|address   |doorNum     |
    +----+---+----------+------------+
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    +----+---+----------+------------+
    * */

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, Row("abc","bang"), mutable.WrappedArray.newBuilder[Int].+=(1,2,3,4)),
      Row("bob", 10, Row("abc","bang"), mutable.WrappedArray.newBuilder[Int].+=(1,2,3,4)),
      Row("bob", 10, Row("abc","bang"), mutable.WrappedArray.newBuilder[Int].+=(1,2,3,4))))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  test("Read sdk writer Avro output with Array of struct") {
    buildAvroTestDataArrayOfStructType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+----------------------------------------------------+
    |name|age|doorNum                                             |
    +----+---+----------------------------------------------------+
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    +----+---+----------------------------------------------------+ */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  // Struct of array
  test("Read sdk writer Avro output with struct of Array") {
    buildAvroTestDataStructOfArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+-------------------------------------------------------+
    |name|age|address                                                |
    +----+---+-------------------------------------------------------+
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    +----+---+-------------------------------------------------------+*/

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  // test multi level -- 3 levels [array of struct of array of int]
  def buildAvroTestDataMultiLevel3(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "name",
                     |			"type": "string"
                     |		},
                     |		{
                     |			"name": "age",
                     |			"type": "int"
                     |		},
                     |		{
                     |			"name": "doorNum",
                     |			"type": {
                     |				"type": "array",
                     |				"items": {
                     |					"type": "record",
                     |					"name": "my_address",
                     |					"fields": [
                     |						{
                     |							"name": "street",
                     |							"type": "string"
                     |						},
                     |						{
                     |							"name": "city",
                     |							"type": "string"
                     |						},
                     |						{
                     |							"name": "FloorNum",
                     |							"type": {
                     |								"type": "array",
                     |								"items": {
                     |									"name": "floor",
                     |									"type": "int"
                     |								}
                     |							}
                     |						}
                     |					]
                     |				}
                     |			}
                     |		}
                     |	]
                     |} """.stripMargin
    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"doorNum": [
        |		{
        |			"street": "abc",
        |			"city": "city1",
        |			"FloorNum": [0,1,2]
        |		},
        |		{
        |			"street": "def",
        |			"city": "city2",
        |			"FloorNum": [3,4,5]
        |		},
        |		{
        |			"street": "ghi",
        |			"city": "city3",
        |			"FloorNum": [6,7,8]
        |		},
        |		{
        |			"street": "jkl",
        |			"city": "city4",
        |			"FloorNum": [9,10,11]
        |		}
        |	]
        |} """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))
    fld.add(new StructField("FloorNum", DataTypes.createArrayType(DataTypes.INT)))

    val fld2 = new util.ArrayList[StructField]
    fld2.add(new StructField("my_address", DataTypes.createStructType(fld), fld))
    fields(2) = new Field("doorNum", DataTypes.createArrayType(fld2.get(0).getDataType), fld2)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataMultiLevel3Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3(3, null)
  }

  // test multi level -- 3 levels [array of struct of array of int]
  test("test multi level support : array of struct of array of int") {
    buildAvroTestDataMultiLevel3Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+-----------------------------------------------------------------------------------+
    |name|age|doorNum
                                                               |
    +----+---+-----------------------------------------------------------------------------------+
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    +----+---+-----------------------------------------------------------------------------------+*/

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  // test multi level -- 3 levels [array of struct of struct of string, int]
  def buildAvroTestDataMultiLevel3_1(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "name",
                     |			"type": "string"
                     |		},
                     |		{
                     |			"name": "age",
                     |			"type": "int"
                     |		},
                     |		{
                     |			"name": "doorNum",
                     |			"type": {
                     |				"type": "array",
                     |				"items": {
                     |					"type": "record",
                     |					"name": "my_address",
                     |					"fields": [
                     |						{
                     |							"name": "street",
                     |							"type": "string"
                     |						},
                     |						{
                     |							"name": "city",
                     |							"type": "string"
                     |						},
                     |						{
                     |							"name": "FloorNum",
                     |                			"type": {
                     |                				"type": "record",
                     |                				"name": "Floor",
                     |                				"fields": [
                     |                					{
                     |                						"name": "wing",
                     |                						"type": "string"
                     |                					},
                     |                					{
                     |                						"name": "number",
                     |                						"type": "int"
                     |                					}
                     |                				]
                     |                			}
                     |						}
                     |					]
                     |				}
                     |			}
                     |		}
                     |	]
                     |} """.stripMargin


    val json =
      """  {
        |	"name": "bob",
        |	"age": 10,
        |	"doorNum": [
        |		{
        |			"street": "abc",
        |			"city": "city1",
        |			"FloorNum": {"wing" : "a", "number" : 1}
        |		},
        |		{
        |			"street": "def",
        |			"city": "city2",
        |			"FloorNum": {"wing" : "b", "number" : 0}
        |		},
        |		{
        |			"street": "ghi",
        |			"city": "city3",
        |			"FloorNum": {"wing" : "a", "number" : 2}
        |		}
        |	]
        |}  """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))

    val subFld = new util.ArrayList[StructField]
    subFld.add(new StructField("wing", DataTypes.STRING))
    subFld.add(new StructField("number", DataTypes.INT))
    fld.add(new StructField("FloorNum", DataTypes.createStructType(subFld)))

    // array of struct of struct
    val fld2 = new util.ArrayList[StructField]
    fld2.add(new StructField("my_address", DataTypes.createStructType(fld), fld))
    fields(2) = new Field("doorNum", DataTypes.createArrayType(fld2.get(0).getDataType), fld2)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataMultiLevel3_1Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3_1(3, null)
  }

  // test multi level -- 3 levels [array of struct of struct of string, int]
  test("test multi level support : array of struct of struct") {
    buildAvroTestDataMultiLevel3_1Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+---------------------------------------------------------+
    |name|age|doorNum                                                  |
    +----+---+---------------------------------------------------------+
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    +----+---+---------------------------------------------------------+
    */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  // test multi level -- 3 levels [array of array of array of int]
  def buildAvroTestDataMultiLevel3_2(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "name",
                     |			"type": "string"
                     |		},
                     |		{
                     |			"name": "age",
                     |			"type": "int"
                     |		},
                     |		{
                     |			"name": "BuildNum",
                     |			"type": {
                     |				"type": "array",
                     |				"items": {
                     |					"name": "FloorNum",
                     |					"type": "array",
                     |					"items": {
                     |						"name": "doorNum",
                     |						"type": "array",
                     |						"items": {
                     |							"name": "EachdoorNums",
                     |							"type": "int",
                     |							"default": -1
                     |						}
                     |					}
                     |				}
                     |			}
                     |		}
                     |	]
                     |} """.stripMargin

    val json =
      """   {
        |        	"name": "bob",
        |        	"age": 10,
        |        	"BuildNum": [[[1,2,3],[4,5,6]],[[10,20,30],[40,50,60]]]
        |        }   """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val subFld = new util.ArrayList[StructField]
    subFld.add(new StructField("EachDoorNum", DataTypes.INT))

    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("DoorNum", DataTypes.createArrayType(DataTypes.INT), subFld))
    // array of struct of struct
    val doorNum = new util.ArrayList[StructField]
    doorNum.add(new StructField("FloorNum",
      DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.INT)), fld))
    fields(2) = new Field("BuildNum", "array", doorNum)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataMultiLevel3_2Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3_2(3, null)
  }

  // test multi level -- 3 levels [array of array of array of int]
  test("test multi level support : array of array of array of int") {
    buildAvroTestDataMultiLevel3_2Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+---------------------------------------------------------------------------+
    |name|age|BuildNum
                                               |
    +----+---+---------------------------------------------------------------------------+
    |bob |10 |[WrappedArray(WrappedArray(1, 2, 3), WrappedArray(4, 5, 6)), WrappedArray
    (WrappedArray(10, 20, 30), WrappedArray(40, 50, 60))]|
    |bob |10 |[WrappedArray(WrappedArray(1, 2, 3), WrappedArray(4, 5, 6)), WrappedArray
    (WrappedArray(10, 20, 30), WrappedArray(40, 50, 60))]|
    |bob |10 |[WrappedArray(WrappedArray(1, 2, 3), WrappedArray(4, 5, 6)), WrappedArray
    (WrappedArray(10, 20, 30), WrappedArray(40, 50, 60))]|
    +----+---+---------------------------------------------------------------------------+
   */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }



  // test multi level -- 4 levels [array of array of array of struct]
  def buildAvroTestDataMultiLevel4(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =  """ {
                      |	"name": "address",
                      |	"type": "record",
                      |	"fields": [
                      |		{
                      |			"name": "name",
                      |			"type": "string"
                      |		},
                      |		{
                      |			"name": "age",
                      |			"type": "int"
                      |		},
                      |		{
                      |			"name": "BuildNum",
                      |			"type": {
                      |				"type": "array",
                      |				"items": {
                      |					"name": "FloorNum",
                      |					"type": "array",
                      |					"items": {
                      |						"name": "doorNum",
                      |						"type": "array",
                      |						"items": {
                      |							"name": "my_address",
                      |							"type": "record",
                      |							"fields": [
                      |								{
                      |									"name": "street",
                      |									"type": "string"
                      |								},
                      |								{
                      |									"name": "city",
                      |									"type": "string"
                      |								}
                      |							]
                      |						}
                      |					}
                      |				}
                      |			}
                      |		}
                      |	]
                      |} """.stripMargin

    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"BuildNum": [
        |		[
        |			[
        |				{"street":"abc", "city":"city1"},
        |				{"street":"def", "city":"city2"},
        |				{"street":"cfg", "city":"city3"}
        |			],
        |			[
        |				 {"street":"abc1", "city":"city3"},
        |				 {"street":"def1", "city":"city4"},
        |				 {"street":"cfg1", "city":"city5"}
        |			]
        |		],
        |		[
        |			[
        |				 {"street":"abc2", "city":"cityx"},
        |				 {"street":"abc3", "city":"cityy"},
        |				 {"street":"abc4", "city":"cityz"}
        |			],
        |			[
        |				 {"street":"a1bc", "city":"cityA"},
        |				 {"street":"a1bc", "city":"cityB"},
        |				 {"street":"a1bc", "city":"cityc"}
        |			]
        |		]
        |	]
        |} """.stripMargin

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)

    val subFld = new util.ArrayList[StructField]
    subFld.add(new StructField("EachDoorNum", DataTypes.INT))

    val address = new util.ArrayList[StructField]
    address.add(new StructField("street", DataTypes.STRING))
    address.add(new StructField("city", DataTypes.STRING))

    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("DoorNum",
        DataTypes.createArrayType(DataTypes.createStructType(address)),
        subFld))
    // array of struct of struct
    val doorNum = new util.ArrayList[StructField]
    doorNum.add(new StructField("FloorNum",
      DataTypes.createArrayType(
        DataTypes.createArrayType(DataTypes.createStructType(address))), fld))
    fields(2) = new Field("BuildNum", "array", doorNum)

    WriteFilesWithAvroWriter(rows, mySchema, json, fields)
  }

  def buildAvroTestDataMultiLevel4Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel4(3, null)
  }

  // test multi level -- 4 levels [array of array of array of struct]
  test("test multi level support : array of array of array of struct") {
    buildAvroTestDataMultiLevel4Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  test(
    "test if exception is thrown when a column which is not in schema is specified in sort columns")
  {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "int"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101,"course_details": { "course_struct_course_time":"2014-01-05"  }}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json1.getBytes(CharEncoding.UTF_8), nn)

    val fields = new Array[Field](2)
    fields(0) = new Field("id", DataTypes.INT)
    val fld_s = new java.util.ArrayList[StructField]
    fld_s.add(new StructField("course_struct_course_time", DataTypes.STRING))
    fields(1) = new Field("course_details", "struct", fld_s)

    assert(intercept[RuntimeException] {
      val writer = CarbonWriter.builder.withSchema(new Schema(fields)).sortBy(Array("name", "id"))
        .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput
      writer.write(record)
      writer.close()
    }.getMessage.toLowerCase.contains("column: name specified in sort columns"))
  }

  test("test if load is passing with NULL type") {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "null"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101,"course_details": { "course_struct_course_time":"2014-01-05"  }}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json1.getBytes(CharEncoding.UTF_8), nn)

    val writer = CarbonWriter.builder.withSchema(AvroCarbonWriter.getCarbonSchemaFromAvroSchema(schema1))
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput
    writer.write(record)
    writer.close()
  }

  test("test if data load is success with a struct having timestamp column  ") {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "int"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101,"course_details": { "course_struct_course_time":"2014-01-05 00:00:00"  }}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json1.getBytes(CharEncoding.UTF_8), nn)

    val fields = new Array[Field](2)
    fields(0) = new Field("id", DataTypes.INT)
    val fld_s = new java.util.ArrayList[StructField]
    fld_s.add(new StructField("course_struct_course_time", DataTypes.TIMESTAMP))
    fields(1) = new Field("course_details", "struct", fld_s)

    val writer = CarbonWriter.builder.withSchema(new Schema(fields)).sortBy(Array("id"))
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput
    writer.write(record)
    writer.close()
  }

  test(
    "test is dataload is successful if childcolumn has same name as one of the other fields(not " +
    "complex)")
  {
    val schema =
      """{
        |	"type": "record",
        |	"name": "Order",
        |	"namespace": "com.apache.schema",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "long"
        |		},
        |		{
        |			"name": "entries",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "Entry",
        |					"fields": [
        |						{
        |							"name": "id",
        |							"type": "long"
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |}""".stripMargin
    val json1 =
      """{"id": 101, "entries": [ {"id":1234}, {"id":3212}  ]}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json1.getBytes(CharEncoding.UTF_8), nn)

    val fields = new Array[Field](2)
    fields(0) = new Field("id", DataTypes.LONG)
    val fld_s = new java.util.ArrayList[StructField]
    fld_s.add(new StructField("id", DataTypes.LONG))
    fields(1) = new Field("entries", DataTypes.createArrayType(DataTypes.createStructType(fld_s)))
    val writer = CarbonWriter.builder.withSchema(new Schema(fields))
      .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput
    writer.write(record)
    writer.close()
  }

  test("test if data load with various bad_records_action") {
    val schema =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "string"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_string",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		},
        |		{
        |			"name": "salary_string",
        |			"type": {
        |				"type": "array",
        |				"items": "string"
        |			}
        |		}
        |	]
        |}""".stripMargin
    val json1 =
      """{
        |	"id": "cust_1",
        |	"course_details": {
        |		"course_struct_course_string": "asd"
        |	},
        |	"salary_string": [
        |		"xyz",
        |		"abc"
        |	]
        |}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json1.getBytes(CharEncoding.UTF_8), nn)

    val fields = new Array[Field](3)
    fields(0)=new Field("id", DataTypes.STRING)
    val fld_s = new java.util.ArrayList[StructField]
    fld_s.add(new StructField("carbon_int", DataTypes.INT))
    fields(1)=new Field("course_details", "struct",fld_s)

    val fld_a = new java.util.ArrayList[StructField]
    fld_a.add(new StructField("carbon_array", DataTypes.INT))
    fields(2)=new Field("salary_string", "array",fld_a)

    val loadOptions = new util.HashMap[String, String]()
    loadOptions.put("bad_records_action", "fail")
    assert(intercept[Exception] {
      val writer = CarbonWriter.builder.withSchema(new Schema(fields)).outputPath(writerPath)
        .isTransactionalTable(false).withLoadOptions(loadOptions).buildWriterForAvroInput
      writer.write(record)
      writer.close()
    }.getMessage.contains("Data load failed due to bad record"))

    loadOptions.put("bad_records_action", "FORCE")
      val writer = CarbonWriter.builder.withSchema(new Schema(fields)).outputPath(writerPath)
        .isTransactionalTable(false).withLoadOptions(loadOptions).buildWriterForAvroInput
      writer.write(record)
      writer.close()
  }

}
