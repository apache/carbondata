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

import java.io._
import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.TableBlockInfo
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.DimensionChunkReaderV3
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.reader.CarbonFooterReaderV3
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.{CarbonMetadataUtil, CarbonProperties, CarbonUtil, DataFileFooterConverterV3}
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.sdk.file._


class TestNonTransactionalCarbonTable extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            + "../../target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")

  def buildTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null)
  }

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(1000000, null)
  }

  def buildTestDataTwice(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null)
    buildTestData(3, null)
  }

  def buildTestDataSameDirectory(): Any = {
    buildTestData(3, null)
  }

  def buildTestDataWithBadRecordForce(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FORCE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordFail(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FAIL").asJava
    buildTestData(15001, options)
  }

  def buildTestDataWithBadRecordIgnore(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "IGNORE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordRedirect(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "REDIRECT").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithSortColumns(sortColumns: List[String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null, sortColumns)
  }

  def buildTestData(rows: Int, options: util.Map[String, String]): Any = {
    buildTestData(rows, options, List("name"))
  }

  def buildTestDataWithOptionsAndEmptySortColumn(rows: Int,
      options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(rows, options, List())
  }


  // prepare sdk writer output
  def buildTestData(rows: Int,
      options: util.Map[String, String],
      sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"NaMe\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        if (options != null) {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2).withLoadOptions(options)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
        } else {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
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
      if ((options != null) && sortColumns.nonEmpty) {
        //Keep one valid record. else carbon data file will not generate
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
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
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.currentTimeMillis()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("true", String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  // prepare sdk writer output
  def buildTestDataWithSameUUID(rows: Int,
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
        builder.outputPath(writerPath)
          .sortBy(sortColumns.toArray)
          .uniqueIdentifier(
            123).withBlockSize(2)
          .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
  }

  def deleteFile(path: String, extension: String): Unit = {
    val file: CarbonFile = FileFactory.getCarbonFile(path)

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
      s"""
         | CREATE EXTERNAL TABLE sdkOutputTable
         | STORED AS carbondata
         | LOCATION '$writerPath'
      """.stripMargin)

    sql("drop table if exists t1")
    sql("create table if not exists t1 (name string, age int, height double) STORED AS carbondata")
    var i =0
    while (i<50){
      sql (s"""insert into t1 values ("aaaaa", 12, 20)""").show(200,false)
      i = i+1
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
    buildTestDataWithSameUUID(3, null, List("name"))
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("SELECT name,name FROM sdkOutputTable"), Seq(
      Row("robot0", "robot0"),
      Row("robot1", "robot1"),
      Row("robot2", "robot2")))
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))
    FileUtils.deleteDirectory(new File(writerPath))
    // Thread.sleep is required because it is possible sometime deletion
    // and creation of new file can happen at same timestamp.
    Thread.sleep(1000)
    assert(!new File(writerPath).exists())
    buildTestDataWithSameUUID(4, null, List("name"))
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

  test(" test csv fileheader for transactional table") {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestDataWithSameUUID(3, null, List("Name"))
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("SELECT name,name FROM sdkOutputTable"), Seq(
      Row("robot0", "robot0"),
      Row("robot1", "robot1"),
      Row("robot2", "robot2")))
    //load csvfile without fileheader
    var exception = intercept[CarbonDataLoadingException] {
      sql(s"""load data inpath '$resourcesPath/nontransactional1.csv' into table sdkOutputTable""").show(200,false)
    }
    assert(exception.getMessage()
      .contains("CSV header in input file is not proper. Column names in schema and csv header are not the same."))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("test count star with multiple loads files with same schema and UUID") {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestDataWithSameUUID(3, null, List("namE"))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql(s"""select count(*) from sdkOutputTable """), Seq(Row(3)))
    buildTestDataWithSameUUID(3, null, List("name"))
    // should reflect new count
    checkAnswer(sql(s"""select count(*) from sdkOutputTable """), Seq(Row(6)))
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, writerPath)

    buildTestDataWithSortColumns(List("age"))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "Sort Columns age")
    checkExistence(sql("describe formatted sdkOutputTable"), false, "Sort Columns name, age")
    checkExistence(sql("describe formatted sdkOutputTable"), false, "Sort Columns age, name")
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "Sort Columns name")

    buildTestDataWithSortColumns(List())
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"),false,"Sort Columns name")

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()

    intercept[RuntimeException] {
      buildTestDataWithSortColumns(List(""))
    }

    assert(!(new File(writerPath).exists()))
    cleanTestData()
  }

  test("test create external table with all the records as bad record with redirect") {
    var options = Map("bAd_RECords_action" -> "REDIRECT").asJava
    buildTestDataWithOptionsAndEmptySortColumn(3, options)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // when one row is bad record and it it redirected.
    // Empty carbon files must not create in no_sort flow
    var exception = intercept[AnalysisException] {
      sql(
        s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
           | LOCATION
           |'$writerPath' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("Unable to infer the schema"))
    cleanTestData()
  }

  test("test create External Table with Schema with partition, should ignore schema and partition") {
    buildTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED AS carbondata")
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED AS carbondata")
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    sql("create table if not exists t1 (name string, age int, height double) STORED AS carbondata")
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable1 STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
    val ex = intercept[MalformedCarbonCommandException] {
      sql("Show partitions sdkOutputTable").show(false)
    }
    assert(ex.getMessage()
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

    val path1 = writerPath + "/0testdir"
    val path2 = writerPath + "/testdir"

    FileFactory.getCarbonFile(path1)
    FileFactory.mkdirs(path1)

    FileFactory.getCarbonFile(path2)
    FileFactory.mkdirs(path2)

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with schema
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
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
        s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
           |'$writerPath' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("Unable to infer the schema"))

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
        s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
           |'$writerPath' """.stripMargin)

    }
    assert(exception.getMessage()
      .contains("Unable to infer the schema"))

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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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

    buildTestDataWithSameUUID(3, null, List("name"))

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

    buildTestDataWithSameUUID(3, null, List("name"))

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
      "Sort Columns name")

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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(4, null)

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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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

    val builder: CarbonWriterBuilder = CarbonWriter.builder.outputPath(writerPath)
      .withLoadOptions(options)

    val writer: CarbonWriter = builder.withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(Array("babu","1","02-01-2002","02-01-2002 01:01:00"))
    writer.close()

    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(true, 0, 0.0),
      Row(true, 1, 0.5),
      Row(true, 2, 1.0)))


    buildTestDataOtherDataType(3, Array[String]("age"))
    // put other sdk writer output to same path,
    // which has same column names but different sort column
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(true, 0, 0.0),
          Row(true, 1, 0.5),
          Row(true, 2, 1.0),
          Row(true, 0, 0.0),
          Row(true, 1, 0.5),
          Row(true, 2, 1.0)))


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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      .contains("Problem in loading segment blocks"))


    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test SDK Read with merge index file") {
    sql("DROP TABLE IF EXISTS normalTable1")
    sql(
      "create table if not exists normalTable1(name string, age int, height double) " +
      "STORED AS carbondata")
    sql(s"""insert into normalTable1 values ("aaaaa", 12, 20)""").show(200, false)
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    val carbonTable = CarbonEnv
      .getCarbonTable(Option("default"), "normalTable1")(sqlContext.sparkSession)
    sql("describe formatted normalTable1").show(200, false)
    val fileLocation = carbonTable.getSegmentPath("0")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$fileLocation' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("aaaaa", 12, 20)))
    sql("DROP TABLE sdkOutputTable")
    sql("DROP TABLE normalTable1")
  }

  // --------------------------------------------- AVRO test cases ---------------------------
  def WriteFilesWithAvroWriter(rows: Int,
      mySchema: String,
      json: String) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
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
        |  { "name": "age", "type": "float"},
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "street", "type": "string"},
        |    {"name": "city", "type": "string"}]}}
        |]}
      """.stripMargin

    val json = """ {"name":"bob", "age":10.24, "address" : {"street":"abc", "city":"bang"}} """

    WriteFilesWithAvroWriter(rows, mySchema, json)
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataStructOfArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStructOfArray(3, null)
  }

  // ArrayOfStruct test
  def buildAvroTestDataArrayOfStructWithNoSortCol(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = """ {
                     |	"name": "address",
                     |	"type": "record",
                     |	"fields": [
                     |		{
                     |			"name": "exp",
                     |			"type": "int"
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
      """ {"exp":5,"age":10,"doorNum" :
        |[{"street":"abc","city":"city1"},
        |{"street":"def","city":"city2"},
        |{"street":"ghi","city":"city3"},
        |{"street":"jkl","city":"city4"}]} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  test("Read sdk writer Avro output Record Type with no sort columns") {
    buildAvroTestDataArrayOfStructWithNoSortCol(3,null)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("desc formatted sdkOutputTable").show(false)
    sql("select * from sdkOutputTable").show(false)

    /*
    +---+---+----------------------------------------------------+
    |exp|age|doorNum                                             |
    +---+---+----------------------------------------------------+
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    +---+---+----------------------------------------------------+
    */
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }

  test("Read sdk writer Avro output Record Type") {
    buildAvroTestDataStructType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(s"CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION '$writerPath'")
    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10.24f, Row("abc","bang")),
      Row("bob", 10.24f, Row("abc","bang")),
      Row("bob", 10.24f, Row("abc","bang"))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }

  test("Read sdk writer Avro output Array Type") {
    buildAvroTestDataSingleFileArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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

  // array type Default value test
  def buildAvroTestDataArrayDefaultType(rows: Int, options: util.Map[String, String]): Any = {
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
                     |      "items": "string"
                     |      },
                     |      "default": ["sc","ab"]
                     |      }
                     |      ]
                     |  }
                   """.stripMargin

    // skip giving array value to take default values
    val json: String = "{\"name\": \"bob\",\"age\": 10}"

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataSingleFileArrayDefaultType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayDefaultType(3, null)
  }

  test("Read sdk writer Avro output Array Type with Default value") {
    // avro1.8.x Parser donot handles default value , this willbe fixed in 1.9.x. So for now this
    // will throw exception. After upgradation of Avro we can change this test case.
    val exception = intercept[RuntimeException] {
      buildAvroTestDataSingleFileArrayDefaultType()
    }
    assert(exception.getMessage.contains("Expected array-start. Got END_OBJECT"))
    /*assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(200,false)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab")))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    cleanTestData()*/
  }


  test("Read sdk writer Avro output with both Array and Struct Type") {
    buildAvroTestDataBothStructArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("SELECT name,name FROM sdkOutputTable").show()
    checkAnswer(sql("SELECT name,name FROM sdkOutputTable"), Seq(
      Row("bob", "bob"),
      Row("bob", "bob"),
      Row("bob", "bob")))

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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
                     |              "logicalType": "date",
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataMultiLevel3_2Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3_2(3, null)
  }

  // test multi level -- 3 levels [array of array of array of int with logical type]
  test("test multi level support : array of array of array of int with logical type") {
    buildAvroTestDataMultiLevel3_2Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable limit 1").show(false)

    // TODO: Add a validation
    /*
    +----+---+------------------------------------------------------------------+
    |name|age|BuildNum                                                          |
    +----+---+------------------------------------------------------------------+
    |bob |10 |[WrappedArray(WrappedArray(1970-01-02, 1970-01-03, 1970-01-04),   |
    |                    WrappedArray(1970-01-05, 1970-01-06, 1970-01-07)),     |
    |       WrappedArray(WrappedArray(1970-01-11, 1970-01-21, 1970-01-31),      |
    |                    WrappedArray(1970-02-10, 1970-02-20, 1970-03-02))]     |
    +----+---+------------------------------------------------------------------+
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

    WriteFilesWithAvroWriter(rows, mySchema, json)
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
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
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
    val record = testUtil.jsonToAvro(json1, schema1)

    assert(intercept[RuntimeException] {
      val writer = CarbonWriter.builder.sortBy(Array("name", "id"))
        .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
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
      """{"id": null,"course_details": { "course_struct_course_time":"2014-01-05"  }}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
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
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder.sortBy(Array("id"))
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
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
    val record = testUtil.jsonToAvro(json1, schema)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
  }

  test("test logical type date") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "int", "logicalType": "date"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "int", "logicalType": "date"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101, "course_details": { "course_struct_course_time":10}}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION '$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(java.sql.Date.valueOf("1970-04-12"), Row(java.sql.Date.valueOf("1970-01-11")))))
  }

  test("test logical type timestamp-millis") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "long", "logicalType": "timestamp-millis"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "long", "logicalType": "timestamp-millis"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 172800000,"course_details": { "course_struct_course_time":172800000}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Timestamp.valueOf("1970-01-02 16:00:00"), Row(Timestamp.valueOf("1970-01-02 16:00:00")))))
  }

  test("test logical type-micros timestamp") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "long", "logicalType": "timestamp-micros"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "long", "logicalType": "timestamp-micros"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 172800000000,"course_details": { "course_struct_course_time":172800000000}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)


    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata
         | LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Timestamp.valueOf("1970-01-02 16:00:00"), Row(Timestamp.valueOf("1970-01-02 16:00:00")))))
  }

  test("test Sort Scope with SDK") {
    cleanTestData()
    // test with no_sort
    val options = Map("sort_scope" -> "no_sort").asJava
    val fields: Array[Field] = new Array[Field](4)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    val writer: CarbonWriter = CarbonWriter.builder
      .outputPath(writerPath)
      .withTableProperties(options)
      .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(Array("carbon", "1"))
    writer.write(Array("hydrogen", "10"))
    writer.write(Array("boron", "4"))
    writer.write(Array("zirconium", "5"))
    writer.close()

    // read no sort data
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION '$writerPath' """
        .stripMargin)
    checkAnswer(sql("select * from sdkTable"),
      Seq(Row("carbon", 1), Row("hydrogen", 10), Row("boron", 4), Row("zirconium", 5)))

    // write local sort data
    val writer1: CarbonWriter = CarbonWriter.builder
      .outputPath(writerPath)
      .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
    writer1.write(Array("carbon", "1"))
    writer1.write(Array("hydrogen", "10"))
    writer1.write(Array("boron", "4"))
    writer1.write(Array("zirconium", "5"))
    writer1.close()
    // read both-no sort and local sort data
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(8)))
    sql("DROP TABLE sdkTable")
    cleanTestData()
  }

  test("test LocalDictionary with True") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    sql("describe formatted sdkTable").show(100, false)
    val descLoc = sql("describe formatted sdkTable").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test LocalDictionary with custom Threshold") {
    FileUtils.deleteDirectory(new File(writerPath))
    val tablePropertiesMap: util.Map[String, String] =
      Map("table_blocksize" -> "12",
        "sort_columns" -> "name",
        "local_dictionary_threshold" -> "200",
        "local_dictionary_enable" -> "true",
        "inverted_index" -> "name").asJava
    val builder = CarbonWriter.builder
      .withTableProperties(tablePropertiesMap)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test Local Dictionary with FallBack") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .localDictionaryThreshold(5)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(!testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test local dictionary with External Table data load ") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .localDictionaryThreshold(200)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    FileUtils.deleteDirectory(new File(writerPath))
    sql("insert into sdkTable select 's1','s2',23 ")
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(1)))
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // Inverted index display is based on sort_scope, now by default sort_scope is no_sort.
  // Hence inverted index will not be displayed for external table
  // as we don't support table-properties inferring
  ignore("test inverted index column by API") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath)
      .invertedIndexFor(Array[String]("name")).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    FileUtils.deleteDirectory(new File(writerPath))
    sql("insert into sdkTable select 's1','s2',23 ")
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Inverted Index Columns name")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(1)))
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test Local Dictionary with Default") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0,writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    val descLoc = sql("describe formatted sdkTable").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("Test with long string columns with 1 MB pageSize") {
    FileUtils.deleteDirectory(new File(writerPath))
    // here we specify the long string column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"  address    \":\"varchar\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"note\":\"varchar\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema))
      .withPageSizeInMb(1)
      .writtenBy("TestCreateTableUsingSparkCarbonFileFormat").build()
    val totalRecordsNum = 10
    for (i <- 0 until totalRecordsNum) {
      // write a varchar with 250,000 length
      writer
        .write(Array[String](s"name_$i",
          RandomStringUtils.randomAlphabetic(250000),
          i.toString,
          RandomStringUtils.randomAlphabetic(250000)))
    }
    writer.close()

    // read footer and verify number of pages
    val folder = FileFactory.getCarbonFile(writerPath)
    val files = folder.listFiles(true)
    import scala.collection.JavaConverters._
    val dataFiles = files.asScala.filter(_.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT))
    dataFiles.foreach { dataFile =>
      val fileReader = FileFactory
        .getFileHolder(FileFactory.getFileType(dataFile.getPath))
      val buffer = fileReader
        .readByteBuffer(FileFactory.getUpdatedFilePath(dataFile.getPath), dataFile.getSize - 8, 8)
      val footerReader = new CarbonFooterReaderV3(dataFile.getAbsolutePath, buffer.getLong)
      val footer = footerReader.readFooterVersion3
      // without page_size configuration there will be only 1 page, now it will be more.
      assert(footer.getBlocklet_info_list3.get(0).number_number_of_pages != 1)
    }

  }

  def generateCarbonData(builder :CarbonWriterBuilder): Unit ={
    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("surname", DataTypes.STRING)
    fields(2) = new Field("age", DataTypes.INT)
    val carbonWriter = builder.withCsvInput(new Schema(fields)).build()
    var i = 0
    while (i < 100) {
      {
        carbonWriter
          .write(Array[String]("robot" + (i % 10), "robot_surname" + (i % 10), String.valueOf(i)))
      }
      { i += 1; i - 1 }
    }
    carbonWriter.close()
  }
}


object testUtil{

  def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
    var input: InputStream = null
    var writer: DataFileWriter[GenericRecord] = null
    var encoder: Encoder = null
    var output: ByteArrayOutputStream = null
    try {
      val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      input = new ByteArrayInputStream(json.getBytes())
      output = new ByteArrayOutputStream()
      val din = new DataInputStream(input)
      writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
      writer.create(schema, output)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      var datum: GenericRecord = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }

  /**
   * this method returns true if local dictionary is created for all the blocklets or not
   *
   * @return
   */
  def getDimRawChunk(blockindex: Int,storePath :String): util.ArrayList[DimensionRawColumnChunk] = {
    val dataFiles = FileFactory.getCarbonFile(storePath)
      .listFiles(new CarbonFileFilter() {
        override def accept(file: CarbonFile): Boolean = {
          if (file.getName
            .endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
            true
          } else {
            false
          }
        }
      })
    val dimensionRawColumnChunks = read(dataFiles(0).getAbsolutePath,
      blockindex)
    dimensionRawColumnChunks
  }

  def read(filePath: String, blockIndex: Int) = {
    val carbonDataFiles = new File(filePath)
    val dimensionRawColumnChunks = new
        util.ArrayList[DimensionRawColumnChunk]
    val offset = carbonDataFiles.length
    val converter = new DataFileFooterConverterV3
    val fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
    val actualOffset = fileReader.readLong(carbonDataFiles.getAbsolutePath, offset - 8)
    val blockInfo = new TableBlockInfo(carbonDataFiles.getAbsolutePath,
      actualOffset,
      "0",
      new Array[String](0),
      carbonDataFiles.length,
      ColumnarFormatVersion.V3,
      null)
    val dataFileFooter = converter.readDataFileFooter(blockInfo)
    val blockletList = dataFileFooter.getBlockletList.asScala
    for (blockletInfo <- blockletList) {
      val dimensionColumnChunkReader =
        CarbonDataReaderFactory
          .getInstance
          .getDimensionColumnChunkReader(ColumnarFormatVersion.V3,
            blockletInfo,
            carbonDataFiles.getAbsolutePath,
            false).asInstanceOf[DimensionChunkReaderV3]
      dimensionRawColumnChunks
        .add(dimensionColumnChunkReader.readRawDimensionChunk(fileReader, blockIndex))
    }
    dimensionRawColumnChunks
  }

  def validateDictionary(rawColumnPage: DimensionRawColumnChunk,
      data: Array[String]): Boolean = {
    val local_dictionary = rawColumnPage.getDataChunkV3.local_dictionary
    if (null != local_dictionary) {
      val compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        rawColumnPage.getDataChunkV3.getData_chunk_list.get(0).getChunk_meta)
      val encodings = local_dictionary.getDictionary_meta.encoders
      val encoderMetas = local_dictionary.getDictionary_meta.getEncoder_meta
      val encodingFactory = DefaultEncodingFactory.getInstance
      val decoder = encodingFactory.createDecoder(encodings, encoderMetas, compressorName)
      val dictionaryPage = decoder
        .decode(local_dictionary.getDictionary_data, 0, local_dictionary.getDictionary_data.length)
      val dictionaryMap = new util.HashMap[DictionaryByteArrayWrapper, Integer]
      val usedDictionaryValues = util.BitSet
        .valueOf(CompressorFactory.getInstance.getCompressor(compressorName)
          .unCompressByte(local_dictionary.getDictionary_values))
      var index = 0
      var i = usedDictionaryValues.nextSetBit(0)
      while ( { i >= 0 }) {
        dictionaryMap
          .put(new DictionaryByteArrayWrapper(dictionaryPage.getBytes({ index += 1; index - 1 })),
            i)
        i = usedDictionaryValues.nextSetBit(i + 1)
      }
      for (i <- data.indices) {
        if (null == dictionaryMap.get(new DictionaryByteArrayWrapper(data(i).getBytes))) {
          return false
        }
      }
      return true
    }
    false
  }

  def checkForLocalDictionary(dimensionRawColumnChunks: util
  .List[DimensionRawColumnChunk]): Boolean = {
    var isLocalDictionaryGenerated = false
    import scala.collection.JavaConversions._
    for (dimensionRawColumnChunk <- dimensionRawColumnChunks) {
      if (dimensionRawColumnChunk.getDataChunkV3
        .isSetLocal_dictionary) {
        isLocalDictionaryGenerated = true
      }
    }
    isLocalDictionaryGenerated
  }

}