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

import java.io.{File, FileFilter, IOException}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.{Assert, Test}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang.CharEncoding
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}

class TestNonTransactionalCarbonTable extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
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

  def buildTestDataWithSortColumns(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, false, null, List("age", "name"))
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
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  test("test create external table with sort columns") {
    buildTestDataWithSortColumns()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    // with partition
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable(name string) PARTITIONED BY (age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkExistence(sql("describe formatted sdkOutputTable"), true, "age,name")

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create External Table with Schema with partition, should ignore schema and partition")
  {
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


  test("test create External Table with insert into feature")
  {
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
    sql("select * from t1").show(200,false)
    sql("insert into sdkOutputTable select * from t1").show(200,false)

    checkAnswer(sql(s"""select * from sdkOutputTable where age = 12"""),
      Seq(Row("aaaaa", 12, 20.0)))

    sql("DROP TABLE sdkOutputTable")
    sql("drop table t1")

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("test create External Table with insert overwrite")
  {
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


  test("test create External Table with Load")
  {
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

      sql("select * from sdkOutputTable").show(false)
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

    //test filter query
    checkAnswer(sql("select * from sdkOutputTable where age = 1"), Seq(
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

  test("test huge data write with one batch having bad record") {

    val exception =
      intercept[RuntimeException] {
      buildTestDataWithBadRecordFail()
    }
    assert(exception.getMessage()
      .contains("Data load failed due to bad record"))

  }


  def buildAvroTestData(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema = "{" + "  \"name\": \"address\", " + "   \"type\": \"record\", " +
                   "    \"fields\": [  " +
                   "  { \"name\": \"name\", \"type\": \"string\"}, " +
                   "  { \"name\": \"age\", \"type\": \"int\"}, " + "  { " +
                   "    \"name\": \"address\", " + "      \"type\": { " +
                   "    \"type\" : \"record\", " + "        \"name\" : \"my_address\", " +
                   "        \"fields\" : [ " +
                   "    {\"name\": \"street\", \"type\": \"string\"}, " +
                   "    {\"name\": \"city\", \"type\": \"string\"} " + "  ]} " + "  } " +
                   "] " + "}"
    val json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", " +
               "\"city\":\"bang\"}}"

    // conversion to GenericData.Record
    val nn = new org.apache.avro.Schema.Parser().parse(mySchema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json.getBytes(CharEncoding.UTF_8), nn)
    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    // fields[1] = new Field("age", DataTypes.INT);
    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))
    fields(2) = new Field("address", "struct", fld)
    try {
      val writer = CarbonWriter.builder.withSchema(new Schema(fields))
        .outputPath(writerPath).isTransactionalTable(false).buildWriterForAvroInput
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

  def buildAvroTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestData(3, null)
  }

  def buildAvroTestDataArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    /**
     * *
     * {
     * "name": "address",
     * "type": "record",
     * "fields": [
     * {
     * "name": "name",
     * "type": "string"
     * },
     * {
     * "name": "age",
     * "type": "int"
     * },
     * {
     * "name": "address",
     * "type": {
     * "type": "array",
     * "items": {
     * "name": "street",
     * "type": "string"
     * }
     * }
     * }
     * ]
     * }
     **/
    val mySchema = "{\n" + "\t\"name\": \"address\",\n" + "\t\"type\": \"record\",\n" +
                   "\t\"fields\": [\n" + "\t\t{\n" + "\t\t\t\"name\": \"name\",\n" +
                   "\t\t\t\"type\": \"string\"\n" + "\t\t},\n" + "\t\t{\n" +
                   "\t\t\t\"name\": \"age\",\n" + "\t\t\t\"type\": \"int\"\n" + "\t\t},\n" +
                   "\t\t{\n" + "\t\t\t\"name\": \"address\",\n" + "\t\t\t\"type\": {\n" +
                   "\t\t\t\t\"type\": \"array\",\n" + "\t\t\t\t\"items\": {\n" +
                   "\t\t\t\t\t\"name\": \"street\",\n" +
                   "\t\t\t\t\t\"type\": \"string\"\n" + "\t\t\t\t}\n" + "\t\t\t}\n" +
                   "\t\t}\n" + "\t]\n" + "}"
    /**
     * {
     * "name": "bob",
     * "age": 10,
     * "address": [
     * "abc", "def"
     * ]
     * }
     **/
    val json: String = "{\n" + "\t\"name\": \"bob\",\n" + "\t\"age\": 10,\n" +
                       "\t\"address\": [\n" + "\t\t\"abc\", \"defg\"\n" + "\t]\n" + "}"
    // conversion to GenericData.Record
    val nn = new org.apache.avro.Schema.Parser().parse(mySchema)
    val converter = new JsonAvroConverter
    val record = converter
      .convertToGenericDataRecord(json.getBytes(CharEncoding.UTF_8), nn)
    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    val fld = new util.ArrayList[StructField]
    fld.add(new StructField("street", DataTypes.STRING))
    fld.add(new StructField("city", DataTypes.STRING))
    fields(2) = new Field("address", "struct", fld)
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
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  def buildAvroTestDataSingleFileArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayType(3, null)
  }

  test("Read sdk writer Avro output Record Type") {
    buildAvroTestDataSingleFile()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

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

    sql("select * from sdkOutputTable").show(false)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, Row("abc","defg")),
      Row("bob", 10, Row("abc","defg")),
      Row("bob", 10, Row("abc","defg"))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }
}