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

package org.apache.carbondata.cluster.sdv.generated


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, InputStream}
import java.util

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterEach
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.lang.CharEncoding
import org.apache.commons.lang3.RandomStringUtils
import org.apache.hadoop.conf.Configuration
import org.junit.Assert

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{AvroCarbonWriter, CarbonWriter, Schema}

/**
 * Test Class for SDKwriterTestcase to verify all scenarios
 */

class SDKwriterTestCase extends QueryTest with BeforeAndAfterEach {

  var writerPath =
    s"${ resourcesPath }" + "/SparkCarbonFileFormat/WriterOutput1/"

  override def beforeEach: Unit = {
    sql("DROP TABLE IF EXISTS sdkTable1")
    sql("DROP TABLE IF EXISTS sdkTable2")
    sql("DROP TABLE IF EXISTS table1")
    cleanTestData()
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS sdkTable1")
    sql("DROP TABLE IF EXISTS sdkTable2")
    sql("DROP TABLE IF EXISTS table1")
    cleanTestData()
  }

  def cleanTestData() = {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
  }

  def buildTestDataSingleFile(): Any = {
    buildTestData(3, null)
  }

  def buildTestDataWithBadRecordForce(writerPath: String): Any = {
    var options = Map("bAd_RECords_action" -> "FORCE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordFail(writerPath: String): Any = {
    var options = Map("bAd_RECords_action" -> "FAIL").asJava
    buildTestData(15001, options)
  }

  def buildTestData(rows: Int, options: util.Map[String, String]): Any = {
    buildTestData(rows, options, List("name"), writerPath)
  }

  // prepare sdk writer output
  def buildTestData(rows: Int, options: util.Map[String, String], sortColumns: List[String], writerPath: String): Any = {
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
        if (options != null) {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2).withLoadOptions(options)
            .withCsvInput(Schema.parseJson(schema)).build()
        } else {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2)
            .withCsvInput(Schema.parseJson(schema)).build()
        }
      var i = 0
      while (i < rows) {
        if ((options != null) && (i < 3)) {
          // writing a bad record
          writer.write(Array[String]("abc" + i, String.valueOf(i.toDouble / 2), "abc"))
        } else {
          writer.write(Array[String]("abc" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        }
        i += 1
      }
      if (options != null) {
        //Keep one valid record. else carbon data file will not generate
        writer.write(Array[String]("abc" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)

      case _ => None
    }
  }

  def buildTestDataWithBadRecordIgnore(writerPath: String): Any = {
    var options = Map("bAd_RECords_action" -> "IGNORE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordRedirect(writerPath: String): Any = {
    var options = Map("bAd_RECords_action" -> "REDIRECT").asJava
    buildTestData(3, options)
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

  test("test create External Table with WriterPath") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))
  }

  test("test create External Table with Comment") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable comment 'this is comment' STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))
  }

  test("test create External Table and test files written from sdk writer") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))

    checkAnswer(sql("select name from sdkTable"), Seq(Row("abc0"),
      Row("abc1"),
      Row("abc2")))

    checkAnswer(sql("select age from sdkTable"), Seq(Row(0), Row(1), Row(2)))
    checkAnswer(sql("select * from sdkTable where age > 1 and age < 8"),
      Seq(Row("abc2", 2, 1.0)))

    checkAnswer(sql("select * from sdkTable where name = 'abc2'"),
      Seq(Row("abc2", 2, 1.0)))

    checkAnswer(sql("select * from sdkTable where name like '%b%' limit 2"),
      Seq(Row("abc0", 0, 0.0),
        Row("abc1", 1, 0.5)))

    checkAnswer(sql("select sum(age) from sdkTable where name like 'abc%'"), Seq(Row(3)))
    checkAnswer(sql("select count(*) from sdkTable where name like 'abc%' "), Seq(Row(3)))
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(3)))

  }

  test("test create External Table and test insert into external table") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql(s"""select count(*) from sdkTable where age = 1"""),
      Seq(Row(1)))

    sql("insert into sdktable select 'def0',1,5.5")
    sql("insert into sdktable select 'def1',5,6.6")

    checkAnswer(sql(s"""select count(*) from sdkTable where age = 1"""),
      Seq(Row(2)))
  }

  test("test create External Table and test insert into normal table with different schema") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql("DROP TABLE IF EXISTS table1")

    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql(
      "create table if not exists table1 (name string, age int) STORED BY 'carbondata'")
    sql("insert into table1 select * from sdkTable")
    checkAnswer(sql("select * from table1"), Seq(Row("abc0", 0),
      Row("abc1", 1),
      Row("abc2", 2)))
  }

  test("test Insert into External Table from another External Table with Same Schema") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable1")
    sql("DROP TABLE IF EXISTS sdkTable2")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable1 STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable2 STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("insert into sdkTable1 select *from sdkTable2")
    checkAnswer(sql("select count(*) from sdkTable1"), Seq(Row(6)))
  }

  test("test create External Table without Schema") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))
  }

  test("test External Table with insert overwrite") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql("DROP TABLE IF EXISTS table1")

    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))

    sql(
      "create table if not exists table1 (name string, age int, height double) STORED BY 'org" +
      ".apache.carbondata.format'")
    sql(s"""insert into table1 values ("aaaaa", 12, 20)""")

    checkAnswer(sql(s"""select count(*) from sdkTable where age = 1"""),
      Seq(Row(1)))

    sql("insert overwrite table sdkTable select * from table1")

    checkAnswer(sql(s"""select count(*) from sdkTable where age = 1"""),
      Seq(Row(0)))
  }

  test("test create External Table with Table properties should fail") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")

    val ex = intercept[AnalysisException] {
      sql(
        s"""CREATE EXTERNAL TABLE sdkTable STORED BY
           |'carbondata' LOCATION
           |'$writerPath' TBLPROPERTIES('sort_scope'='batch_sort') """.stripMargin)
    }
    assert(ex.message.contains("Table properties are not supported for external table"))
  }

  test("Read sdk writer output file and test without carbondata and carbonindex files should fail")
  {
    buildTestDataSingleFile()
    deleteFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    deleteFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")

    val exception = intercept[Exception] {
      //data source file format
      sql(
        s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
           |'$writerPath' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("Operation not allowed: Invalid table path provided:"))
  }

  test("test create External Table and test CTAS") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))

    sql("create table table1 stored by 'carbondata' as select *from sdkTable")

    checkAnswer(sql("select * from table1"), Seq(Row("abc0", 0, 0.0),
      Row("abc1", 1, 0.5),
      Row("abc2", 2, 1.0)))
  }

  test("test create External Table and test JOIN on External Tables") {
    buildTestDataSingleFile()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql("DROP TABLE IF EXISTS sdkTable1")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable1 STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable JOIN sdkTable1 on (sdkTable.age=sdkTable1.age)"),
      Seq(Row("abc0", 0, 0.0, "abc0", 0, 0.0),
        Row("abc1", 1, 0.5, "abc1", 1, 0.5),
        Row("abc2", 2, 1.0, "abc2", 2, 1.0)))
    checkAnswer(sql(
      "select * from sdkTable LEFT OUTER JOIN sdkTable1 on (sdkTable.age=sdkTable1.age)"),
      Seq(Row("abc0", 0, 0.0, "abc0", 0, 0.0),
        Row("abc1", 1, 0.5, "abc1", 1, 0.5),
        Row("abc2", 2, 1.0, "abc2", 2, 1.0)))
    checkAnswer(sql(
      "select * from sdkTable RIGHT OUTER JOIN sdkTable1 on (sdkTable.age=sdkTable1.age)"),
      Seq(Row("abc0", 0, 0.0, "abc0", 0, 0.0),
        Row("abc1", 1, 0.5, "abc1", 1, 0.5),
        Row("abc2", 2, 1.0, "abc2", 2, 1.0)))
  }

  test("test create external table and test bad record") {
    //1. Action = FORCE
    buildTestDataWithBadRecordForce(writerPath)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkTable"), Seq(
      Row("abc0", null, null),
      Row("abc1", null, null),
      Row("abc2", null, null),
      Row("abc3", 3, 1.5)))

    sql("DROP TABLE sdkTable")
    cleanTestData()

    //2. Action = REDIRECT
    buildTestDataWithBadRecordRedirect(writerPath)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(
      Row("abc3", 3, 1.5)))

    sql("DROP TABLE sdkTable")
    cleanTestData()

    //3. Action = IGNORE
    buildTestDataWithBadRecordIgnore(writerPath)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkTable"), Seq(
      Row("abc3", 3, 1.5)))

  }

  def buildAvroTestDataStructType(): Any = {
    buildAvroTestDataStruct(3, null)
  }

  def buildAvroTestDataStruct(rows: Int,
      options: util.Map[String, String]): Any = {

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
    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataBothStructArrayType(): Any = {
    buildAvroTestDataStructWithArrayType(3, null)
  }

  def buildAvroTestDataStructWithArrayType(rows: Int,
      options: util.Map[String, String]): Any = {

    val mySchema =
      """
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

  private def WriteFilesWithAvroWriter(rows: Int,
      mySchema: String,
      json: String): Unit = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = avroUtil.jsonToAvro(json, mySchema)

    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).build()
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

  def buildAvroTestDataArrayOfStructType(): Any = {
    buildAvroTestDataArrayOfStruct(3, null)
  }

  def buildAvroTestDataArrayOfStruct(rows: Int,
      options: util.Map[String, String]): Any = {

    val mySchema =
      """ {
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

  def buildAvroTestDataStructOfArrayType(): Any = {
    buildAvroTestDataStructOfArray(3, null)
  }

  def buildAvroTestDataStructOfArray(rows: Int,
      options: util.Map[String, String]): Any = {

    val mySchema =
      """ {
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

    val json =
      """ {
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

  test("Read sdk writer Avro output Record Type for nontransactional table") {
    buildAvroTestDataStructType()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkTable"), Seq(
      Row("bob", 10, Row("abc", "bang")),
      Row("bob", 10, Row("abc", "bang")),
      Row("bob", 10, Row("abc", "bang"))))

  }

  test("Read sdk writer Avro output with both Array and Struct Type for nontransactional table") {
    buildAvroTestDataBothStructArrayType()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkTable"), Seq(
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4)),
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4)),
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4))))
  }

  test("Read sdk writer Avro output with Array of struct for external table") {
    buildAvroTestDataArrayOfStructType()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql(s"""select count(*) from sdkTable"""),
      Seq(Row(3)))
  }

  test("Read sdk writer Avro output with struct of Array for nontransactional table") {
    buildAvroTestDataStructOfArrayType()
    assert(FileFactory.getCarbonFile(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql(s"""select count(*) from sdkTable"""),
      Seq(Row(3)))
  }

  test("Test sdk with longstring") {
    // here we specify the longstring column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"address\":\"varchar\"},\n")
      .append("   {\"age\":\"int\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema)).build()

    for (i <- 0 until 5) {
      writer.write(Array[String](s"name_$i", RandomStringUtils.randomAlphabetic(33000), i.toString))
    }
    writer.close()

    assert(FileFactory.getCarbonFile(writerPath).exists)
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(s"CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION '$writerPath'")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(5)))
  }

  test("Test sdk with longstring with more than 2MB length") {
    // here we specify the longstring column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"address\":\"varchar\"},\n")
      .append("   {\"age\":\"int\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema)).build()
    val varCharLen = 4000000
    for (i <- 0 until 3) {
      writer
        .write(Array[String](s"name_$i",
          RandomStringUtils.randomAlphabetic(varCharLen),
          i.toString))
    }
    writer.close()

    assert(FileFactory.getCarbonFile(writerPath).exists)
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(s"CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION '$writerPath'")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(3)))

    val op = sql("select address from sdkTable limit 1").collectAsList()
    assert(op.get(0).getString(0).length == varCharLen)
  }

  test("Test sdk with longstring with empty sort column and some direct dictionary columns") {
    // here we specify the longstring column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"address\":\"varchar\"},\n")
      .append("   {\"date1\":\"date\"},\n")
      .append("   {\"date2\":\"date\"},\n")
      .append("   {\"age\":\"int\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder
      .outputPath(writerPath)
      .sortBy(Array[String]())
      .withCsvInput(Schema.parseJson(schema)).build()

    for (i <- 0 until 5) {
      writer
        .write(Array[String](RandomStringUtils.randomAlphabetic(40000),
          "1999-12-01",
          "1998-12-01",
          i.toString))
    }
    writer.close()

    assert(FileFactory.getCarbonFile(writerPath).exists)
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(s"CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION '$writerPath'")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(5)))
  }
}

object avroUtil{
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
      var datum: GenericRecord = null
      datum = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }
}