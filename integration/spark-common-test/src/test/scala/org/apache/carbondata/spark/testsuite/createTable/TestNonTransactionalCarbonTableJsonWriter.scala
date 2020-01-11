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

import java.io.{File, IOException}
import java.sql.Timestamp
import java.util

import org.apache.avro
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file._
import scala.collection.JavaConverters._

class TestNonTransactionalCarbonTableJsonWriter extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            + "../."
                            + "./target/SparkCarbonFileFormat/WriterOutput/").getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")

  var backupdateFormat = CarbonProperties.getInstance().getProperty(
    CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  var backupTimeFormat = CarbonProperties.getInstance().getProperty(
    CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        backupTimeFormat)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        backupdateFormat)
  }

  /**
   * Utility function to read a whole file as a string,
   * Must not use this if the file is very huge. As it may result in memory exhaustion.
   *
   * @param filePath
   * @return
   */
  def readFromFile(filePath: String): String = {
    val file = new File(filePath)
    val uri = file.toURI
    try {
      val bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(uri))
      new String(bytes, "UTF-8")
    } catch {
      case e: IOException =>
        e.printStackTrace()
        return "ERROR loading file " + filePath
    }
  }

  private def writeCarbonFileFromJsonRowInput(jsonRow: String,
      carbonSchema: Schema) = {
    try {
      val options: util.Map[String, String] = Map("bAd_RECords_action" -> "FAIL", "quotechar" -> "\"").asJava
      val writer = CarbonWriter.builder
              .outputPath(writerPath)
              .uniqueIdentifier(System.currentTimeMillis())
              .withLoadOptions(options)
              .withJsonInput(carbonSchema)
              .writtenBy("TestNonTransactionalCarbonTableJsonWriter")
              .build()
      writer.write(jsonRow)
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  // test all primitive type
  test("Read sdk writer Json output of all primitive type") {
    FileUtils.deleteDirectory(new File(writerPath))

    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/allPrimitiveType.json"

    val fields = new Array[Field](9)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    fields(2) = new Field("shortField", DataTypes.SHORT)
    fields(3) = new Field("longField", DataTypes.LONG)
    fields(4) = new Field("doubleField", DataTypes.DOUBLE)
    fields(5) = new Field("boolField", DataTypes.BOOLEAN)
    fields(6) = new Field("dateField", DataTypes.DATE)
    fields(7) = new Field("timeField", DataTypes.TIMESTAMP)
    fields(8) = new Field("decimalField", DataTypes.createDecimalType(8, 2))

    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row("ajantha\"bhat\"",
        26,
        26,
        1234567,
        23.3333,
        false,
        java.sql.Date.valueOf("2019-03-02"),
        Timestamp.valueOf("2019-02-12 03:03:34"),
        55.35)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test all primitive type with bad record
  test("Read sdk writer Json output of all primitive type with Bad record") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/allPrimitiveTypeBadRecord.json"

    val fields = new Array[Field](9)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    fields(2) = new Field("shortField", DataTypes.SHORT)
    fields(3) = new Field("longField", DataTypes.LONG)
    fields(4) = new Field("doubleField", DataTypes.DOUBLE)
    fields(5) = new Field("boolField", DataTypes.BOOLEAN)
    fields(6) = new Field("dateField", DataTypes.DATE)
    fields(7) = new Field("timeField", DataTypes.TIMESTAMP)
    fields(8) = new Field("decimalField", DataTypes.createDecimalType(8, 2))

    val jsonRow = readFromFile(dataPath)
    var exception = intercept[java.lang.AssertionError] {
      writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    }
    assert(exception.getMessage()
      .contains("Data load failed due to bad record"))

    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test array Of array Of array Of Struct
  test("Read sdk writer Json output of array Of array Of array Of Struct") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath = resourcesPath + "/jsonFiles/data/arrayOfarrayOfarrayOfStruct.json"
    // for testing purpose get carbonSchema from avro schema.
    // Carbon schema will be passed without AVRO in the real scenarios
    var schemaPath = resourcesPath + "/jsonFiles/schema/arrayOfarrayOfarrayOfStruct.avsc"
    val avroSchema = new avro.Schema.Parser().parse(readFromFile(schemaPath))
    val carbonSchema = AvroCarbonWriter.getCarbonSchemaFromAvroSchema(avroSchema)

    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, carbonSchema)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)
    /*
    * +-------+---+-----------------------------------------+
      |name   |age|BuildNum                                 |
      +-------+---+-----------------------------------------+
      |ajantha|26 |[WrappedArray(WrappedArray([abc,city1]))]|
      +-------+---+-----------------------------------------+
    *
    */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test array Of Struct Of Struct
  test("Read sdk writer Json output of array Of Struct Of Struct") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath = resourcesPath + "/jsonFiles/data/arrayOfStructOfStruct.json"
    // for testing purpose get carbonSchema from avro schema.
    // Carbon schema will be passed without AVRO in the real scenarios
    var schemaPath = resourcesPath + "/jsonFiles/schema/arrayOfStructOfStruct.avsc"
    val avroSchema = new avro.Schema.Parser().parse(readFromFile(schemaPath))
    val carbonSchema = AvroCarbonWriter.getCarbonSchemaFromAvroSchema(avroSchema)

    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, carbonSchema)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    /*
    *  +----+---+-------------------+
    *  |name|age|doorNum            |
    *  +----+---+-------------------+
    *  |bob |10 |[[abc,city1,[a,1]]]|
    *  +----+---+-------------------+
    * */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test struct of all types
  test("Read sdk writer Json output of Struct of all types") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath = resourcesPath + "/jsonFiles/data/StructOfAllTypes.json"
    // for testing purpose get carbonSchema from avro schema.
    // Carbon schema will be passed without AVRO in the real scenarios
    var schemaPath = resourcesPath + "/jsonFiles/schema/StructOfAllTypes.avsc"
    val avroSchema = new avro.Schema.Parser().parse(readFromFile(schemaPath))
    val carbonSchema = AvroCarbonWriter.getCarbonSchemaFromAvroSchema(avroSchema)

    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, carbonSchema)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    assert(sql("select * from sdkOutputTable").collectAsList().toString.equals(
      "[[[bob,10,12345678,123400.78,true,WrappedArray(1, 2, 3, 4, 5, 6),WrappedArray(abc, def)," +
      "WrappedArray(1234567, 2345678),WrappedArray(1.0, 2.0, 33.33),WrappedArray(true, false, " +
      "false, true)]]]"))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test : One element as null
  test("Read sdk writer Json output of primitive type with one element as null") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/PrimitiveTypeWithNull.json"
    val fields = new Array[Field](2)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(null,
        26)))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test : Schema length is greater than array length
  test("Read Json output of primitive type with Schema length is greater than array length") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/PrimitiveTypeWithNull.json"
    val fields = new Array[Field](5)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    fields(2) = new Field("shortField", DataTypes.SHORT)
    fields(3) = new Field("longField", DataTypes.LONG)
    fields(4) = new Field("doubleField", DataTypes.DOUBLE)
    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row(null, 26, null, null, null)))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test : Schema length is lesser than array length
  test("Read Json output of primitive type with Schema length is lesser than array length") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/allPrimitiveType.json"
    val fields = new Array[Field](2)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row("ajantha\"bhat\"", 26)))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }

  // test : Schema length is lesser than array length
  test("Read Json for binary") {
    FileUtils.deleteDirectory(new File(writerPath))
    var dataPath: String = null
    dataPath = resourcesPath + "/jsonFiles/data/allPrimitiveType.json"
    val fields = new Array[Field](3)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    fields(2) = new Field("binaryField", DataTypes.BINARY)
    val jsonRow = readFromFile(dataPath)
    writeCarbonFileFromJsonRowInput(jsonRow, new Schema(fields))
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath' """.stripMargin)
    sql("select * from sdkOutputTable").show()
    checkAnswer(sql("select * from sdkOutputTable"),
      Seq(Row("ajantha\"bhat\"", 26, "abc".getBytes())))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    FileUtils.deleteDirectory(new File(writerPath))
  }
}
