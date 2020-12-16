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

package org.apache.carbondata.presto.integrationtest

import java.io.{BufferedInputStream, File, FileInputStream}
import java.sql.SQLException
import java.util

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.{DataTypes, Field, StructField}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.{PrestoServer, PrestoTestUtil}
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}

class PrestoTestNonTransactionalTableFiles
  extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoTestNonTransactionalTableFiles].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val writerPath = storePath + "/sdk_output/files"
  private val writerPathBinary = storePath + "/sdk_output/files1"
  private val prestoServer = new PrestoServer
  private var varcharString = new String

  override def beforeAll: Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")
    prestoServer.startServer("sdk_output", map)
    prestoServer.execute("drop schema if exists sdk_output")
    prestoServer.execute("create schema sdk_output")
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

  def buildTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    createTable

    buildTestData(3, null, true)
  }

  def buildStructData(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    createTable
    buildTestData(3, null, true)
  }

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    createTable
    buildTestData(1000000, null, false)
  }

  private def createTable = {
    prestoServer.execute("drop table if exists sdk_output.files")
    prestoServer
      .execute(
        "create table sdk_output.files(name varchar, age int, id tinyint, height double, salary " +
        "real, address varchar) with" +
        "(format='CARBON') ")
  }

  private def createTableBinary = {
    prestoServer.execute("drop table if exists sdk_output.files1")
    prestoServer.execute(
      "create table sdk_output.files1(name boolean, age int, id varbinary, height double, salary " +
        "real) with(format='CARBON') ")
  }

  def buildTestData(rows: Int, options: util.Map[String, String], varcharDataGen: Boolean): Any = {
    buildTestData(rows, options, List("name"), varcharDataGen)
  }

  // prepare sdk writer output
  def buildTestData(rows: Int,
      options: util.Map[String, String],
      sortColumns: List[String],
      varcharDataGen: Boolean): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"NaMe\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"id\":\"byte\"},\n")
      .append("   {\"height\":\"double\"},\n")
      .append("   {\"salary\":\"float\"},\n")
      .append("   {\"address\":\"varchar\"}\n")
      .append("]")
      .toString()

    // Build Varchar Column data
    var varcharValue: String = {
      if (varcharDataGen) {
        RandomStringUtils.randomAlphabetic(32001)
      } else {
        "a"
      }
    }

    varcharString = varcharValue
    try {
      val builder = CarbonWriter.builder()
      val writer =
        if (options != null) {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2).withLoadOptions(options)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable")
            .build()
        } else {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable")
            .build()
        }
      var i = 0
      while (i < rows) {
        if ((options != null) && (i < 3)) {
          // writing a bad record
          writer
            .write(Array[String]("robot" + i,
              String.valueOf(i),
              String.valueOf(i.toDouble / 2),
              "robot",
              String.valueOf(i.toFloat / 2),
              String.valueOf(varcharValue)))
        } else {
          writer
            .write(Array[String]("robot" + i,
              String.valueOf(i),
              String.valueOf(i % 128),
              String.valueOf(i.toDouble / 2),
              String.valueOf(i.toFloat / 2),
              String.valueOf(varcharValue)))
        }
        i += 1
      }
      if (options != null) {
        // Keep one valid record. else carbon data file will not generate
        writer
          .write(Array[String]("robot" + i,
            String.valueOf(i),
            String.valueOf(i),
            String.valueOf(i.toDouble / 2),
            String.valueOf(i.toFloat / 2),
            String.valueOf(varcharValue)))
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int, sortColumns: Array[String], path : String): Any = {
    val fields: Array[Field] = new Array[Field](5)
    // same column name, but name as boolean type
    fields(0) = new Field("name", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("id", DataTypes.BINARY)
    fields(3) = new Field("height", DataTypes.DOUBLE)
    fields(4) = new Field("salary", DataTypes.FLOAT)

    val imagePath = rootPath + "/sdk/sdk/src/test/resources/image/carbondatalogo.jpg"
    try {
      var i = 0
      val bis = new BufferedInputStream(new FileInputStream(imagePath))
      var hexValue: Array[Char] = null
      val originBinary = new Array[Byte](bis.available)
      while (bis.read(originBinary) != -1) {
        hexValue = Hex.encodeHex(originBinary)
      }
      bis.close()
      val binaryValue = String.valueOf(hexValue)
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.currentTimeMillis()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
      while (i < rows) {
        writer
          .write(Array[String]("true",
            String.valueOf(i),
            binaryValue,
            String.valueOf(i.toDouble / 2),
            String.valueOf(i.toFloat / 2)))
        i += 1
      }
      writer
        .write(Array[String]("true",
          String.valueOf(i),
          String.valueOf("abc"),
          String.valueOf(i.toDouble / 2),
          String.valueOf(i.toFloat / 2)))
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  def cleanTestData(): Unit = {
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

  test("test show schemas") {
    buildTestDataSingleFile()
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("show schemas ")
    assert(actualResult
      .equals(List(Map("Schema" -> "information_schema"),
        Map("Schema" -> "sdk_output"))))
    cleanTestData()
  }

  test("test show tables") {
    buildTestDataSingleFile()
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("show tables")
    assert(actualResult
      .equals(List(Map("Table" -> "files"))))
    cleanTestData()
  }

  test("test read sdk output files") {
    buildTestDataSingleFile()
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(*) AS RESULT FROM files ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 3))
    assert(actualResult.equals(expectedResult))

    val FloatFilterResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM files where salary > 0.0")
    assert(FloatFilterResult.length == 2 )

    val ByteFilterResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM files where id = 2")
    assert(ByteFilterResult.length == 1 )
    cleanTestData()
  }

  test("test read multiple carbondata and index files") {
    buildTestDataMultipleFiles()
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(*) AS RESULT FROM files ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 1000000))
    assert(actualResult.equals(expectedResult))
    cleanTestData()
  }

  test("test reading different schema") {
    buildTestDataSingleFile()
    buildTestDataOtherDataType(3, null, writerPath)
    val exception =
      intercept[SQLException] {
        val actualResult: List[Map[String, Any]] = prestoServer
          .executeQuery("select count(*) as RESULT from files ")
      }
    assert(exception.getMessage()
      .contains("All common columns present in the files doesn't have same datatype"))
    cleanTestData()
  }

  test("test reading binary") {
    FileUtils.deleteDirectory(new File(writerPathBinary))
    createTableBinary
    buildTestDataOtherDataType(3, null, writerPathBinary)
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select id from files1 ")
    assert(actualResult.size == 4)
    // check the binary byte Array size, as original hex encoded image byte array size is 118198
    assert(actualResult.head("id").asInstanceOf[Array[Byte]].length == 118198)
    // validate some initial bytes
    assert(actualResult.head("id").asInstanceOf[Array[Byte]](0) == 56)
    assert(actualResult.head("id").asInstanceOf[Array[Byte]](1) == 57)
    assert(actualResult.head("id").asInstanceOf[Array[Byte]](2) == 53)
    assert(actualResult.head("id").asInstanceOf[Array[Byte]](3) == 48)

    val binaryFilterResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select id from files1 where id = cast('abc' as varbinary)");
    assert(binaryFilterResult.length == 1)
    FileUtils.deleteDirectory(new File(writerPathBinary))
  }

  test("test reading without carbon index file") {
    buildTestDataSingleFile()
    deleteFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    val exception =
      intercept[SQLException] {
        val actualResult: List[Map[String, Any]] = prestoServer
          .executeQuery("select * from files ")
      }
    assert(exception.getMessage()
      .contains("No Index files are present in the table location"))
    cleanTestData()
  }

  test("test select all columns") {
    buildTestDataSingleFile()
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "name" -> "robot0",
      "height" -> 0.0,
      "age" -> 0,
      "salary" -> 0.0,
      "id" -> 0,
      "address" -> varcharString),
      Map("name" -> "robot1",
        "height" -> 0.5,
        "age" -> 1,
        "salary" -> 0.5,
        "id" -> 1,
        "address" -> varcharString),
      Map("name" -> "robot2",
        "height" -> 1.0,
        "age" -> 2,
        "salary" -> 1.0,
        "id" -> 2,
        "address" -> varcharString))
    assert(actualResult.toString() equals expectedResult.toString())
  }

  test("Test for query on Varchar columns") {
    buildTestDataSingleFile()
    val actualRes: List[Map[String, Any]] = prestoServer.
      executeQuery("select max(length(address)) from files")
    val expectedRes: List[Map[String, Any]] = List(Map("_col0" -> 32001))
    assert(actualRes.toString() equals expectedRes.toString())
  }

  test("test struct of primitive type") {
    import scala.collection.JavaConverters._
    val writerPathComplex = storePath + "/sdk_output/files4"
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files4")
    prestoServer.execute(
      "create table sdk_output.files4(stringField varchar, structField ROW(byteField tinyint, " +
      "shortField SMALLINT, intField Integer, longField BIGINT, floatField real, doubleField " +
      "DOUBLE, binaryField varbinary, dateField date, timeStampField timestamp, booleanField " +
      "boolean, longStringField varchar, decimalField decimal(8,2), stringChildField varchar)) " +
      "with(format='CARBON') ")

    val imagePath = rootPath + "/sdk/sdk/src/test/resources/image/carbondatalogo.jpg"
    val bis = new BufferedInputStream(new FileInputStream(imagePath))
    var hexValue: Array[Char] = null
    val originBinary = new Array[Byte](bis.available)
    while (bis.read(originBinary) != -1) {
      hexValue = Hex.encodeHex(originBinary)
    }
    bis.close()
    val binaryValue = String.valueOf(hexValue)

    val longChar = RandomStringUtils.randomAlphabetic(33000)

    val fields = List(new StructField("byteField", DataTypes.BYTE),
      new StructField("shortField", DataTypes.SHORT),
      new StructField("intField", DataTypes.INT),
      new StructField("longField", DataTypes.LONG),
      new StructField("floatField", DataTypes.FLOAT),
      new StructField("doubleField", DataTypes.DOUBLE),
      new StructField("binaryField", DataTypes.BINARY),
      new StructField("dateField", DataTypes.DATE),
      new StructField("timeStampField", DataTypes.TIMESTAMP),
      new StructField("booleanField", DataTypes.BOOLEAN),
      new StructField("longStringField", DataTypes.VARCHAR),
      new StructField("decimalField", DataTypes.createDecimalType(8, 2)),
      new StructField("stringChildField", DataTypes.STRING))
    val structType = Array(new Field("stringField", DataTypes.STRING), new Field
    ("structField", "struct", fields.asJava))

    try {
      val options: util.Map[String, String] =
        Map("bAd_RECords_action" -> "FORCE", "quotechar" -> "\"").asJava
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex)
          .uniqueIdentifier(System.nanoTime())
          .withLoadOptions(options)
          .withBlockSize(2)
          .enableLocalDictionary(false)
          .withCsvInput(new Schema(structType)).writtenBy("presto").build()

      val array1 = Array[String]("row1", null, null, null, null, null, null, null, null, null,
        null, null, null, null)

      val array2 = Array[String]("row2", "5" + "\001" + "5" + "\001" + "5" + "\001" + "5" +
                                         "\001" + "5.512" + "\001" + "5.512" + "\001" +
                                         binaryValue + "\001" + "2019-03-02" + "\001" +
                                         "2019-02-12 03:03:34" + "\001" + "true" + "\001" +
                                         longChar + "\001" + "-2.2" + "\001" + "stringName")
      writer.write(array1)
      writer.write(array2)
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files4 ")
    assert(actualResult.size == 2)


    for(i <- 0 to 1) {
      val row = actualResult(i)("stringfield")
      val result = actualResult(i)("structfield").asInstanceOf[java.util.Map[String, Any]]
      if(row == "row1") { assert(result.get("bytefield") == null)
        assert(result.get("shortfield") == null)
        assert(result.get("intfield") == null)
        assert(result.get("longfield") == null)
        assert(result.get("floatfield") == null)
        assert(result.get("doublefield") == null)
        assert(result.get("binaryfield") == null)
        assert(result.get("datefield") == null)
        assert(result.get("timestampfield") == null)
        assert(result.get("booleanfield") == null)
        assert(result.get("longstringfield") == null)
        assert(result.get("decimalfield") == null)
        assert(result.get("stringchildfield") == null)
      } else {
        assert(result.get("bytefield") == 5)
        assert(result.get("shortfield") == 5)
        assert(result.get("intfield") == 5)
        assert(result.get("longfield") == 5L)
        assert(result.get("floatfield") == 5.512f)
        assert(result.get("doublefield") == 5.512)
        assert((result.get("binaryfield").asInstanceOf[Array[Byte]]).length == 118198)
        assert(result.get("datefield") == "2019-03-02")
        assert(result.get("timestampfield") == "2019-02-12 03:03:34.000")
        assert(result.get("booleanfield") == true)
        assert(result.get("longstringfield") == longChar)
        assert(result.get("decimalfield") == "-2.20")
        assert(result.get("stringchildfield") == "stringName")
      }
    }
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

  test("test struct of date type with huge data") {
    import scala.collection.JavaConverters._
    val writerPathComplex = storePath + "/sdk_output/files2"
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files2")
    prestoServer
      .execute(
        "create table sdk_output.files2(structField ROW(dateField date)) with(format='CARBON') ")
    val fields = List(
      new StructField("dateField", DataTypes.DATE))
    val structType = Array(new Field("structField", "struct", fields.asJava))
    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).enableLocalDictionary(false)
          .withCsvInput(new Schema(structType)).writtenBy("presto").build()
      var i = 0
      while (i < 100000) {
        val array = Array[String]("2019-03-02")
        writer.write(array)
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select structField from files2 ")
    assert(actualResult.size == 100000)
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

  test("test Array of primitive type") {
    val writerPathComplex = storePath + "/sdk_output/files5"
    import scala.collection.JavaConverters._
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files5")
    prestoServer.execute(
      "create table sdk_output.files5(arrayByte ARRAY(tinyint), arrayShort ARRAY(smallint), " +
      "arrayInt ARRAY(int), arrayLong ARRAY(bigint), arrayFloat ARRAY(real), arrayDouble ARRAY" +
      "(double), arrayBinary ARRAY(varbinary), arrayDate ARRAY(date), arrayTimestamp ARRAY" +
      "(timestamp), arrayBoolean ARRAY(boolean), arrayVarchar ARRAY(varchar), arrayDecimal ARRAY" +
      "(decimal(8,2)), arrayString ARRAY(varchar), stringField varchar ) with(format='CARBON') ")

    val imagePath = rootPath + "/sdk/sdk/src/test/resources/image/carbondatalogo.jpg"
    val bis = new BufferedInputStream(new FileInputStream(imagePath))
    var hexValue: Array[Char] = null
    val originBinary = new Array[Byte](bis.available)
    while (bis.read(originBinary) != -1) {
      hexValue = Hex.encodeHex(originBinary)
    }
    bis.close()
    val binaryValue = String.valueOf(hexValue)

    val longChar = RandomStringUtils.randomAlphabetic(33000)

    val fields1 = List(new StructField("byteField", DataTypes.BYTE))
    val structType1 = new Field("arrayByte", "array", fields1.asJava)
    val fields2 = List(new StructField("shortField", DataTypes.SHORT))
    val structType2 = new Field("arrayShort", "array", fields2.asJava)
    val fields3 = List(new StructField("intField", DataTypes.INT))
    val structType3 = new Field("arrayInt", "array", fields3.asJava)
    val fields4 = List(new StructField("longField", DataTypes.LONG))
    val structType4 = new Field("arrayLong", "array", fields4.asJava)
    val fields5 = List(new StructField("floatField", DataTypes.FLOAT))
    val structType5 = new Field("arrayFloat", "array", fields5.asJava)
    val fields6 = List(new StructField("DoubleField", DataTypes.DOUBLE))
    val structType6 = new Field("arrayDouble", "array", fields6.asJava)
    val fields7 = List(new StructField("binaryField", DataTypes.BINARY))
    val structType7 = new Field("arrayBinary", "array", fields7.asJava)
    val fields8 = List(new StructField("dateField", DataTypes.DATE))
    val structType8 = new Field("arrayDate", "array", fields8.asJava)
    val fields9 = List(new StructField("timestampField", DataTypes.TIMESTAMP))
    val structType9 = new Field("arrayTimestamp", "array", fields9.asJava)
    val fields10 = List(new StructField("booleanField", DataTypes.BOOLEAN))
    val structType10 = new Field("arrayBoolean", "array", fields10.asJava)
    val fields11 = List(new StructField("varcharField", DataTypes.VARCHAR))
    val structType11 = new Field("arrayVarchar", "array", fields11.asJava)
    val fields12 = List(new StructField("decimalField", DataTypes.createDecimalType(8, 2)))
    val structType12 = new Field("arrayDecimal", "array", fields12.asJava)
    val fields13 = List(new StructField("stringField", DataTypes.STRING))
    val structType13 = new Field("arrayString", "array", fields13.asJava)
    val structType14 = new Field("stringField", DataTypes.STRING)

    try {
      val options: util.Map[String, String] =
        Map("bAd_RECords_action" -> "FORCE", "quotechar" -> "\"").asJava
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex).withLoadOptions(options)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).enableLocalDictionary(false)
          .withCsvInput(new Schema(Array[Field](
            structType1, structType2, structType3, structType4, structType5, structType6,
            structType7, structType8, structType9, structType10, structType11, structType12,
            structType13, structType14))).writtenBy("presto").build()

      var array = Array[String](null, null, null, null, null, null, null, null, null, null,
        null, null, null, "row1")
      writer.write(array)
      array = Array[String]("3" + "\001" + "5" + "\001" + "4",
        "4" + "\001" + "5" + "\001" + "6",
        "4",
        "2" + "\001" + "59999999" + "\001" + "99999999999",
        "5.4646" + "\001" + "5.55" + "\001" + "0.055",
        "5.46464646464" + "\001" + "5.55" + "\001" + "0.055",
        binaryValue,
        "2019-03-02" + "\001" + "2020-03-02" + "\001" + "2021-04-02",
        "2019-02-12 03:03:34" + "\001" +"2020-02-12 03:03:34" + "\001" + "2021-03-12 03:03:34",
        "true" + "\001" + "false",
        longChar,
        "999.232323" + "\001" + "0.1234",
        "japan" + "\001" + "china" + "\001" + "iceland",
        "row2")
      writer.write(array)
      writer.close()
    } catch {
      case e: Exception =>
        assert(false)
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files5 ")

    assert(actualResult.size == 2)
    PrestoTestUtil.validateArrayOfPrimitiveTypeData(actualResult, longChar)
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

  test("test Array of date type with huge data") {
    val writerPathComplex = storePath + "/sdk_output/files6"
    import scala.collection.JavaConverters._
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files6")
    prestoServer
      .execute(
        "create table sdk_output.files6(arrayDate ARRAY(date)) with(format='CARBON') ")
    val fields8 = List(new StructField("intField", DataTypes.DATE))
    val structType8 = new Field("arrayDate", "array", fields8.asJava)
    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).enableLocalDictionary(false)
          .withCsvInput(new Schema(Array[Field](structType8))).writtenBy("presto").build()

      var i = 0
      while (i < 50000) {
        val array = Array[String]("2019-03-02" + "\001" + "2020-03-02" + "\001" + "2021-04-02")
        writer.write(array)
        val array1 = Array[String]("2021-04-02")
        writer.write(array1)
        i += 1
      }
      writer.close()
    } catch {
      case e: Exception =>
        assert(false)
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files6 ")
    assert(actualResult.size == 100 * 1000)
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

  test("test Array with local dictionary") {
    val writerPathComplex = storePath + "/sdk_output/files7"
    import scala.collection.JavaConverters._
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files7")
    prestoServer
      .execute(
        "create table sdk_output.files7(arrayString ARRAY(varchar), arrayDate ARRAY(DATE), " +
        "arrayVarchar ARRAY(varchar), stringField varchar ) with(format='CARBON') ")

    val field1 = List(new StructField("stringField", DataTypes.STRING))
    val structType1 = new Field("arrayString", "array", field1.asJava)
    val field2 = List(new StructField("dateField", DataTypes.DATE))
    val structType2 = new Field("arrayDate", "array", field2.asJava)
    val fields3 = List(new StructField("varcharField", DataTypes.VARCHAR))
    val structType3 = new Field("arrayVarchar", "array", fields3.asJava)
    val structType4 = new Field("stringField", DataTypes.STRING)

    val longChar = RandomStringUtils.randomAlphabetic(33000)

    try {
      val options: util.Map[String, String] = Map("bAd_RECords_action" -> "FORCE",
        "quotechar" -> "\"").asJava
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex).withLoadOptions(options)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).enableLocalDictionary(true)
          .withCsvInput(new Schema(Array[Field](structType1,
            structType2,
            structType3,
            structType4))).writtenBy("presto").build()

      var array = Array[String](null,
        null,
        null,
        "row1")
      writer.write(array)
      array = Array[String]("India" + "\001" + "Japan" + "\001" + "India",
        "2019-03-02" + "\001" + "2020-03-02",
        longChar,
        "row2")
      writer.write(array)
      array = Array[String](
        "Iceland",
        "2019-03-02" + "\001" + "2020-03-02" + "\001" + "2021-04-02" + "\001" + "2021-04-03" +
        "\001" + "2021-04-02",
        longChar,
        "row3")
      writer.write(array)

      writer.close()
    } catch {
      case e: Exception =>
        assert(false)
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files7 ")
    PrestoTestUtil.validateArrayOfPrimitiveTypeDataWithLocalDict(actualResult, longChar)
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

  test("test Struct with local dictionary") {
    import scala.collection.JavaConverters._
    val writerPathComplex = storePath + "/sdk_output/files8"
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files8")
    prestoServer
      .execute(
        "create table sdk_output.files8(stringField varchar, structField ROW(stringChildField " +
        "varchar, dateField date, longStringField varchar)) with(format='CARBON') ")
    val longChar = RandomStringUtils.randomAlphabetic(33000)

    val fields = List(new StructField("stringChildField", DataTypes.STRING),
      new StructField("dateField", DataTypes.DATE),
      new StructField("longStringField", DataTypes.VARCHAR)
    )
    val structType = Array(new Field("stringField", DataTypes.STRING), new Field
    ("structField", "struct", fields.asJava))
    try {
      val options: util.Map[String, String] = Map("bAd_RECords_action" -> "FORCE",
        "quotechar" -> "\"").asJava
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex)
          .uniqueIdentifier(System.nanoTime())
          .withLoadOptions(options)
          .withBlockSize(2)
          .enableLocalDictionary(true)
          .withCsvInput(new Schema(structType))
          .writtenBy("presto")
          .build()

      val array1 = Array[String]("row1",
        null,
        null,
        null)
      val array2 = Array[String]("row2", "local dictionary"
                                         + "\001" + "2019-03-02"
                                         + "\001" + longChar)
      writer.write(array1)
      writer.write(array2)
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }

    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files8 ")
    assert(actualResult.size == 2)

    for (i <- 0 to 1) {
      val row = actualResult(i)("stringfield")
      val result = actualResult(i)("structfield").asInstanceOf[java.util.Map[String, Any]]
      if (row == "row1") {
        assert(result.get("stringchildfield") == null)
        assert(result.get("datefield") == null)
        assert(result.get("longStringField") == null)
      }
      else if (row == "row2") {
        assert(result.get("stringchildfield") == "local dictionary")
        assert(result.get("datefield") == "2019-03-02")
        assert(result.get("longstringfield") == longChar)
      }
    }
    FileUtils.deleteDirectory(new File(writerPathComplex))

  }

  test("test Array of varchar type with huge data enabling local dictionary") {
    val writerPathComplex = storePath + "/sdk_output/files9"
    import scala.collection.JavaConverters._
    FileUtils.deleteDirectory(new File(writerPathComplex))
    prestoServer.execute("drop table if exists sdk_output.files9")
    prestoServer
      .execute(
        "create table sdk_output.files9(arrayString ARRAY(varchar)) with(format='CARBON') ")
    val fields8 = List(new StructField("intField", DataTypes.STRING))
    val structType8 = new Field("arrayString", "array", fields8.asJava)
    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPathComplex)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).enableLocalDictionary(true)
          .withCsvInput(new Schema(Array[Field](structType8))).writtenBy("presto").build()

      var i = 0
      while (i < 50000) {
        val array = Array[String]("India" + "\001" + "China" + "\001" + "Japan")
        writer.write(array)
        val array1 = Array[String]("Korea")
        writer.write(array1)
        i += 1
      }
      writer.close()
    } catch {
      case e: Exception =>
        assert(false)
    }
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from files9 ")
    PrestoTestUtil.validateHugeDataForArrayWithLocalDict(actualResult)
    FileUtils.deleteDirectory(new File(writerPathComplex))
  }

}
