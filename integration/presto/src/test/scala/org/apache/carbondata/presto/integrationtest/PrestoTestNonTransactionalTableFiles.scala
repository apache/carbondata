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

import java.io.File
import java.sql.SQLException
import java.util

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}


class PrestoTestNonTransactionalTableFiles extends FunSuiteLike with BeforeAndAfterAll {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoTestNonTransactionalTableFiles].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val systemPath = s"$rootPath/integration/presto/target/system"
  private val writerPath = storePath + "/sdk_output/files"
  private val prestoServer = new PrestoServer
  private var varcharString = new String

  override def beforeAll: Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
      systemPath)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")

    prestoServer.startServer("sdk_output", map)
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

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    createTable
    buildTestData(1000000, null, false)
  }

  private def createTable = {
    prestoServer.execute("drop table if exists sdk_output.files")
    prestoServer.execute("drop schema if exists sdk_output")
    prestoServer.execute("create schema sdk_output")
    prestoServer
      .execute(
        "create table sdk_output.files(name varchar, age int, id tinyint, height double, salary " +
        "real, address varchar) with" +
        "(format='CARBON') ")
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
        //Keep one valid record. else carbon data file will not generate
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
  def buildTestDataOtherDataType(rows: Int, sortColumns: Array[String]): Any = {
    val fields: Array[Field] = new Array[Field](5)
    // same column name, but name as boolean type
    fields(0) = new Field("name", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("id", DataTypes.BYTE)
    fields(3) = new Field("height", DataTypes.DOUBLE)
    fields(4) = new Field("salary", DataTypes.FLOAT)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.currentTimeMillis()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer
          .write(Array[String]("true",
            String.valueOf(i),
            String.valueOf(i),
            String.valueOf(i.toDouble / 2),
            String.valueOf(i.toFloat / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  def cleanTestData(): Unit = {
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
    buildTestDataOtherDataType(3, null)
    val exception =
      intercept[SQLException] {
        val actualResult: List[Map[String, Any]] = prestoServer
          .executeQuery("select count(*) as RESULT from files ")
      }
    assert(exception.getMessage()
      .contains("All the files doesn't have same schema"))
    cleanTestData()
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
}