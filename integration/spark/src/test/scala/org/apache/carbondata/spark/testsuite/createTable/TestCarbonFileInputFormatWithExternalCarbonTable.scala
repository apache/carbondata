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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}

class TestCarbonFileInputFormatWithExternalCarbonTable extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath +
                            "../../src/test/resources/SparkCarbonFileFormat/WriterOutput")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")


  def buildTestData(): Any = {

    FileUtils.deleteDirectory(new File(writerPath))

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
          .withCsvInput(Schema.parseJson(schema)).writtenBy("TestCarbonFileInputFormatWithExternalCarbonTable").build()

      var i = 0
      while (i < 100) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case _: Throwable => None
    }
  }

  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
  }

  def deleteIndexFile(path: String, extension: String) : Unit = {
    val file: CarbonFile = FileFactory.getCarbonFile(path)
    for (eachDir <- file.listFiles) {
      if (!eachDir.isDirectory) {
        if (eachDir.getName.endsWith(extension)) {
          CarbonUtil.deleteFoldersAndFilesSilent(eachDir)
        }
      } else {
        deleteIndexFile(eachDir.getPath, extension)
      }
    }
  }

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // create carbon table and insert data
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  //TO DO, need to remove segment dependency and tableIdentifier Dependency
  test("read carbondata files (sdk Writer Output) using the carbonfile ") {
    buildTestData()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //new provider carbonfile
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbon LOCATION
         |'$writerPath' """.stripMargin)

    sql("Describe formatted sdkOutputTable").show(false)

    sql("select * from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable limit 3").show(false)

    sql("select name from sdkOutputTable").show(false)

    sql("select age from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable where age > 2 and age < 8").show(200, false)

    sql("select * from sdkOutputTable where name = 'robot3'").show(200, false)

    sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200, false)

    sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200, false)

    sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200, false)

    sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200, false)

    sql("select count(*) from sdkOutputTable").show(200, false)

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("should not allow to alter datasource carbontable ") {
    buildTestData()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbon LOCATION
         |'$writerPath' """.stripMargin)

    val exception = intercept[AnalysisException]
    {
      sql("Alter table sdkOutputTable change age age BIGINT")
    }
    assert(exception.getMessage().contains(
      "ALTER TABLE CHANGE COLUMN is not supported for changing column 'age' with type 'IntegerType' to 'age' with type 'LongType'"))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file without index file should fail") {
    buildTestData()
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbon LOCATION
         |'$writerPath' """.stripMargin)

    //org.apache.spark.SparkException: Index file not present to read the carbondata file
    checkAnswer(sql("select count(*) from sdkOutputTable"), Seq(Row(100)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  // TODO: Make the sparkCarbonFileFormat to work without index file
  test("Read sdk writer output file without Carbondata file should fail") {
    buildTestData()
    deleteIndexFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //    data source file format
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbon LOCATION
         |'$writerPath' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("Unable to infer the schema"))


    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("Read sdk writer output file without any file should fail") {
    buildTestData()
    deleteIndexFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //data source file format
      sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbon LOCATION
         |'$writerPath' """.stripMargin)

      sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
        .contains("Unable to infer the schema"))

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }
}
