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

package org.apache.spark.sql.carbondata.datasource

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.util.SparkUtil
import org.apache.spark.sql.carbondata.datasource.TestUtil.{spark, _}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}

class TestCreateTableUsingSparkCarbonFileFormat extends FunSuite with BeforeAndAfterAll {



  override def beforeAll(): Unit = {
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/");

  val filePath = writerPath + "/Fact/Part0/Segment_null/"

  def buildTestData(persistSchema:Boolean) = {

    FileUtils.deleteDirectory(new File(writerPath))

    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder().isTransactionalTable(true)
      val writer =
        if (persistSchema) {
          builder.persistSchemaFile(true)
          builder.outputPath(writerPath).buildWriterForCSVInput(Schema.parseJson(schema), spark
            .sparkContext.hadoopConfiguration)
        } else {
          builder.outputPath(writerPath).buildWriterForCSVInput(Schema.parseJson(schema), spark
            .sparkContext.hadoopConfiguration)
        }

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
    val file: CarbonFile = FileFactory
      .getCarbonFile(path, FileFactory.getFileType(path))

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

  //TO DO, need to remove segment dependency and tableIdentifier Dependency
  test("read carbondata files (sdk Writer Output) using the SparkCarbonFileFormat ") {
    buildTestData(false)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$filePath' """.stripMargin)
    } else{
      // TO DO
    }

    spark.sql("Describe formatted sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable limit 3").show(false)

    spark.sql("select name from sdkOutputTable").show(false)

    spark.sql("select age from sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable where age > 2 and age < 8").show(200,false)

    spark.sql("select * from sdkOutputTable where name = 'robot3'").show(200,false)

    spark.sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200,false)

    spark.sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200,false)

    spark.sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200,false)

    spark.sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200,false)

    spark.sql("select count(*) from sdkOutputTable").show(200,false)

    spark.sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }

  test("Running SQL directly and read carbondata files (sdk Writer Output) using the SparkCarbonFileFormat ") {
    buildTestData(false)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$filePath' """.stripMargin)
    } else {
      // TO DO
    }

    val directSQL = spark.sql(s"""select * FROM  carbon.`$filePath`""".stripMargin)
    directSQL.show(false)
    TestUtil.checkAnswer(spark.sql("select * from sdkOutputTable"), directSQL)

    spark.sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }

  // TODO: Make the sparkCarbonFileFormat to work without index file
  test("Read sdk writer output file without Carbondata file should fail") {
    buildTestData(false)
    deleteIndexFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //    data source file format
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        //data source file format
        spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        //data source file format
        spark.sql(
          s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
             |'$filePath' """.stripMargin)
      } else{
        // TO DO
      }
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the table location"))

    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }


  test("Read sdk writer output file without any file should fail") {
    buildTestData(false)
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    deleteIndexFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //data source file format
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        //data source file format
        spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        //data source file format
        spark.sql(
          s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
             |'$filePath' """.stripMargin)
      } else{
        // TO DO
      }

      spark.sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the table location"))

    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file withSchema") {
    buildTestData(true)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$filePath' """.stripMargin)
    } else{
      // TO DO
    }

    spark.sql("Describe formatted sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable limit 3").show(false)

    spark.sql("select name from sdkOutputTable").show(false)

    spark.sql("select age from sdkOutputTable").show(false)

    spark.sql("select * from sdkOutputTable where age > 2 and age < 8").show(200, false)

    spark.sql("select * from sdkOutputTable where name = 'robot3'").show(200, false)

    spark.sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200, false)

    spark.sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200, false)

    spark.sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200, false)

    spark.sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200, false)

    spark.sql("select count(*) from sdkOutputTable").show(200, false)

    spark.sql("DROP TABLE sdkOutputTable")

    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file without index file should not fail") {
    buildTestData(false)
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(new File(filePath).exists())
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")

    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      spark.sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$filePath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$filePath' """.stripMargin)
    } else{
      // TO DO
    }
    //org.apache.spark.SparkException: Index file not present to read the carbondata file
    assert(spark.sql("select * from sdkOutputTable").collect().length == 100)
    assert(spark.sql("select * from sdkOutputTable where name='robot0'").collect().length == 1)

    spark.sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(filePath).exists())
    cleanTestData()
  }

  test("Test with long string columns") {
    FileUtils.deleteDirectory(new File(writerPath))
    // here we specify the long string column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"address\":\"varchar\"},\n")
      .append("   {\"age\":\"int\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath)
      .buildWriterForCSVInput(Schema.parseJson(schema), spark.sessionState.newHadoopConf())
    for (i <- 0 until 3) {
      // write a varchar with 75,000 length
      writer.write(Array[String](s"name_$i", RandomStringUtils.randomAlphabetic(75000), i.toString))
    }
    writer.close()

    //--------------- data source external table with schema ---------------------------
    spark.sql("DROP TABLE IF EXISTS sdkOutputTable")
    if (spark.sparkContext.version.startsWith("2.1")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable (name string, address string, age int)
           |USING carbon OPTIONS (PATH '$writerPath', "long_String_columns" "address") """
          .stripMargin)
    } else {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable (name string, address string, age int) USING carbon
           |OPTIONS("long_String_columns"="address") LOCATION
           |'$writerPath' """.stripMargin)
    }
    assert(spark.sql("select * from sdkOutputTable where age = 0").count() == 1)
    val op = spark.sql("select address from sdkOutputTable limit 1").collectAsList()
    assert(op.get(0).getString(0).length == 75000)
    spark.sql("DROP TABLE sdkOutputTable")

    //--------------- data source external table without schema ---------------------------
    spark.sql("DROP TABLE IF EXISTS sdkOutputTableWithoutSchema")
    if (spark.sparkContext.version.startsWith("2.1")) {
      //data source file format
      spark
        .sql(
          s"""CREATE TABLE sdkOutputTableWithoutSchema USING carbon OPTIONS (PATH
             |'$writerPath', "long_String_columns" "address") """.stripMargin)
    } else {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTableWithoutSchema USING carbon OPTIONS
           |("long_String_columns"="address") LOCATION '$writerPath' """.stripMargin)
    }
    assert(spark.sql("select * from sdkOutputTableWithoutSchema where age = 0").count() == 1)
    val op1 = spark.sql("select address from sdkOutputTableWithoutSchema limit 1").collectAsList()
    assert(op1.get(0).getString(0).length == 75000)
    spark.sql("DROP TABLE sdkOutputTableWithoutSchema")
    cleanTestData()
  }
}
