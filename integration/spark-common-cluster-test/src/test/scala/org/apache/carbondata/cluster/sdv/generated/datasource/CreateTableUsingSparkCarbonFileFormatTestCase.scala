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

package org.apache.carbondata.cluster.sdv.generated.datasource

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.util.SparkUtil
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

class CreateTableUsingSparkCarbonFileFormatTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  val writerPath = s"${TestQueryExecutor.projectPath}/integration/spark-common-cluster-test/src/test/resources/SparkCarbonFileFormat/WriterOutput/"

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
        builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema))
          .writtenBy("CreateTableUsingSparkCarbonFileFormatTestCase").build()
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

  def deleteIndexFile(path: String, extension: String): Unit = {
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

  test(
    "Running SQL directly and read carbondata files (sdk Writer Output) using the " +
    "SparkCarbonFileFormat ") {
    buildTestData()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$writerPath' """.stripMargin)
    }

    val directSQL = sql(s"""select * FROM  carbon.`$writerPath`""".stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), directSQL)

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
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        //data source file format
        sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        //data source file format
        sql(
          s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
             |'$writerPath' """.stripMargin)
      }
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the table location"))

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file without index file should not fail") {
    buildTestData()
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$writerPath' """.stripMargin)
    }
    //org.apache.spark.SparkException: Index file not present to read the carbondata file
    assert(sql("select * from sdkOutputTable").collect().length == 100)
    assert(sql("select * from sdkOutputTable where name='robot0'").collect().length == 1)

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }
  test("Read data having multi blocklet") {
    buildTestDataMuliBlockLet(750000, 50000)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
    } else {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$writerPath' """.stripMargin)
    }
    checkAnswer(sql("select count(*) from sdkOutputTable"), Seq(Row(800000)))
    checkAnswer(sql(
      "select count(*) from sdkOutputTable where from_email='Email for testing min max for " +
      "allowed chars'"),
      Seq(Row(50000)))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    clearDataMapCache
    cleanTestData()
  }

  def buildTestDataMuliBlockLet(recordsInBlocklet1: Int, recordsInBlocklet2: Int): Unit = {
    FileUtils.deleteDirectory(new File(writerPath))
    val fields = new Array[Field](8)
    fields(0) = new Field("myid", DataTypes.INT)
    fields(1) = new Field("event_id", DataTypes.STRING)
    fields(2) = new Field("eve_time", DataTypes.DATE)
    fields(3) = new Field("ingestion_time", DataTypes.TIMESTAMP)
    fields(4) = new Field("alldate", DataTypes.createArrayType(DataTypes.DATE))
    fields(5) = new Field("subject", DataTypes.STRING)
    fields(6) = new Field("from_email", DataTypes.STRING)
    fields(7) = new Field("sal", DataTypes.DOUBLE)
    import scala.collection.JavaConverters._
    val emailDataBlocklet1 = "FromEmail"
    val emailDataBlocklet2 = "Email for testing min max for allowed chars"
    try {
      val options = Map("bad_records_action" -> "FORCE", "complex_delimiter_level_1" -> "$").asJava
      val writer = CarbonWriter.builder().outputPath(writerPath).withBlockletSize(16)
        .sortBy(Array("myid", "ingestion_time", "event_id")).withLoadOptions(options)
        .withCsvInput(new Schema(fields)).writtenBy("CreateTableUsingSparkCarbonFileFormatTestCase")
        .build()
      val timeF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date_F = new SimpleDateFormat("yyyy-MM-dd")
      for (i <- 1 to recordsInBlocklet1) {
        val time = new Date(System.currentTimeMillis())
        writer
          .write(Array("" + i,
            "event_" + i,
            "" + date_F.format(time),
            "" + timeF.format(time),
            "" + date_F.format(time) + "$" + date_F.format(time),
            "Subject_0",
            emailDataBlocklet1,
            "" + new Random().nextDouble()))
      }
      for (i <- 1 to recordsInBlocklet2) {
        val time = new Date(System.currentTimeMillis())
        writer
          .write(Array("" + i,
            "event_" + i,
            "" + date_F.format(time),
            "" + timeF.format(time),
            "" + date_F.format(time) + "$" + date_F.format(time),
            "Subject_0",
            emailDataBlocklet2,
            "" + new Random().nextDouble()))
      }
      writer.close()
    }
  }

  private def clearDataMapCache(): Unit = {
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(writerPath))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    }
  }

  test("Test with long string columns") {
    FileUtils.deleteDirectory(new File(writerPath))
    // here we specify the long string column as varchar
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"address\":\"varchar\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"note\":\"varchar\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema))
      .writtenBy("CreateTableUsingSparkCarbonFileFormatTestCase").build()
    val totalRecordsNum = 3
    for (i <- 0 until totalRecordsNum) {
      // write a varchar with 75,000 length
      writer
        .write(Array[String](s"name_$i",
          RandomStringUtils.randomAlphabetic(75000),
          i.toString,
          RandomStringUtils.randomAlphabetic(75000)))
    }
    writer.close()

    //--------------- data source external table with schema ---------------------------
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    if (sqlContext.sparkContext.version.startsWith("2.1")) {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable (name string, address string, age int, note string)
           |USING carbon OPTIONS (PATH '$writerPath', "long_String_columns" "address, note") """
          .stripMargin)
    } else {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable (name string, address string, age int, note string) USING
           | carbon
           |OPTIONS("long_String_columns"="address, note") LOCATION
           |'$writerPath' """.stripMargin)
    }
    checkAnswer(sql("select count(*) from sdkOutputTable where age = 0"), Seq(Row(1)))
    checkAnswer(sql(
      "SELECT COUNT(*) FROM (select address,age,note from sdkOutputTable where length(address)" +
      "=75000 and length(note)=75000)"),
      Seq(Row(totalRecordsNum)))
    checkAnswer(sql("select name from  sdkOutputTable where age = 2"), Seq(Row("name_2")))
    sql("DROP TABLE sdkOutputTable")

    //--------------- data source external table without schema ---------------------------
    sql("DROP TABLE IF EXISTS sdkOutputTableWithoutSchema")
    if (sqlContext.sparkContext.version.startsWith("2.1")) {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTableWithoutSchema USING carbon OPTIONS (PATH
           |'$writerPath', "long_String_columns" "address, note") """.stripMargin)
    } else {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTableWithoutSchema USING carbon OPTIONS
           |("long_String_columns"="address, note") LOCATION '$writerPath' """.stripMargin)
    }
    checkAnswer(sql("select count(*) from sdkOutputTableWithoutSchema where age = 0"), Seq(Row(1)))
    checkAnswer(sql(
      "SELECT COUNT(*) FROM (select address,age,note from sdkOutputTableWithoutSchema where " +
      "length(address)=75000 and length(note)=75000)"),
      Seq(Row(totalRecordsNum)))
    checkAnswer(sql("select name from sdkOutputTableWithoutSchema where age = 2"),
      Seq(Row("name_2")))
    sql("DROP TABLE sdkOutputTableWithoutSchema")
    clearDataMapCache
    cleanTestData()
  }
}
