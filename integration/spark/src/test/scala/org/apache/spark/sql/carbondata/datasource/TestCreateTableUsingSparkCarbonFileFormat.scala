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
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataFileFooterConverter}
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter

class TestCreateTableUsingSparkCarbonFileFormat extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    defaultConfig()
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT,
        CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT)
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/");

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
        builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema)).writtenBy("TestCreateTableUsingSparkCarbonFileFormat").build()
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

  //TO DO, need to remove segment dependency and tableIdentifier Dependency
  test("read carbondata files (sdk Writer Output) using the SparkCarbonFileFormat ") {
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
    } else{
      // TO DO
    }

    sql("Describe formatted sdkOutputTable").show(false)

    sql("select * from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable limit 3").show(false)

    sql("select name from sdkOutputTable").show(false)

    sql("select age from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable where age > 2 and age < 8").show(200,false)

    sql("select * from sdkOutputTable where name = 'robot3'").show(200,false)

    sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200,false)

    sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200,false)

    sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200,false)

    sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200,false)

    sql("select count(*) from sdkOutputTable").show(200,false)

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Running SQL directly and read carbondata files (sdk Writer Output) using the SparkCarbonFileFormat ") {
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
    } else {
      // TO DO
    }

    val directSQL = sql(s"""select * FROM  carbon.`$writerPath`""".stripMargin)
    directSQL.show(false)
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
      } else{
        // TO DO
      }
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the table location"))

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


  test("Read sdk writer output file without any file should fail") {
    buildTestData()
    deleteIndexFile(writerPath, CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
    deleteIndexFile(writerPath, CarbonCommonConstants.FACT_FILE_EXT)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[Exception] {
      //data source file format
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        //data source file format
        sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        //data source file format
        sql(
          s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
             |'$writerPath' """.stripMargin)
      } else{
        // TO DO
      }

      sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the table location"))

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read sdk writer output file withSchema") {
    buildTestData()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      //data source file format
      sql(s"""CREATE TABLE sdkOutputTable USING carbon OPTIONS (PATH '$writerPath') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING carbon LOCATION
           |'$writerPath' """.stripMargin)
    } else{
      // TO DO
    }

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

  test("Test complex json nested data with empty array of struct data") {
    val resource = s"$integrationPath/spark/src/test/resources/test_json.json"
    val path = writerPath + "_json"
    FileUtils.deleteDirectory(new File(path))
    val json = sqlContext.read.json(s"$resource")
    json.write.format("carbon").save(s"$path")
    sql("DROP TABLE IF EXISTS test_json")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      sql(s"""CREATE TABLE test_json USING carbon OPTIONS (PATH '$path') """)
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql(
        s"""CREATE TABLE test_json USING carbon LOCATION
           |'$path' """.stripMargin)
    } else {
    }
    assert(sql("select age from test_json").collect().length == 1)
    sql("DROP TABLE IF EXISTS test_json")
    FileUtils.deleteDirectory(new File(path))
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
    } else{
      // TO DO
    }
    //org.apache.spark.SparkException: Index file not present to read the carbondata file
    assert(sql("select * from sdkOutputTable").collect().length == 100)
    assert(sql("select * from sdkOutputTable where name='robot0'").collect().length == 1)

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }
  test("Read data having multi blocklet and validate min max flag") {
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
    checkAnswer(sql("select count(*) from sdkOutputTable"),Seq(Row(800000)))
    checkAnswer(sql(
        "select count(*) from sdkOutputTable where from_email='Email for testing min max for " +
        "allowed chars'"),
      Seq(Row(50000)))
    //expected answer for min max flag. FInally there should be 2 blocklets with one blocklet
    // having min max flag as false for email column and other as true
    val blocklet1MinMaxFlag = Array(true, true, true, true, true, false, true, true, true)
    val blocklet2MinMaxFlag = Array(true, true, true, true, true, true, true, true, true)
    val expectedMinMaxFlag = Array(blocklet1MinMaxFlag, blocklet2MinMaxFlag)
    validateMinMaxFlag(expectedMinMaxFlag, 2)

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).exists())
    clearDataMapCache
    cleanTestData()
  }
  def buildTestDataMuliBlockLet(recordsInBlocklet1 :Int, recordsInBlocklet2 :Int): Unit ={
    FileUtils.deleteDirectory(new File(writerPath))
    val fields=new Array[Field](8)
    fields(0)=new Field("myid",DataTypes.INT)
    fields(1)=new Field("event_id",DataTypes.STRING)
    fields(2)=new Field("eve_time",DataTypes.DATE)
    fields(3)=new Field("ingestion_time",DataTypes.TIMESTAMP)
    fields(4)=new Field("alldate",DataTypes.createArrayType(DataTypes.DATE))
    fields(5)=new Field("subject",DataTypes.STRING)
    fields(6)=new Field("from_email",DataTypes.STRING)
    fields(7)=new Field("sal",DataTypes.DOUBLE)
    import scala.collection.JavaConverters._
    val emailDataBlocklet1 = "FromEmail"
    val emailDataBlocklet2 = "Email for testing min max for allowed chars"
    try{
      val options=Map("bad_records_action"->"FORCE","complex_delimiter_level_1"->"$").asJava
      val writer = CarbonWriter.builder().outputPath(writerPath).withBlockletSize(16)
        .sortBy(Array("myid", "ingestion_time", "event_id")).withLoadOptions(options)
        .withCsvInput(new Schema(fields)).writtenBy("TestCreateTableUsingSparkCarbonFileFormat").build()
      val timeF=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date_F=new SimpleDateFormat("yyyy-MM-dd")
      for(i<- 1 to recordsInBlocklet1){
        val time=new Date(System.currentTimeMillis())
        writer.write(Array(""+i,"event_"+i,""+date_F.format(time),""+timeF.format(time),""+date_F.format(time)+"$"+date_F.format(time),"Subject_0",emailDataBlocklet1,""+new Random().nextDouble()))
      }
      for(i<- 1 to recordsInBlocklet2){
        val time=new Date(System.currentTimeMillis())
        writer.write(Array(""+i,"event_"+i,""+date_F.format(time),""+timeF.format(time),""+date_F.format(time)+"$"+date_F.format(time),"Subject_0",emailDataBlocklet2,""+new Random().nextDouble()))
      }
      writer.close()
    }
  }

  /**
   * read carbon index file and  validate the min max flag written in each blocklet
   *
   * @param expectedMinMaxFlag
   * @param numBlocklets
   */
  private def validateMinMaxFlag(expectedMinMaxFlag: Array[Array[Boolean]],
      numBlocklets: Int): Unit = {
    val carbonFiles: Array[File] = new File(writerPath).listFiles()
    val carbonIndexFile = carbonFiles.filter(file => file.getName.endsWith(".carbonindex"))(0)
    val converter: DataFileFooterConverter = new DataFileFooterConverter(sqlContext.sessionState
      .newHadoopConf())
    val carbonIndexFilePath = FileFactory.getUpdatedFilePath(carbonIndexFile.getCanonicalPath)
    val indexMetadata: List[DataFileFooter] = converter
      .getIndexInfo(carbonIndexFilePath, null, false).asScala.toList
    assert(indexMetadata.size == numBlocklets)
    indexMetadata.zipWithIndex.foreach { filefooter =>
      val isMinMaxSet: Array[Boolean] = filefooter._1.getBlockletIndex.getMinMaxIndex.getIsMinMaxSet
      assert(isMinMaxSet.sameElements(expectedMinMaxFlag(filefooter._2)))
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
      .append("   {\"  address    \":\"varchar\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"note\":\"varchar\"}\n")
      .append("]")
      .toString()
    val builder = CarbonWriter.builder()
    val writer = builder.outputPath(writerPath).withCsvInput(Schema.parseJson(schema)).writtenBy("TestCreateTableUsingSparkCarbonFileFormat").build()
    val totalRecordsNum = 3
    for (i <- 0 until totalRecordsNum) {
      // write a varchar with 75,000 length
      writer.write(Array[String](s"name_$i", RandomStringUtils.randomAlphabetic(75000), i.toString, RandomStringUtils.randomAlphabetic(75000)))
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
        s"""CREATE TABLE sdkOutputTable (name string, address string, age int, note string) USING carbon
           |OPTIONS("long_String_columns"="address, note") LOCATION
           |'$writerPath' """.stripMargin)
    }
    checkAnswer(sql("select count(*) from sdkOutputTable where age = 0"), Seq(Row(1)))
    checkAnswer(sql("SELECT COUNT(*) FROM (select address,age,note from sdkOutputTable where length(address)=75000 and length(note)=75000)"),
      Seq(Row(totalRecordsNum)))
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
    checkAnswer(sql("SELECT COUNT(*) FROM (select address,age,note from sdkOutputTableWithoutSchema where length(address)=75000 and length(note)=75000)"),
      Seq(Row(totalRecordsNum)))
    sql("DROP TABLE sdkOutputTableWithoutSchema")
    clearDataMapCache
    cleanTestData()
  }
}
