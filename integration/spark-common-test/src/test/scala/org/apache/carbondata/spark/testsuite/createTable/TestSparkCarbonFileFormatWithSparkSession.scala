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
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}


object TestSparkCarbonFileFormatWithSparkSession {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
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
      val builder = CarbonWriter.builder()
      val writer =
        if (persistSchema) {
          builder.persistSchemaFile(true)
          builder.withSchema(Schema.parseJson(schema)).outputPath(writerPath).buildWriterForCSVInput()
        } else {
          builder.withSchema(Schema.parseJson(schema)).outputPath(writerPath).buildWriterForCSVInput()
        }

      var i = 0
      while (i < 100) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => None
      case _ => None
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

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metastoredb)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestSparkCarbonFileFormatWithSparkSession")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastoredb;create=true")
      .getOrCreate()

    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", storeLocation)

    spark.sparkContext.setLogLevel("WARN")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    buildTestData(false)
    assert(new File(filePath).exists())
    //data source file format
    if (spark.sparkContext.version.startsWith("2.1")) {
      //data source file format
      spark.sql(s"""CREATE TABLE sdkOutputTable USING carbonfile OPTIONS (PATH '$filePath') """)
    } else if (spark.sparkContext.version.startsWith("2.2")) {
      //data source file format
      spark.sql(
        s"""CREATE TABLE sdkOutputTable USING carbonfile LOCATION
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

    spark.stop()
  }
}