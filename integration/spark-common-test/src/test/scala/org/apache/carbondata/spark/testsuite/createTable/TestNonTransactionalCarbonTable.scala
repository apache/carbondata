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

import java.io.{File, FileFilter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}


class TestNonTransactionalCarbonTable extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
                            +
                            "../." +
                            "./src/test/resources/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
  writerPath = writerPath.replace("\\", "/");

  def buildTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3,false)
  }

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(1000000,false)
  }

  def buildTestDataTwice(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3,false)
    buildTestData(3,false)
  }

  // prepare sdk writer output
  def buildTestData(rows:Int, persistSchema:Boolean): Any = {
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
            .outputPath(writerPath)
            .isTransactionalTable(false)
            .uniqueIdentifier(System.currentTimeMillis)
            .buildWriterForCSVInput()
        } else {
          builder.withSchema(Schema.parseJson(schema))
            .outputPath(writerPath)
            .isTransactionalTable(false)
            .uniqueIdentifier(System.currentTimeMillis).withBlockSize(2)
            .buildWriterForCSVInput()
        }
      var i = 0
      while (i < rows) {
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
    buildTestData(3, false)
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
    buildTestData(3, false)
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
    buildTestData(3, false)
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

    // drop table should not delete the files
    assert(new File(writerPath).exists())
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

    // drop table should not delete the files
    assert(new File(writerPath).exists())
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

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }

  test("Read two sdk writer outputs with same column name placed in same folder") {
    buildTestDataTwice()
    assert(new File(writerPath).exists())

    sql("DROP TABLE IF EXISTS sdkOutputTable")

    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)


    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0),
      Row("robot0", 0, 0.0),
      Row("robot1", 1, 0.5),
      Row("robot2", 2, 1.0)))

    // drop table should not delete the files
    assert(new File(writerPath).exists())
    cleanTestData()
  }


}
