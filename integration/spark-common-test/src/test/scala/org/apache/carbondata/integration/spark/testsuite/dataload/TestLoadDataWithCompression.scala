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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File
import java.text.SimpleDateFormat
import java.util.concurrent.{ExecutorService, Executors, Future}
import java.util.Calendar

import scala.util.Random

import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{CarbonEnv, Row, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

case class Rcd(booleanField: Boolean, shortField: Short, intField: Int, bigintField: Long,
    doubleField: Double, stringField: String, timestampField: String, decimalField: Double,
    dateField: String, charField: String, floatField: Float, stringDictField: String,
    stringSortField: String, stringLocalDictField: String, longStringField: String)

class TestLoadDataWithCompression extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  private val tableName = "load_test_with_compressor"
  private var executorService: ExecutorService = _
  private val csvDataDir = s"$integrationPath/spark2/target/csv_load_compression"

  override protected def beforeAll(): Unit = {
    executorService = Executors.newFixedThreadPool(3)
    CarbonUtil.deleteFoldersAndFilesSilent(FileFactory.getCarbonFile(csvDataDir))
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  override protected def afterAll(): Unit = {
    executorService.shutdown()
    CarbonUtil.deleteFoldersAndFilesSilent(FileFactory.getCarbonFile(csvDataDir))
    try {
      sql(s"DROP TABLE IF EXISTS $tableName")
    } catch {
      case _: Exception =>
    }
  }

  override protected def afterEach(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR,
      CarbonCommonConstants.DEFAULT_COMPRESSOR)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
      CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)

    try {
      sql(s"DROP TABLE IF EXISTS $tableName")
    } catch {
      case _: Exception =>
    }
  }

  private def createTable(streaming: Boolean = false, columnCompressor: String = ""): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float,
         |    stringDictField string,
         |    stringSortField string,
         |    stringLocalDictField string,
         |    longStringField string
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         |  ${if (StringUtils.isBlank(columnCompressor)) "" else s"'${CarbonCommonConstants.COMPRESSOR}'='$columnCompressor',"}
         |  ${if (streaming) "" else s"'LONG_STRING_COLUMNS'='longStringField',"}
         |  'SORT_COLUMNS'='stringSortField',
         |  'DICTIONARY_INCLUDE'='stringDictField',
         |  'local_dictionary_enable'='true',
         |  'local_dictionary_threshold'='10000',
         |  'local_dictionary_include'='stringLocalDictField' ${if (streaming) s", 'STREAMING'='true'" else ""})
       """.stripMargin)
  }

  private def loadData(): Unit = {
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,1,11,101,41.4,'string1','2015/4/23 12:01:01',12.34,'2015/4/23','aaa',1.5,'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015/5/23 12:01:03',23.45,'2015/5/23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,3,13,163,43.4,'string3','2015/7/26 12:01:06',34.56,'2015/7/26','ccc',3.5,'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $tableName VALUES
         |  (true,${Short.MaxValue - 2},${Int.MinValue + 2},${Long.MaxValue - 2},${Double.MinValue + 2},'string1','2015/4/23 12:01:01',${Double.MinValue + 2},'2015/4/23','aaa',${Float.MaxValue - 2},'dict1','sort1','local_dict1','longstring1'),
         | (false,2,12,102,42.4,'string2','2015/5/23 12:01:03',23.45,'2015/5/23','bbb',2.5,'dict2','sort2','local_dict2','longstring2'),
         |  (true,${Short.MinValue + 2},${Int.MaxValue - 2},${Long.MinValue + 2},${Double.MaxValue - 2},'string3','2015/7/26 12:01:06',${Double.MinValue + 2},'2015/7/26','ccc',${Float.MinValue + 2},'dict3','sort3','local_dict3','longstring3'),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
  }

  test("test data loading with snappy compressor and offheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    createTable()
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(8)))
  }

  test("test data loading with zstd compressor and offheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(8)))
  }

  test("test data loading with zstd compressor and onheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(8)))
  }

  test("test current zstd compressor on legacy store with snappy") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    createTable()
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(16)))
  }

  test("test current snappy compressor on legacy store with zstd") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(16)))
  }

  test("test compaction with different compressor for each load") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    createTable()
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    loadData()

    // there are 8 loads
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(4 * 8)))
    assert(sql(s"SHOW SEGMENTS FOR TABLE $tableName").count() == 8)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")
    sql(s"CLEAN FILES FOR TABLE $tableName")
    // after compaction and clean, there should be on segment
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(4 * 8)))
    assert(sql(s"SHOW SEGMENTS FOR TABLE $tableName").count() == 1)
  }

  test("test data loading with unsupported compressor and onheap") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "fake")
    createTable()
    val exception = intercept[UnsupportedOperationException] {
      loadData()
    }
    assert(exception.getMessage.contains("Invalid compressor type provided"))
  }

  test("test compaction with unsupported compressor") {
    createTable()
    loadData()
    loadData()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "fake")
    val exception = intercept[UnsupportedOperationException] {
      sql(s"ALTER TABLE $tableName COMPACT 'major'")
    }
    assert(exception.getMessage.contains("Invalid compressor type provided"))
  }

  private def generateAllDataTypeDF(lineNum: Int) = {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to lineNum)
      .map { p =>
        calendar.add(Calendar.HOUR, p)
        Rcd(Random.nextBoolean(), (Random.nextInt() % Short.MaxValue).toShort, Random.nextInt(), Random.nextLong(),
          Random.nextDouble(), Random.nextString(6), tsFormat.format(calendar.getTime), 0.01 * p,
          dateFormat.format(calendar.getTime), s"$p", Random.nextFloat(), s"stringDict$p",
          s"stringSort$p", s"stringLocalDict$p", RandomStringUtils.randomAlphabetic(33000))
      }
      .toDF()
      .cache()
  }

  test("test data loading & compaction with more pages and change the compressor during loading") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "2000")
    val lineNum = 5000
    val df = generateAllDataTypeDF(lineNum)

    def loadDataAsync(): Future[_] = {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          df.write
            .format("carbondata")
            .option("tableName", tableName)
            .mode(SaveMode.Append)
            .save()
        }
      })
    }

    createTable()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    var future = loadDataAsync()
    // change the compressor randomly during the loading
    while (!future.isDone) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, if (Random.nextBoolean()) "snappy" else "zstd")
    }

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    future = loadDataAsync()
    while (!future.isDone) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, if (Random.nextBoolean()) "snappy" else "zstd")
    }

    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(lineNum * 2)))
    checkAnswer(sql(s"SELECT stringDictField, stringSortField FROM $tableName WHERE stringDictField='stringDict1'"), Seq(Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1")))

    def compactAsync(): Future[_] = {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          sql(s"ALTER TABLE $tableName COMPACT 'MAJOR'")
        }
      })
    }

    // change the compressor randomly during compaction
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    future = compactAsync()
    while (!future.isDone) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, if (Random.nextBoolean()) "snappy" else "zstd")
    }

    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(lineNum * 2)))
    checkAnswer(sql(s"SELECT stringDictField, stringSortField FROM $tableName WHERE stringDictField='stringDict1'"), Seq(Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1")))
  }

  test("test creating table with specified compressor") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    // the system configuration for compressor is snappy
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    // create table with zstd as compressor
    createTable(columnCompressor = "zstd")
    loadData()
    checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(8)))
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), tableName)(sqlContext.sparkSession)
    val tableColumnCompressor = carbonTable.getTableInfo.getFactTable.getTableProperties.get(CarbonCommonConstants.COMPRESSOR)
    assert("zstd".equalsIgnoreCase(tableColumnCompressor))
  }

  test("test creating table with unsupported compressor") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    // the system configuration for compressor is snappy
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    // create table with unsupported compressor
    val exception = intercept[InvalidConfigurationException] {
      createTable (columnCompressor = "fakecompressor")
    }
    assert(exception.getMessage.contains("fakecompressor compressor is not supported"))
  }

  private def generateAllDataTypeFiles(lineNum: Int, csvDir: String,
      saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to lineNum)
      .map { p =>
        calendar.add(Calendar.HOUR, p)
        Rcd(Random.nextBoolean(), (Random.nextInt() % Short.MaxValue / 2).toShort, Random.nextInt(), Random.nextLong(),
          Random.nextDouble(), RandomStringUtils.randomAlphabetic(6), tsFormat.format(calendar.getTime), 0.01 * p,
          dateFormat.format(calendar.getTime), s"$p", Random.nextFloat(), s"stringDict$p",
          s"stringSort$p", s"stringLocalDict$p", RandomStringUtils.randomAlphabetic(3))
      }
      .toDF()
      .write
      .option("header", "false")
      .mode(saveMode)
      .csv(csvDir)
  }

  ignore("test streaming ingestion with different compressor for each mini-batch") {
    createTable(streaming = true)
    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), tableName)(sqlContext.sparkSession)
    val lineNum = 10
    val dataLocation = new File(csvDataDir).getCanonicalPath

    def doStreamingIngestionThread(): Thread = {
      new Thread() {
        override def run(): Unit = {
          var streamingQuery: StreamingQuery = null
          try {
            val streamingQuery = sqlContext.sparkSession.readStream
              .text(dataLocation)
              .writeStream
              .format("carbondata")
              .trigger(ProcessingTime(s"1 seconds"))
              .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
              .option("dbName", "default")
              .option("tableName", tableName)
              .option(CarbonStreamParser.CARBON_STREAM_PARSER, CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
              .start()
            streamingQuery.awaitTermination()
          } catch {
            case ex: Exception => LOGGER.error(ex)
          } finally {
            streamingQuery.stop()
          }
        }
      }
    }

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    generateAllDataTypeFiles(lineNum, dataLocation)
    val thread = doStreamingIngestionThread()
    thread.start()
    Thread.sleep(10 * 1000)

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    generateAllDataTypeFiles(lineNum, dataLocation, SaveMode.Append)
    Thread.sleep(10 * 1000)

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "zstd")
    generateAllDataTypeFiles(lineNum, dataLocation, SaveMode.Append)
    Thread.sleep(10 * 1000)

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "snappy")
    generateAllDataTypeFiles(lineNum, dataLocation, SaveMode.Append)
    Thread.sleep(40 * 1000)
    thread.interrupt()
    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(lineNum * 4)))
    checkAnswer(sql(s"SELECT stringDictField, stringSortField FROM $tableName WHERE stringDictField='stringDict1'"),
      Seq(Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1")))

    sql(s"alter table $tableName compact 'streaming'")

    checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(lineNum * 4)))
    checkAnswer(sql(s"SELECT stringDictField, stringSortField FROM $tableName WHERE stringDictField='stringDict1'"),
      Seq(Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1"), Row("stringDict1", "stringSort1")))
    try {
      sql(s"DROP TABLE IF EXISTS $tableName")
    } catch {
      case _: Exception =>
    }
  }
}
