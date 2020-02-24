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

package org.apache.spark.carbondata

import java.io.{File, PrintWriter}
import java.math.BigDecimal
import java.net.{BindException, ServerSocket}
import java.sql.{Date, Timestamp}

import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

case class StreamLongStrData(id: Integer, name: String, city: String, salary: java.lang.Float,
    tax: BigDecimal, percent: java.lang.Double, birthday: String,
    register: String, updated: String, longStr: String,
    file: FileElement)

class TestStreamingTableWithLongString extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample_with_long_string.csv"
  private val csvDataDir = integrationPath + "/spark/target/csvdata_longstr"
  private val longStrValue = "abc" * 12000

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP DATABASE IF EXISTS streaming_longstr CASCADE")
    sql("CREATE DATABASE streaming_longstr")
    sql("USE streaming_longstr")

    dropTable()

    // 1. streaming table with long string field
    // socket source
    createTable(tableName = "stream_table_longstr", streaming = true, withBatchLoad = true)

    // 2. streaming table with long string field
    // file source
    createTable(tableName = "stream_table_longstr_file", streaming = true, withBatchLoad = true)

    // 3. streaming table with long string and complex field
    createTableWithComplexType(
      tableName = "stream_table_longstr_complex", streaming = true, withBatchLoad = true)
  }

  override def afterAll {
    dropTable()
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming_longstr CASCADE")
    new File(csvDataDir).delete()
  }

  def dropTable(): Unit = {
    sql("drop table if exists streaming_longstr.stream_table_longstr")
    sql("drop table if exists streaming_longstr.stream_table_longstr_file")
    sql("drop table if exists streaming_longstr.stream_table_longstr_complex")
  }

  // input source: file
  test("[CARBONDATA-3497] Support to write long string for streaming table: ingest from file source") {
    val identifier = new TableIdentifier("stream_table_longstr_file", Option("streaming_longstr"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].carbonTable
    // streaming ingest 10 rows
    generateCSVDataFile(spark, idStart = 10, rowNums = 10, csvDataDir)
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(3000)
    generateCSVDataFile(spark, idStart = 30, rowNums = 10, csvDataDir)
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming_longstr.stream_table_longstr_file"),
      Seq(Row(25))
    )

    val row = sql("select * from streaming_longstr.stream_table_longstr_file order by id").head()
    val exceptedRow = Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), "10" + longStrValue)
    assertResult(exceptedRow)(row)
    new File(csvDataDir).delete()
  }

  test("[CARBONDATA-3497] Support to write long string for streaming table") {
    executeStreamingIngest(
      tableName = "stream_table_longstr",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      handoffSize = 51200,
      autoHandoff = false
    )

    var result = sql("select * from streaming_longstr.stream_table_longstr order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(1).getString(1) == "name_2")
    assert(result(1).getString(9) == ("2" + longStrValue))
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")
    assert(result(50).getString(9) == ("1" + longStrValue))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    sql("show segments for table streaming_longstr.stream_table_longstr").show(20, false)
    sql("alter table streaming_longstr.stream_table_longstr finish streaming")
    sql("alter table streaming_longstr.stream_table_longstr compact 'streaming'")
    sql("show segments for table streaming_longstr.stream_table_longstr").show(20, false)
    Thread.sleep(5000)

    result = sql("select * from streaming_longstr.stream_table_longstr order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(2).getString(1) == "name_3")
    assert(result(2).getString(9) == ("3" + longStrValue))
    // check one row of batch loading
    assert(result(51).getInt(0) == 100000002)
    assert(result(51).getString(1) == "batch_2")
    assert(result(51).getString(9) == ("2" + longStrValue))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    sql("alter table streaming_longstr.stream_table_longstr compact 'major'")
    sql("show segments for table streaming_longstr.stream_table_longstr").show(20, false)
    Thread.sleep(5000)

    result = sql("select * from streaming_longstr.stream_table_longstr order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(3).getString(1) == "name_4")
    assert(result(3).getString(9) == ("4" + longStrValue))
    // check one row of batch loading
    assert(result(52).getInt(0) == 100000003)
    assert(result(52).getString(1) == "batch_3")
    assert(result(52).getString(9) == ("3" + longStrValue))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue))))
  }

  test("[CARBONDATA-3497] Support to write long string for streaming table: include complex column") {
    executeStreamingIngest(
      tableName = "stream_table_longstr_complex",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      handoffSize = 51200,
      autoHandoff = false
    )

    // non-filter
    val result = sql("select * from streaming_longstr.stream_table_longstr_complex order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(3).getString(1) == "name_4")
    assert(result(3).getString(9) == ("4" + longStrValue))
    // check one row of batch loading
    assert(result(52).getInt(0) == 100000003)
    assert(result(52).getString(1) == "batch_3")
    assert(result(52).getString(9) == ("3" + longStrValue))
    assert(result(52).getStruct(10).getInt(1) == 40)

    // filter
    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr_complex where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 1))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr_complex where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from streaming_longstr.stream_table_longstr_complex where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 20))))
  }

  test("[CARBONDATA-3497] Support to write long string for streaming table: StreamSQL") {
    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sink")

    var rows = sql("SHOW STREAMS").collect()
    assertResult(0)(rows.length)

    val csvDataDir = integrationPath + "/spark/target/streamSql_longstr"
    // streaming ingest 10 rows
    generateCSVDataFile(spark, idStart = 10, rowNums = 10, csvDataDir)

    sql(
      s"""
         |CREATE TABLE source(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | longstr STRING
         |)
         |STORED AS carbondata
         |TBLPROPERTIES (
         | 'streaming'='source',
         | 'format'='csv',
         | 'path'='$csvDataDir'
         |)
      """.stripMargin)

    sql(
      s"""
         |CREATE TABLE sink(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | longstr STRING
         | )
         |STORED AS carbondata
         |TBLPROPERTIES('streaming'='sink', 'LONG_STRING_COLUMNS'='longstr')
      """.stripMargin)

    sql(
      """
        |CREATE STREAM stream123 ON TABLE sink
        |STMPROPERTIES(
        |  'trigger'='ProcessingTime',
        |  'interval'='5 seconds')
        |AS
        |  SELECT *
        |  FROM source
        |  WHERE id % 2 = 1
      """.stripMargin).show(false)

    Thread.sleep(200)
    sql("select * from sink").show

    generateCSVDataFile(spark, idStart = 30, rowNums = 10, csvDataDir, SaveMode.Append)
    Thread.sleep(7000)

    // after 2 minibatch, there should be 10 row added (filter condition: id%2=1)
    checkAnswer(sql("select count(*) from sink"), Seq(Row(10)))

    val row = sql("select * from sink order by id").head()
    val exceptedRow = Row(11, "name_11", "city_11", 110000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("11" + longStrValue))
    assertResult(exceptedRow)(row)

    sql("SHOW STREAMS").show(false)

    rows = sql("SHOW STREAMS").collect()
    assertResult(1)(rows.length)
    assertResult("stream123")(rows.head.getString(0))
    assertResult("RUNNING")(rows.head.getString(2))
    assertResult("streaming_longstr.source")(rows.head.getString(3))
    assertResult("streaming_longstr.sink")(rows.head.getString(4))

    rows = sql("SHOW STREAMS ON TABLE sink").collect()
    assertResult(1)(rows.length)
    assertResult("stream123")(rows.head.getString(0))
    assertResult("RUNNING")(rows.head.getString(2))
    assertResult("streaming_longstr.source")(rows.head.getString(3))
    assertResult("streaming_longstr.sink")(rows.head.getString(4))

    sql("DROP STREAM stream123")
    sql("DROP STREAM IF EXISTS stream123")

    rows = sql("SHOW STREAMS").collect()
    assertResult(0)(rows.length)

    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sink")
    new File(csvDataDir).delete()
  }

  def createWriteSocketThread(
      serverSocket: ServerSocket,
      writeNums: Int,
      rowNums: Int,
      intervalSecond: Int): Thread = {
    new Thread() {
      override def run(): Unit = {
        // wait for client to connection request and accept
        val clientSocket = serverSocket.accept()
        val socketWriter = new PrintWriter(clientSocket.getOutputStream())
        var index = 0
        for (_ <- 1 to writeNums) {
          // write 5 records per iteration
          val stringBuilder = new StringBuilder()
          for (_ <- 1 to rowNums) {
            index = index + 1
            stringBuilder.append(index.toString + ",name_" + index
                                 + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                 ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01," +
                                 index.toString() + ("abc" * 12000) +
                                 ",school_" + index + ":school_" + index + index + "$" + index)
            stringBuilder.append("\n")
          }
          socketWriter.append(stringBuilder.toString())
          socketWriter.flush()
          Thread.sleep(1000 * intervalSecond)
        }
        socketWriter.close()
      }
    }
  }

  def createSocketStreamingThread(
      spark: SparkSession,
      port: Int,
      carbonTable: CarbonTable,
      tableIdentifier: TableIdentifier,
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          import spark.implicits._
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", port)
            .load().as[String]
            .map(_.split(","))
            .map { fields => {
              val tmp = fields(10).split("\\$")
              val file = FileElement(tmp(0).split(":"), tmp(1).toInt)
              StreamLongStrData(fields(0).toInt, fields(1), fields(2), fields(3).toFloat,
                  BigDecimal.valueOf(fields(4).toDouble), fields(5).toDouble,
                  fields(6), fields(7), fields(8), fields(9), file)
            } }

          // Write data from socket stream to carbondata file
          // repartition to simulate an empty partition when readSocketDF has only one row
          qry = readSocketDF.repartition(2).writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option(CarbonCommonConstants.HANDOFF_SIZE, handoffSize)
            .option("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
            .option(CarbonCommonConstants.ENABLE_AUTO_HANDOFF, autoHandoff)
            .start()
          qry.awaitTermination()
        } catch {
          case ex: Throwable =>
            LOGGER.error(ex.getMessage)
            throw new Exception(ex.getMessage, ex)
        } finally {
          if (null != qry) {
            qry.stop()
          }
        }
      }
    }
  }

  /**
   * start ingestion thread: write `rowNumsEachBatch` rows repeatly for `batchNums` times.
   */
  def executeStreamingIngest(
      tableName: String,
      batchNums: Int,
      rowNumsEachBatch: Int,
      intervalOfSource: Int,
      intervalOfIngest: Int,
      continueSeconds: Int,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("streaming_longstr"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket()
      val thread1 = createWriteSocketThread(
        serverSocket = server,
        writeNums = batchNums,
        rowNums = rowNumsEachBatch,
        intervalSecond = intervalOfSource)
      val thread2 = createSocketStreamingThread(
        spark = spark,
        port = server.getLocalPort,
        carbonTable = carbonTable,
        tableIdentifier = identifier,
        intervalSecond = intervalOfIngest,
        handoffSize = handoffSize,
        autoHandoff = autoHandoff)
      thread1.start()
      thread2.start()
      Thread.sleep(continueSeconds * 1000)
      thread2.interrupt()
      thread1.interrupt()
    } finally {
      if (null != server) {
        server.close()
      }
    }
  }

  def generateCSVDataFile(
      spark: SparkSession,
      idStart: Int,
      rowNums: Int,
      csvDirPath: String,
      saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    // Create csv data frame file
    val csvDataDF = {
      // generate data with dimension columns (name and city)
      val csvRDD = spark.sparkContext.parallelize(idStart until idStart + rowNums)
        .map { id =>
          (id,
            "name_" + id,
            "city_" + id,
            10000.00 * id,
            BigDecimal.valueOf(0.01),
            80.01,
            "1990-01-01",
            "2010-01-01 10:01:01",
            "2010-01-01 10:01:01",
            id.toString() + ("abc" * 12000),
            "school_" + id + "\002school_" + id + id + "\001" + id)
        }
      spark.createDataFrame(csvRDD).toDF(
        "id", "name", "city", "salary", "tax", "percent", "birthday", "register", "updated", "longstr", "file")
    }

    csvDataDF.write
      .option("header", "false")
      .mode(saveMode)
      .csv(csvDirPath)
  }

  def createFileStreamingThread(
      spark: SparkSession,
      carbonTable: CarbonTable,
      csvDataDir: String,
      intervalSecond: Int,
      tableIdentifier: TableIdentifier): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream.text(csvDataDir)

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"${ intervalSecond } seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
            .option(CarbonStreamParser.CARBON_STREAM_PARSER,
              CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
            .start()

          qry.awaitTermination()
        } catch {
          case _: InterruptedException =>
            println("Done reading and writing streaming data")
        } finally {
          if (qry != null) {
            qry.stop()
          }
        }
      }
    }
  }

  def createTable(tableName: String, streaming: Boolean, withBatchLoad: Boolean): Unit = {
    sql(
      s"""
         | CREATE TABLE streaming_longstr.$tableName(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | longstr STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'LONG_STRING_COLUMNS'='longstr')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def createTableWithComplexType(
      tableName: String,
      streaming: Boolean,
      withBatchLoad: Boolean): Unit = {
    sql(
      s"""
         | CREATE TABLE streaming_longstr.$tableName(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | longstr STRING,
         | file struct<school:array<string>, age:int>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'LONG_STRING_COLUMNS'='longstr')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def executeBatchLoad(tableName: String): Unit = {
    sql(
      s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE streaming_longstr.$tableName OPTIONS" +
      "('HEADER'='true','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
  }

  def wrap(array: Array[String]) = {
    new mutable.WrappedArray.ofRef(array)
  }

  /**
   * get a ServerSocket
   * if the address was already used, it will retry to use new port number.
   *
   * @return ServerSocket
   */
  def getServerSocket(): ServerSocket = {
    var port = 7071
    var serverSocket: ServerSocket = null
    var retry = false
    do {
      try {
        retry = false
        serverSocket = new ServerSocket(port)
      } catch {
        case ex: BindException =>
          retry = true
          port = port + 2
          if (port >= 65535) {
            throw ex
          }
      }
    } while (retry)
    serverSocket
  }
}
