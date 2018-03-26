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
package org.apache.carbondata.cluster.sdv.generated

import java.io.{File, PrintWriter}
import java.math.BigDecimal
import java.net.{BindException, ServerSocket}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{CarbonEnv, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath

class StreamingTestCase extends QueryTest with BeforeAndAfterAll {
  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"

  override def beforeAll: Unit = {
    sql("drop table if exists stream_table_file")
    sql("drop table if exists stream_table_socket")
    sql("drop table if exists stream_table_handoff")
    sql("drop table if exists stream_table_close")
  }

  test("test loading in streaming table from file source") {
    sql(
      s"""
         | CREATE TABLE stream_table_file(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('streaming'='true',
         | 'sort_columns'='name', 'dictionary_include'='city,register')
         | """.stripMargin)

    val identifier = new TableIdentifier("stream_table_file", Option("default"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdata").getCanonicalPath
    // streaming ingest 10 rows
    generateCSVDataFile(spark, idStart = 10, rowNums = 10, csvDataDir)

    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(2000)
    generateCSVDataFile(spark, idStart = 30, rowNums = 10, csvDataDir)
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(sql("select count(*) from stream_table_file"), Seq(Row(20)))
    val row = sql("select * from stream_table_file order by id").head()
    val exceptedRow = Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))
    assertResult(exceptedRow)(row)
  }

  test("test loading in streaming table from socket source") {
    sql("""
         | CREATE TABLE stream_table_socket(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('streaming'='true',
         | 'sort_columns'='name', 'dictionary_include'='city,register')
         | """.stripMargin)

    sql(s"""
         | LOAD DATA LOCAL INPATH '$dataFilePath'
         | INTO TABLE stream_table_socket
         | OPTIONS('HEADER'='true')
         """.stripMargin)

    executeStreamingIngest(
      tableName = "stream_table_socket",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force",
      autoHandoff = false
    )

    // non-filter
    val result = sql("select * from stream_table_socket order by id, name").collect()
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(1).isNullAt(0))
    assert(result(1).getString(1) == "name_6")
    checkAnswer(
      sql("select * from stream_table_socket where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_socket where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_socket where updated > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_socket where id is null order by name"),
      Seq(Row(null, "", "", null, null, null, null, null, null),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(id) as integer), sum(id) " +
          "from stream_table_socket where id >= 2 and id <= 100000004"),
      Seq(Row(51, 100000004, "batch_1", 7843162, 400001276)))
  }

  test("test streaming handoff of 'streaming finish' segment to columnar segment") {
    sql("""
          | CREATE TABLE stream_table_handoff(
          | id INT,
          | name STRING,
          | city STRING,
          | salary FLOAT,
          | tax DECIMAL(8,2),
          | percent double,
          | birthday DATE,
          | register TIMESTAMP,
          | updated TIMESTAMP
          | )
          | STORED BY 'carbondata'
          | """.stripMargin)

    try {
      sql("ALTER TABLE stream_table_handoff SET TBLPROPERTIES('streaming'='true')")
      executeStreamingIngest(
        tableName = "stream_table_handoff",
        batchNums = 2,
        rowNumsEachBatch = 10,
        intervalOfSource = 0,
        intervalOfIngest = 0,
        continueSeconds = 20,
        generateBadRecords = false,
        badRecordAction = "force",
        handoffSize = 3000L,
        autoHandoff = false
      )
    } catch {
      case _ =>
        assert(false, "should support set table to streaming")
    }

    val segments: Array[Row] = sql("show segments for table stream_table_handoff").collect()
    assert(segments.length == 1)
    assertResult("Streaming")(segments(0).getString(1))

    checkAnswer(sql("select count(*) from stream_table_handoff"), Seq(Row(20)))

    sql("alter table stream_table_handoff finish streaming")
    val newSegments1 = sql("show segments for table stream_table_handoff").collect()
    assertResult("Streaming Finish")(newSegments1(0).getString(1))

    val resultBeforeHandoff = sql("select * from stream_table_handoff order by id, name")
    sql("alter table stream_table_handoff compact 'streaming'")
    val resultAfterHandoff = sql("select * from stream_table_handoff order by id, name")
    checkAnswer(resultBeforeHandoff, resultAfterHandoff)
    val newSegments = sql("show segments for table stream_table_handoff").collect()
    assert(newSegments.length == 2)
    assertResult("Success")(newSegments(0).getString(1))
    assertResult("Compacted")(newSegments(1).getString(1))

  }

  test("test 'close_streaming' for streaming table") {
    sql("""
          | CREATE TABLE stream_table_close(
          | id INT,
          | name STRING,
          | city STRING,
          | salary FLOAT,
          | tax DECIMAL(8,2),
          | percent double,
          | birthday DATE,
          | register TIMESTAMP,
          | updated TIMESTAMP
          | )
          | STORED BY 'carbondata'
          | TBLPROPERTIES('streaming'='true')
        """.stripMargin)

    executeStreamingIngest(
      tableName = "stream_table_close",
      batchNums = 2,
      rowNumsEachBatch = 10,
      intervalOfSource = 0,
      intervalOfIngest = 0,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 3000L,
      autoHandoff = false
    )

    val table1 =
      CarbonEnv.getCarbonTable(Option("default"), "stream_table_close")(spark)
    assertResult(true)(table1.isStreamingTable)
    sql("alter table stream_table_close compact 'close_streaming'")
    val segments = sql("show segments for table stream_table_close").collect()
    assertResult("Success")(segments(0).getString(1))
    assertResult("Compacted")(segments(1).getString(1))

    val streamTable = CarbonEnv.getCarbonTable(Option("default"), "stream_table_close")(spark)
    assertResult(false)(streamTable.isStreamingTable)
  }

  def executeStreamingIngest(
      tableName: String,
      batchNums: Int,
      rowNumsEachBatch: Int,
      intervalOfSource: Int,
      intervalOfIngest: Int,
      continueSeconds: Int,
      generateBadRecords: Boolean,
      badRecordAction: String,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("default"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket()
      val thread1 = createWriteSocketThread(
        serverSocket = server,
        writeNums = batchNums,
        rowNums = rowNumsEachBatch,
        intervalSecond = intervalOfSource,
        badRecords = generateBadRecords)
      val thread2 = createSocketStreamingThread(
        spark = spark,
        port = server.getLocalPort,
        carbonTable = carbonTable,
        tableIdentifier = identifier,
        badRecordAction = badRecordAction,
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

  def createSocketStreamingThread(
      spark: SparkSession,
      port: Int,
      carbonTable: CarbonTable,
      tableIdentifier: TableIdentifier,
      badRecordAction: String = "force",
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", port)
            .load()

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("bad_records_action", badRecordAction)
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

  def createWriteSocketThread(
      serverSocket: ServerSocket,
      writeNums: Int,
      rowNums: Int,
      intervalSecond: Int,
      badRecords: Boolean = false): Thread = {
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
            if (badRecords) {
              if (index == 2) {
                // null value
                stringBuilder.append(",,,,,,,,,")
              } else if (index == 6) {
                // illegal number
                stringBuilder.append(index.toString + "abc,name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else if (index == 9) {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.04,80.04" +
                                     ",1990-01-04,2010-01-04 10:01:01,2010-01-04 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              }
            } else {
              stringBuilder.append(index.toString + ",name_" + index
                                   + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                   ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                   ",school_" + index + ":school_" + index + index + "$" + index)
            }
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

  def generateCSVDataFile(
      spark: SparkSession,
      idStart: Int,
      rowNums: Int,
      csvDirPath: String): Unit = {
    // Create csv data frame file
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
          "school_" + id + ":school_" + id + id + "$" + id)
      }
    val csvDataDF = spark.createDataFrame(csvRDD).toDF(
      "id", "name", "city", "salary", "tax", "percent", "birthday", "register", "updated", "file")

    csvDataDF.write
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .csv(csvDirPath)
  }

  override def afterAll(): Unit = {
    sql("drop table if exists stream_table_file")
    sql("drop table if exists stream_table_socket")
    sql("drop table if exists stream_table_handoff")
    sql("drop table if exists stream_table_close")
  }
}
