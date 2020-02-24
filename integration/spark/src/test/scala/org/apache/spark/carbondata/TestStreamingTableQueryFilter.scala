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

import java.io.PrintWriter
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

class TestStreamingTableQueryFilter extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample_with_long_string.csv"
  private val longStrValue = "abc" * 12000

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP DATABASE IF EXISTS streaming_table_filter CASCADE")
    sql("CREATE DATABASE streaming_table_filter")
    sql("USE streaming_table_filter")

    dropTable()
    createTableWithComplexType(
      tableName = "stream_filter", streaming = true, withBatchLoad = true)
  }

  override def afterAll {
    dropTable()
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming_table_filter CASCADE")
  }

  def dropTable(): Unit = {
    sql("drop table if exists streaming_table_filter.stream_filter")
  }

  test("[CARBONDATA-3611] Fix failed when filter with measure columns on stream table when this stream table includes complex columns") {
    executeStreamingIngest(
      tableName = "stream_filter",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      handoffSize = 51200,
      autoHandoff = false
    )

    // non-filter
    val result = sql("select * from streaming_table_filter.stream_filter order by id, name").collect()
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
      sql("select * from streaming_table_filter.stream_filter where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 1))))

    checkAnswer(
      sql("select * from streaming_table_filter.stream_filter where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from streaming_table_filter.stream_filter where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("50" + longStrValue), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("1" + longStrValue), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from streaming_table_filter.stream_filter where salary = 490000.0 and percent = 80.01"),
      Seq(Row(49, "name_49", "city_49", 490000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("49" + longStrValue), Row(wrap(Array("school_49", "school_4949")), 49))))

    checkAnswer(
      sql("select * from streaming_table_filter.stream_filter where id > 20 and salary = 300000.0 and file.age > 25"),
      Seq(Row(30, "name_30", "city_30", 300000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), ("30" + longStrValue), Row(wrap(Array("school_30", "school_3030")), 30))))
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
    val identifier = new TableIdentifier(tableName, Option("streaming_table_filter"))
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

  def createTableWithComplexType(
      tableName: String,
      streaming: Boolean,
      withBatchLoad: Boolean): Unit = {
    sql(
      s"""
         | CREATE TABLE streaming_table_filter.$tableName(
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
      s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE streaming_table_filter.$tableName OPTIONS" +
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
