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
import java.sql.Timestamp
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.streaming.parser.CarbonStreamParser

class TestCarbonStreamingTableOpName extends CarbonQueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"
  def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath
  val badRecordFilePath: File =new File(currentPath + "/target/test/badRecords")

  override def beforeAll {
    badRecordFilePath.delete()
    badRecordFilePath.mkdirs()
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP DATABASE IF EXISTS streaming CASCADE")
    sql("CREATE DATABASE streaming")
    sql("USE streaming")
    sql(
      """
        | CREATE TABLE source(
        |    c1 string,
        |    c2 int,
        |    c3 string,
        |    c5 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'true')
      """.stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO TABLE source""")

    dropTable()

    // 1. normal table not support streaming ingest
    createTable(tableName = "batch_table", streaming = false, withBatchLoad = true)

    // 2. streaming table with different input source
    // file source
    createTable(tableName = "stream_table_file", streaming = true, withBatchLoad = true)

    // 3. streaming table with bad records
    createTable(tableName = "bad_record_fail", streaming = true, withBatchLoad = true)

    // 4. streaming frequency check
    createTable(tableName = "stream_table_1s", streaming = true, withBatchLoad = true)

    // 5. streaming table execute batch loading
    // 6. detail query
    // 8. compaction
    // full scan + filter scan + aggregate query
    createTable(tableName = "stream_table_filter", streaming = true, withBatchLoad = true)

    createTableWithComplexType(
      tableName = "stream_table_filter_complex", streaming = true, withBatchLoad = true)


    // 11. table for delete segment test
    createTable(tableName = "stream_table_delete_id", streaming = true, withBatchLoad = false)
    createTable(tableName = "stream_table_delete_date", streaming = true, withBatchLoad = false)

    // 12. reject alter streaming properties
    // 13. handoff streaming segment and finish streaming
    createTable(tableName = "stream_table_handoff", streaming = false, withBatchLoad = false)

    // 15. auto handoff streaming segment
    // 16. close streaming table
    // 17. reopen streaming table after close
    // 9. create new stream segment if current stream segment is full
    createTable(tableName = "stream_table_reopen", streaming = true, withBatchLoad = false)

    // 18. block drop table while streaming is in progress
    createTable(tableName = "stream_table_drop", streaming = true, withBatchLoad = false)

    // 19. block streaming on 'preaggregate' main table
    createTable(tableName = "agg_table_block", streaming = false, withBatchLoad = false)

    createTable(tableName = "agg_table", streaming = true, withBatchLoad = false)

    createTable(tableName = "stream_table_empty", streaming = true, withBatchLoad = false)

    var csvDataDir = integrationPath + "/spark2/target/csvdatanew"
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir)
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir, SaveMode.Append)
  }

  override def  afterAll {
    dropTable()
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming CASCADE")
    var csvDataDir = integrationPath + "/spark2/target/csvdatanew"
    badRecordFilePath.delete()
    new File(csvDataDir).delete()
    csvDataDir = integrationPath + "/spark2/target/csvdata"
    new File(csvDataDir).delete()
  }

  def dropTable(): Unit = {
    sql("drop table if exists streaming.batch_table")
    sql("drop table if exists streaming.stream_table_file")
    sql("drop table if exists streaming.bad_record_fail")
    sql("drop table if exists streaming.stream_table_1s")
    sql("drop table if exists streaming.stream_table_filter ")
    sql("drop table if exists streaming.stream_table_filter_complex")
    sql("drop table if exists streaming.stream_table_delete_id")
    sql("drop table if exists streaming.stream_table_delete_date")
    sql("drop table if exists streaming.stream_table_handoff")
    sql("drop table if exists streaming.stream_table_reopen")
    sql("drop table if exists streaming.stream_table_drop")
    sql("drop table if exists streaming.agg_table_block")
    sql("drop table if exists streaming.stream_table_empty")
  }

  def loadData() {
    val identifier = new TableIdentifier("agg_table2", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    // streaming ingest 10 rows
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir)
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(2000)
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir)
    Thread.sleep(5000)
    thread.interrupt()
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
                                     ",school_" + index + "\002school_" + index + index + "\001" + index)
              } else if (index == 9) {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.04,80.04" +
                                     ",1990-01-04,2010-01-04 10:01:01,2010-01-04 10:01:01" +
                                     ",school_" + index + "\002school_" + index + index + "\001" + index)
              } else {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + "\002school_" + index + index + "\001" + index)
              }
            } else {
              stringBuilder.append(index.toString + ",name_" + index
                                   + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                   ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                   ",school_" + index + "\002school_" + index + index + "\001" + index)
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

  def createSocketStreamingThread(
      spark: SparkSession,
      port: Int,
      carbonTable: CarbonTable,
      tableIdentifier: TableIdentifier,
      badRecordAction: String = "force",
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean,
      badRecordsPath: String = CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL
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
          // repartition to simulate an empty partition when readSocketDF has only one row
          qry = readSocketDF.repartition(2).writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("bad_records_action", badRecordAction)
            .option("BAD_RECORD_PATH", badRecordsPath)
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option(CarbonStreamParser.CARBON_STREAM_PARSER,
              CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
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
      generateBadRecords: Boolean,
      badRecordAction: String,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean,
      badRecordsPath: String = CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
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
        autoHandoff = autoHandoff,
        badRecordsPath = badRecordsPath)
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
      saveMode: SaveMode = SaveMode.Overwrite,
      withDim: Boolean = true): Unit = {
    // Create csv data frame file
    val csvDataDF = if (withDim) {
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
            "school_" + id + "\002school_" + id + id + "\001" + id)
        }
      spark.createDataFrame(csvRDD).toDF(
        "id", "name", "city", "salary", "tax", "percent", "birthday", "register", "updated", "file")
    } else {
      // generate data without dimension columns
      val csvRDD = spark.sparkContext.parallelize(idStart until idStart + rowNums)
        .map { id =>
          (id % 3 + 1,
            10000.00 * id,
            BigDecimal.valueOf(0.01),
            80.01,
            "1990-01-01",
            "2010-01-01 10:01:01",
            "2010-01-01 10:01:01",
            "school_" + id + "\002school_" + id + id + "\001" + id)
        }
      spark.createDataFrame(csvRDD).toDF(
        "id", "salary", "tax", "percent", "birthday", "register", "updated", "file")
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
         | CREATE TABLE streaming.$tableName(
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
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'dictionary_include'='city,register', 'BAD_RECORD_PATH'='$badRecordFilePath')
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
         | CREATE TABLE streaming.$tableName(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | file struct<school:array<string>, age:int>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'dictionary_include'='id,name,salary,tax,percent,updated', 'BAD_RECORD_PATH'='$badRecordFilePath')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def executeBatchLoad(tableName: String): Unit = {
    sql(
      s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE streaming.$tableName OPTIONS" +
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

  def findCarbonScanRDD(rdd: RDD[_]): RDD[_] = {
    if (rdd.isInstanceOf[CarbonScanRDD[_]]) {
      rdd
    } else {
      findCarbonScanRDD(rdd.dependencies(0).rdd)
    }
  }

  def partitionNums(sqlString : String): Int = {
    val rdd = findCarbonScanRDD(sql(sqlString).rdd)
    rdd.partitions.length
  }


  test("test preaggregate table creation on streaming table without handoff") {
    val identifier = new TableIdentifier("agg_table", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table"),
      Seq(Row(10)))
    sql("create datamap p1 on table agg_table using 'preaggregate' as select name, sum(salary) from agg_table group by name")
    // No data should be loaded into aggregate table as hand-off is not yet fired
    checkAnswer(sql("select * from agg_table_p1"), Seq())
  }

  test("test if data is loaded into preaggregate after handoff is fired") {
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("agg_table2", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table2"),
      Seq(Row(10)))
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    sql("create datamap p2 on table agg_table2 using 'preaggregate' as select name, avg(salary) from agg_table2 group by name")
    sql("create datamap p3 on table agg_table2 using 'preaggregate' as select name, min(salary) from agg_table2 group by name")
    sql("create datamap p4 on table agg_table2 using 'preaggregate' as select name, max(salary) from agg_table2 group by name")
    sql("create datamap p5 on table agg_table2 using 'preaggregate' as select name, count(salary) from agg_table2 group by name")
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    // Data should be loaded into aggregate table as hand-off is fired
    checkAnswer(sql("select * from agg_table2_p1"),
      Seq(
        Row("name_10", 200000.0),
        Row("name_11", 220000.0),
        Row("name_12", 240000.0),
        Row("name_13", 260000.0),
        Row("name_14", 280000.0)))
    checkAnswer(sql("select * from agg_table2_p2"),
      Seq(
        Row("name_10", 200000.0, 2.0),
        Row("name_11", 220000.0, 2.0),
        Row("name_12", 240000.0, 2.0),
        Row("name_13", 260000.0, 2.0),
        Row("name_14", 280000.0, 2.0)))
    checkAnswer(sql("select * from agg_table2_p3"),
      Seq(
        Row("name_10", 100000.0),
        Row("name_11", 110000.0),
        Row("name_12", 120000.0),
        Row("name_13", 130000.0),
        Row("name_14", 140000.0)))
    checkAnswer(sql("select * from agg_table2_p4"),
      Seq(
        Row("name_10", 100000.0),
        Row("name_11", 110000.0),
        Row("name_12", 120000.0),
        Row("name_13", 130000.0),
        Row("name_14", 140000.0)))
    checkAnswer(sql("select * from agg_table2_p5"),
      Seq(
        Row("name_10", 2.0),
        Row("name_11", 2.0),
        Row("name_12", 2.0),
        Row("name_13", 2.0),
        Row("name_14", 2.0)))
    sql("drop table agg_table2")
  }

  test("test whether data is loaded into preaggregate after handoff is fired") {
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("agg_table2", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table2"),
      Seq(Row(10)))
    sql(s"load data inpath '$csvDataDir' into table agg_table2 options('FILEHEADER'='id, name, city, salary, tax, percent, birthday, register, updated, file')")
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    // Data should be loaded into aggregate table as hand-off is fired
    checkAnswer(sql("select name, sum(salary) from agg_table2 group by name"),
      Seq(
        Row("name_10", 400000.0),
        Row("name_14", 560000.0),
        Row("name_12", 480000.0),
        Row("name_11", 440000.0),
        Row("name_13", 520000.0)))
    checkAnswer(sql("select * from agg_table2_p1"),
      Seq(
        Row("name_10", 200000.0),
        Row("name_11", 220000.0),
        Row("name_12", 240000.0),
        Row("name_13", 260000.0),
        Row("name_14", 280000.0),
        Row("name_10", 200000.0),
        Row("name_11", 220000.0),
        Row("name_12", 240000.0),
        Row("name_13", 260000.0),
        Row("name_14", 280000.0)))

    sql("drop table agg_table2")
  }

  test("test whether data is loaded into preaggregate before handoff is fired") {
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("agg_table2", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table2"),
      Seq(Row(10)))
    sql(s"load data inpath '$csvDataDir' into table agg_table2 options('FILEHEADER'='id, name, city, salary, tax, percent, birthday, register, updated, file')")
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    // Data should be loaded into aggregate table as hand-off is fired
    checkAnswer(sql("select name, sum(salary) from agg_table2 group by name"),
      Seq(
        Row("name_10", 400000.0),
        Row("name_14", 560000.0),
        Row("name_12", 480000.0),
        Row("name_11", 440000.0),
        Row("name_13", 520000.0)))
    checkAnswer(sql("select * from agg_table2_p1"),
      Seq(
        Row("name_10", 200000.0),
        Row("name_11", 220000.0),
        Row("name_12", 240000.0),
        Row("name_13", 260000.0),
        Row("name_14", 280000.0)))

    sql("drop table agg_table2")
  }

  test("test if minor compaction is successful for streaming and preaggregate tables") {
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    sql("create datamap p2 on table agg_table2 using 'preaggregate' as select name, min(salary) from agg_table2 group by name")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    sql("alter table agg_table2 compact 'minor'")
    checkAnswer(sql("select * from agg_table2_p1"),
      Seq(
        Row("name_10", 800000.0),
        Row("name_11", 880000.0),
        Row("name_12", 960000.0),
        Row("name_13", 1040000.0),
        Row("name_14", 1120000.0)))
    assert(sql("show segments for table agg_table2").collect().map(_.get(0)).contains("1.1"))
    assert(sql("show segments for table agg_table2_p1").collect().map(_.get(0)).contains("0.1"))
    assert(sql("show segments for table agg_table2_p2").collect().map(_.get(0)).contains("0.1"))
    sql("drop table if exists agg_table2")
  }

  test("test if data is displayed when alias is used for column name") {
    sql("drop table if exists agg_table2")
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("agg_table2", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdata1").getCanonicalPath
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir)
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir, SaveMode.Append)
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table2"),
      Seq(Row(10)))
    sql(s"load data inpath '$csvDataDir' into table agg_table2 options('FILEHEADER'='id, name, city, salary, tax, percent, birthday, register, updated, file')")
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    // Data should be loaded into aggregate table as hand-off is fired
    checkAnswer(sql("select name as abc, sum(salary) as sal from agg_table2 group by name"),
      Seq(
        Row("name_14", 560000.0),
        Row("name_10", 400000.0),
        Row("name_12", 480000.0),
        Row("name_11", 440000.0),
        Row("name_13", 520000.0)))

    sql("drop table agg_table2")
  }

  test("test if data is loaded in aggregate table after handoff is done for streaming table") {
    createTable(tableName = "agg_table3", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("agg_table3", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir)
    generateCSVDataFile(spark, idStart = 10, rowNums = 5, csvDataDir, SaveMode.Append)
    // streaming ingest 10 rows
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.agg_table3"),
      Seq(Row(10)))
    sql("alter table agg_table3 finish streaming")
    sql("alter table agg_table3 compact 'streaming'")
    sql("create datamap p1 on table agg_table3 using 'preaggregate' as select name, sum(salary) from agg_table3 group by name")
    // Data should be loaded into aggregate table as hand-off is fired
    checkAnswer(sql("select * from agg_table3_p1"),
      Seq(
        Row("name_10", 200000.0),
        Row("name_11", 220000.0),
        Row("name_12", 240000.0),
        Row("name_13", 260000.0),
        Row("name_14", 280000.0)))
  }

  test("test autohandoff with preaggregate tables") {
    sql("drop table if exists maintable")
    createTable(tableName = "maintable", streaming = true, withBatchLoad = false)
    sql("create datamap p1 on table maintable using 'preaggregate' as select name, sum(id) from maintable group by name")
    executeStreamingIngest(
      tableName = "maintable",
      batchNums = 2,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1L,
      autoHandoff = false)
    executeStreamingIngest(
      tableName = "maintable",
      batchNums = 2,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1L,
      autoHandoff = true)
    checkAnswer(sql("select count(*) from maintable_p1"), Seq(Row(200)))
  }

  test("support creating datamap on streaming table") {
    sql("CREATE DATAMAP datamap ON TABLE source " +
      "USING 'preaggregate'" +
      " AS SELECT c1, sum(c2) FROM source GROUP BY c1")
  }

  test("test if major compaction is successful for streaming and preaggregate tables") {
    sql("drop table if exists agg_table2")
    createTable(tableName = "agg_table2", streaming = true, withBatchLoad = false)
    sql("create datamap p1 on table agg_table2 using 'preaggregate' as select name, sum(salary) from agg_table2 group by name")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    loadData()
    sql("alter table agg_table2 finish streaming")
    sql("alter table agg_table2 compact 'streaming'")
    sql("alter table agg_table2 compact 'major'")
    checkAnswer(sql("select * from agg_table2_p1"),
      Seq(
        Row("name_10", 800000.0),
        Row("name_11", 880000.0),
        Row("name_12", 960000.0),
        Row("name_13", 1040000.0),
        Row("name_14", 1120000.0)))
    assert(sql("show segments for table agg_table2").collect().map(_.get(0)).contains("1.1"))
    assert(sql("show segments for table agg_table2_p1").collect().map(_.get(0)).contains("0.1"))
    sql("drop table if exists agg_table2")
  }

  test("test if timeseries load is successful when created on streaming table with day granularity") {
    sql("drop table if exists timeseries_table")
    createTable(tableName = "timeseries_table", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("timeseries_table", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    sql(
      s"""
         | CREATE DATAMAP agg0_day ON TABLE timeseries_table
         | USING '${TIMESERIES.toString}'
         | DMPROPERTIES (
         | 'EVENT_TIME'='register',
         | 'DAY_GRANULARITY'='1')
         | AS SELECT register, SUM(id) FROM timeseries_table
         | GROUP BY register
       """.stripMargin)
    sql("alter table timeseries_table finish streaming")
    sql("alter table timeseries_table compact 'streaming'")
    checkAnswer( sql("select * FROM timeseries_table_agg0_day"), Seq(Row(Timestamp.valueOf("2010-01-01 00:00:00.0"), 120)))
  }

  test("test if timeseries load is successful when created on streaming table") {
    sql("drop table if exists timeseries_table")
    createTable(tableName = "timeseries_table", streaming = true, withBatchLoad = false)
    val identifier = new TableIdentifier("timeseries_table", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val csvDataDir = new File("target/csvdatanew").getCanonicalPath
    val thread = createFileStreamingThread(spark, carbonTable, csvDataDir, intervalSecond = 1,
      identifier)
    thread.start()
    Thread.sleep(5000)
    thread.interrupt()
    sql(
      s"""
         | CREATE DATAMAP agg0_second ON TABLE timeseries_table
         | USING '${TIMESERIES.toString}'
         | DMPROPERTIES (
         | 'EVENT_TIME'='register',
         | 'SECOND_GRANULARITY'='1')
         | AS SELECT register, SUM(id) FROM timeseries_table
         | GROUP BY register
       """.stripMargin)
    sql("alter table timeseries_table finish streaming")
    sql("alter table timeseries_table compact 'streaming'")
    checkAnswer( sql("select * FROM timeseries_table_agg0_second"), Seq(Row(Timestamp.valueOf("2010-01-01 10:01:01.0"), 120)))
  }
}
