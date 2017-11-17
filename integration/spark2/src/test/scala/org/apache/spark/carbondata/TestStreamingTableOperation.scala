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
import java.net.ServerSocket
import java.util.{Calendar, Date}
import java.util.concurrent.Executors

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatus}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.hadoop.streaming.CarbonStreamOutputFormat
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TestStreamingTableOperation extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"

  override def beforeAll {
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
    // socket source
    createTable(tableName = "stream_table_socket", streaming = true, withBatchLoad = true)
    // file source
    createTable(tableName = "stream_table_file", streaming = true, withBatchLoad = true)

    // 3. streaming table with bad records
    createTable(tableName = "bad_record_force", streaming = true, withBatchLoad = true)
    createTable(tableName = "bad_record_fail", streaming = true, withBatchLoad = true)

    // 4. streaming frequency check
    createTable(tableName = "stream_table_1s", streaming = true, withBatchLoad = true)
    createTable(tableName = "stream_table_10s", streaming = true, withBatchLoad = true)

    // 5. streaming table execute batch loading
    createTable(tableName = "stream_table_batch", streaming = true, withBatchLoad = false)

    // 6. detail query
    // full scan
    createTable(tableName = "stream_table_scan", streaming = true, withBatchLoad = true)
    createTableWithComplexType(
      tableName = "stream_table_scan_complex", streaming = true, withBatchLoad = true)
    // filter scan
    createTable(tableName = "stream_table_filter", streaming = true, withBatchLoad = true)
    createTableWithComplexType(
      tableName = "stream_table_filter_complex", streaming = true, withBatchLoad = true)

    // 7. aggregate query
    createTable(tableName = "stream_table_agg", streaming = true, withBatchLoad = true)
    createTableWithComplexType(
      tableName = "stream_table_agg_complex", streaming = true, withBatchLoad = true)

    // 8. compaction
    createTable(tableName = "stream_table_compact", streaming = true, withBatchLoad = true)

    // 9. create new stream segment if current stream segment is full
    createTable(tableName = "stream_table_new", streaming = true, withBatchLoad = true)

    // 10. fault tolerant
    createTable(tableName = "stream_table_tolerant", streaming = true, withBatchLoad = true)

    // 11. table for delete segment test
    createTable(tableName = "stream_table_delete", streaming = true, withBatchLoad = false)
  }

  test("validate streaming property") {
    sql(
      """
        | CREATE TABLE correct(
        |    c1 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'true')
      """.stripMargin)
    sql("DROP TABLE correct")
    sql(
      """
        | CREATE TABLE correct(
        |    c1 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'false')
      """.stripMargin)
    sql("DROP TABLE correct")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | create table wrong(
          |    c1 string
          | ) STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES ('streaming' = 'invalid')
        """.stripMargin)
    }
  }

  test("test blocking update and delete operation on streaming table") {
    intercept[MalformedCarbonCommandException] {
      sql("""UPDATE source d SET (d.c2) = (d.c2 + 1) WHERE d.c1 = 'a'""").show()
    }
    intercept[MalformedCarbonCommandException] {
      sql("""DELETE FROM source WHERE d.c1 = 'a'""").show()
    }
  }

  test("test blocking alter table operation on streaming table") {
    intercept[MalformedCarbonCommandException] {
      sql("""ALTER TABLE source ADD COLUMNS (c6 string)""").show()
    }
    intercept[MalformedCarbonCommandException] {
      sql("""ALTER TABLE source DROP COLUMNS (c1)""").show()
    }
    intercept[MalformedCarbonCommandException] {
      sql("""ALTER TABLE source RENAME to t""").show()
    }
    intercept[MalformedCarbonCommandException] {
      sql("""ALTER TABLE source CHANGE c1 c1 int""").show()
    }
  }

  override def afterAll {
    dropTable()
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming CASCADE")
  }

  def dropTable(): Unit = {
    sql("drop table if exists streaming.batch_table")
    sql("drop table if exists streaming.stream_table_socket")
    sql("drop table if exists streaming.stream_table_file")
    sql("drop table if exists streaming.bad_record_force")
    sql("drop table if exists streaming.bad_record_fail")
    sql("drop table if exists streaming.stream_table_1s")
    sql("drop table if exists streaming.stream_table_10s")
    sql("drop table if exists streaming.stream_table_batch")
    sql("drop table if exists streaming.stream_table_scan")
    sql("drop table if exists streaming.stream_table_scan_complex")
    sql("drop table if exists streaming.stream_table_filter")
    sql("drop table if exists streaming.stream_table_filter_complex")
    sql("drop table if exists streaming.stream_table_agg")
    sql("drop table if exists streaming.stream_table_agg_complex")
    sql("drop table if exists streaming.stream_table_compact")
    sql("drop table if exists streaming.stream_table_new")
    sql("drop table if exists streaming.stream_table_tolerant")
    sql("drop table if exists streaming.stream_table_delete")
  }

  // normal table not support streaming ingest
  test("normal table not support streaming ingest") {
    val identifier = new TableIdentifier("batch_table", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    var server: ServerSocket = null
    try {
      server = new ServerSocket(7071)
      val thread1 = createWriteSocketThread(server, 2, 10, 1)
      thread1.start()
      // use thread pool to catch the exception of sink thread
      val pool = Executors.newSingleThreadExecutor()
      val thread2 = createSocketStreamingThread(spark, tablePath, identifier)
      val future = pool.submit(thread2)
      Thread.sleep(1000)
      thread1.interrupt()
      try {
        future.get()
        assert(false)
      } catch {
        case ex =>
          assert(ex.getMessage.contains("is not a streaming table"))
      }
    } finally {
      if (server != null) {
        server.close()
      }
    }
  }

  // input source: socket
  test("streaming ingest from socket source") {
    executeStreamingIngest(
      tableName = "stream_table_socket",
      batchNums = 2,
      rowNumsEachBatch = 10,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 10,
      generateBadRecords = false,
      badRecordAction = "force"
    )

    checkAnswer(
      sql("select count(*) from streaming.stream_table_socket"),
      Seq(Row(25))
    )
  }

  // input source: file
  test("streaming ingest from file source") {
    val identifier = new TableIdentifier("stream_table_file", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val csvDataDir = new File("target/csvdata").getCanonicalPath
    // streaming ingest 10 rows
    generateCSVDataFile(spark, idStart = 10, rowNums = 10, csvDataDir)
    val thread = createFileStreamingThread(spark, tablePath, csvDataDir, intervalSecond = 1,
      identifier )
    thread.start()
    Thread.sleep(2000)
    generateCSVDataFile(spark, idStart = 30, rowNums = 10, csvDataDir)
    Thread.sleep(10000)
    thread.interrupt()
    checkAnswer(
      sql("select count(*) from streaming.stream_table_file"),
      Seq(Row(25))
    )
  }

  // bad records
  test("streaming table with bad records action: force") {
    executeStreamingIngest(
      tableName = "bad_record_force",
      batchNums = 2,
      rowNumsEachBatch = 10,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 10,
      generateBadRecords = true,
      badRecordAction = "force"
    )
    checkAnswer(
      sql("select count(*) from streaming.stream_table_socket"),
      Seq(Row(25))
    )

  }

  test("streaming table with bad records action: fail") {
    executeStreamingIngest(
      tableName = "bad_record_fail",
      batchNums = 2,
      rowNumsEachBatch = 10,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 10,
      generateBadRecords = true,
      badRecordAction = "fail"
    )
    val result = sql("select count(*) from streaming.bad_record_fail").collect()
    assert(result(0).getLong(0) < 25)
  }

  // ingest with different interval
  test("1 row per 1 second interval") {
    executeStreamingIngest(
      tableName = "stream_table_1s",
      batchNums = 20,
      rowNumsEachBatch = 1,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force"
    )
    val result = sql("select count(*) from streaming.stream_table_1s").collect()
    // 20 seconds can't ingest all data, exists data delay
    assert(result(0).getLong(0) > 5 + 10)
  }

  test("10000 row per 10 seconds interval") {
    executeStreamingIngest(
      tableName = "stream_table_10s",
      batchNums = 5,
      rowNumsEachBatch = 10000,
      intervalOfSource = 5,
      intervalOfIngest = 10,
      continueSeconds = 50,
      generateBadRecords = false,
      badRecordAction = "force"
    )
    checkAnswer(
      sql("select count(*) from streaming.stream_table_10s"),
      Seq(Row(5 + 10000*5)))
  }

  // batch loading on streaming table
  test("streaming table execute batch loading") {
    executeStreamingIngest(
      tableName = "stream_table_batch",
      batchNums = 5,
      rowNumsEachBatch = 100,
      intervalOfSource = 3,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force"
    )
    checkAnswer(
      sql("select count(*) from streaming.stream_table_batch"),
      Seq(Row(100*5)))

    executeBatchLoad("stream_table_batch")

    checkAnswer(
      sql("select count(*) from streaming.stream_table_batch"),
      Seq(Row(100*5 + 5)))
  }

  // detail query on batch and stream segment
  test("non-filter query on stream table with dictionary, sort_columns") {
    executeStreamingIngest(
      tableName = "stream_table_scan",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force"
    )

    val result = sql("select * from streaming.stream_table_scan order by id").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(0).getInt(0) == 1)
    assert(result(0).getString(1) == "name_1")
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")
  }

  test("non-filter query on stream table with dictionary, sort_columns and complex column") {
    executeStreamingIngest(
      tableName = "stream_table_scan_complex",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force"
    )

    val result = sql("select * from streaming.stream_table_scan_complex order by id").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(0).getInt(0) == 1)
    assert(result(0).getString(1) == "name_1")
    assert(result(0).getStruct(4).getInt(1) == 1)
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")
    assert(result(50).getStruct(4).getInt(1) == 20)
  }

  test("filter query on stream table with dictionary, sort_columns") {
    executeStreamingIngest(
      tableName = "stream_table_filter",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force"
    )

    checkAnswer(
      sql("select * from stream_table_filter where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0)))

    checkAnswer(
      sql("select * from stream_table_filter where name = 'name_2'"),
      Seq(Row(2, "name_2", "", 20000.0)))

    checkAnswer(
      sql("select * from stream_table_filter where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0),
        Row(100000001, "batch_1", "city_1", 0.1)))

    checkAnswer(
      sql("select * from stream_table_filter where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0),
        Row(100000001, "batch_1", "city_1", 0.1)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null"),
      Seq(Row(null, "name_6", "city_6", 60000.0)))

    checkAnswer(
      sql("select * from stream_table_filter where city = ''"),
      Seq(Row(2, "name_2", "", 20000.0)))

  }

  test("filter query on stream table with dictionary, sort_columns and complex column") {
    executeStreamingIngest(
      tableName = "stream_table_filter_complex",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force"
    )

    checkAnswer(
      sql("select * from stream_table_filter_complex where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, Row(wrap(Array("school_1", "school_11")), 1))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name = 'name_2'"),
      Seq(Row(2, "name_2", "", 20000.0, Row(wrap(Array("school_2", "school_22")), 2))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where file.age = 3"),
      Seq(Row(3, "name_3", "city_3", 30000.0, Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = ''"),
      Seq(Row(2, "name_2", "", 20000.0, Row(wrap(Array("school_2", "school_22")), 2))))

  }

  // aggregation
  test("aggregation query") {
    executeStreamingIngest(
      tableName = "stream_table_agg",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force"
    )

    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(id) as integer), sum(id) " +
          "from stream_table_agg where id >= 2 and id <= 100000004"),
      Seq(Row(52, 100000004, "batch_1", 7692332, 400001278)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(id) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_agg " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 50000001, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 100000002, 0.2, 0.2),
        Row("city_3", 2, 100000006, 50000003, 30000.0, 0.3)))
  }

  test("aggregation query with complex") {
    executeStreamingIngest(
      tableName = "stream_table_agg_complex",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force"
    )

    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(file.age) as integer), sum(file.age) " +
          "from stream_table_agg_complex where id >= 2 and id <= 100000004"),
      Seq(Row(52, 100000004, "batch_1", 27, 1408)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(file.age) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_agg_complex " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 10, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 30, 0.2, 0.2),
        Row("city_3", 2, 100000006, 21, 30000.0, 0.3)))
  }

  // compaction
  test("test compaction on stream table") {
    executeStreamingIngest(
      tableName = "stream_table_compact",
      batchNums = 5,
      rowNumsEachBatch = 10,
      intervalOfSource = 2,
      intervalOfIngest = 4,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force"
    )
    for (_ <- 0 to 3) {
      executeBatchLoad("stream_table_compact")
    }

    sql("alter table streaming.stream_table_compact compact 'minor'")
    sql("show segments for table streaming.stream_table_compact").show

    val result = sql("show segments for table streaming.stream_table_compact").collect()
    result.foreach { row =>
      if (row.getString(0).equals("1")) {
        assertResult(SegmentStatus.STREAMING.getMessage)(row.getString(1))
        assertResult(FileFormat.ROW_V1.toString)(row.getString(4))
      }
    }
  }

  // stream segment max size
  test("create new stream segment if current stream segment is full") {
    executeStreamingIngest(
      tableName = "stream_table_new",
      batchNums = 6,
      rowNumsEachBatch = 10000,
      intervalOfSource = 5,
      intervalOfIngest = 10,
      continueSeconds = 40,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1024L * 200
    )
    assert(sql("show segments for table streaming.stream_table_new").count() > 1)

    checkAnswer(
      sql("select count(*) from streaming.stream_table_new"),
      Seq(Row(5 + 10000 * 6))
    )
  }

  test("test deleting streaming segment by ID while ingesting") {
    executeStreamingIngest(
      tableName = "stream_table_delete",
      batchNums = 6,
      rowNumsEachBatch = 10000,
      intervalOfSource = 3,
      intervalOfIngest = 5,
      continueSeconds = 15,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1024L * 200
    )
    val beforeDelete = sql("show segments for table streaming.stream_table_delete").collect()
    val segmentId = beforeDelete.map(_.getString(0)).mkString(",")
    sql(s"delete from table streaming.stream_table_delete where segment.id in ($segmentId) ")

    val rows = sql("show segments for table streaming.stream_table_delete").collect()
    rows.foreach { row =>
      assertResult(SegmentStatus.MARKED_FOR_DELETE.getMessage)(row.getString(1))
    }
  }

  test("test deleting streaming segment by date while ingesting") {
    executeStreamingIngest(
      tableName = "stream_table_delete",
      batchNums = 6,
      rowNumsEachBatch = 10000,
      intervalOfSource = 3,
      intervalOfIngest = 5,
      continueSeconds = 15,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1024L * 200
    )
    val beforeDelete = sql("show segments for table streaming.stream_table_delete").collect()

    sql(s"delete from table streaming.stream_table_delete where segment.starttime before '2999-10-01 01:00:00'")

    val rows = sql("show segments for table streaming.stream_table_delete").collect()
    assertResult(beforeDelete.length)(rows.length)
    rows.foreach { row =>
      assertResult(SegmentStatus.MARKED_FOR_DELETE.getMessage)(row.getString(1))
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
          for (_ <- 1 to rowNums) {
            index = index + 1
            if (badRecords) {
              if (index == 2) {
                // null value
                socketWriter.println(index.toString + ",name_" + index
                                     + ",," + (10000.00 * index).toString +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else if (index == 6) {
                // illegal number
                socketWriter.println(index.toString + "abc,name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else {
                socketWriter.println(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              }
            } else {
              socketWriter.println(index.toString + ",name_" + index
                                   + ",city_" + index + "," + (10000.00 * index).toString +
                                   ",school_" + index + ":school_" + index + index + "$" + index)
            }
          }
          socketWriter.flush()
          Thread.sleep(1000 * intervalSecond)
        }
        socketWriter.close()
      }
    }
  }

  def createSocketStreamingThread(
      spark: SparkSession,
      tablePath: CarbonTablePath,
      tableIdentifier: TableIdentifier,
      badRecordAction: String = "force",
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonStreamOutputFormat.HANDOFF_SIZE_DEFAULT): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 7071)
            .load()

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", tablePath.getStreamingCheckpointDir)
            .option("tablePath", tablePath.getPath)
            .option("bad_records_action", badRecordAction)
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option(CarbonStreamOutputFormat.HANDOFF_SIZE, handoffSize)
            .start()
          qry.awaitTermination()
        } catch {
          case ex =>
            throw new Exception(ex.getMessage)
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
      handoffSize: Long = CarbonStreamOutputFormat.HANDOFF_SIZE_DEFAULT
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    var server: ServerSocket = null
    try {
      server = new ServerSocket(7071)

      val thread1 = createWriteSocketThread(
        serverSocket = server,
        writeNums = batchNums,
        rowNums = rowNumsEachBatch,
        intervalSecond = intervalOfSource,
        badRecords = generateBadRecords)
      val thread2 = createSocketStreamingThread(
        spark = spark,
        tablePath = tablePath, identifier,
        badRecordAction = badRecordAction,
        intervalSecond = intervalOfIngest,
        handoffSize = handoffSize)
      thread1.start()
      thread2.start()
      Thread.sleep(continueSeconds * 1000)
      thread1.interrupt()
      thread2.interrupt()
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
      csvDirPath: String): Unit = {
    // Create csv data frame file
    val csvRDD = spark.sparkContext.parallelize(idStart until idStart + rowNums)
      .map { id =>
        (id,
          "name_" + id,
          "city_" + id,
          10000.00 * id,
          "school_" + id + ":school_" + id + id + "$" + id)
      }
    val csvDataDF = spark.createDataFrame(csvRDD).toDF("id", "name", "city", "salary", "file")

    csvDataDF.write
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .csv(csvDirPath)
  }

  def createFileStreamingThread(
      spark: SparkSession,
      tablePath: CarbonTablePath,
      csvDataDir: String,
      intervalSecond: Int,
      tableIdentifier: TableIdentifier): Thread = {
    new Thread() {
      override def run(): Unit = {
        val inputSchema = new StructType()
          .add("id", "integer")
          .add("name", "string")
          .add("city", "string")
          .add("salary", "float")
          .add("file", "string")
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("csv")
            .option("sep", ",")
            .schema(inputSchema)
            .option("path", csvDataDir)
            .option("header", "false")
            .load()

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"${ intervalSecond } seconds"))
            .option("checkpointLocation", tablePath.getStreamingCheckpointDir)
            .option("tablePath", tablePath.getPath)
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
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
         | salary FLOAT
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'dictionary_include'='city')
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
         | file struct<school:array<string>, age:int>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name', 'dictionary_include'='city')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def executeBatchLoad(tableName: String): Unit = {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$dataFilePath'
         | INTO TABLE streaming.$tableName
         | OPTIONS('HEADER'='true')
         """.stripMargin)
  }

  def wrap(array: Array[String]) = {
    new mutable.WrappedArray.ofRef(array)
  }
}
