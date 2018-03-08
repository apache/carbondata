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
import java.util.concurrent.Executors

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatus}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.exception.{MalformedCarbonCommandException, ProcessMetaDataException}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TestStreamingTableOperation extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"

  override def beforeAll {
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

    // 10. fault tolerant
    createTable(tableName = "stream_table_tolerant", streaming = true, withBatchLoad = true)

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
    sql("drop table if exists streaming.stream_table_file")
    sql("drop table if exists streaming.bad_record_fail")
    sql("drop table if exists streaming.stream_table_1s")
    sql("drop table if exists streaming.stream_table_filter ")
    sql("drop table if exists streaming.stream_table_filter_complex")
    sql("drop table if exists streaming.stream_table_tolerant")
    sql("drop table if exists streaming.stream_table_delete_id")
    sql("drop table if exists streaming.stream_table_delete_date")
    sql("drop table if exists streaming.stream_table_handoff")
    sql("drop table if exists streaming.stream_table_reopen")
    sql("drop table if exists streaming.stream_table_drop")
    sql("drop table if exists streaming.agg_table_block")
    sql("drop table if exists streaming.agg_table_block_agg0")
  }

  // normal table not support streaming ingest
  test("normal table not support streaming ingest and alter normal table's streaming property") {
    // alter normal table's streaming property
    val msg = intercept[MalformedCarbonCommandException](sql("alter table streaming.batch_table set tblproperties('streaming'='false')"))
    assertResult("Streaming property value is incorrect")(msg.getMessage)

    val identifier = new TableIdentifier("batch_table", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket
      val thread1 = createWriteSocketThread(server, 2, 10, 1)
      thread1.start()
      // use thread pool to catch the exception of sink thread
      val pool = Executors.newSingleThreadExecutor()
      val thread2 = createSocketStreamingThread(spark, server.getLocalPort, carbonTable, identifier)
      val future = pool.submit(thread2)
      Thread.sleep(1000)
      thread1.interrupt()
      val msg = intercept[Exception] {
        future.get()
      }
      assert(msg.getMessage.contains("is not a streaming table"))
    } finally {
      if (server != null) {
        server.close()
      }
    }
  }

  // input source: file
  test("streaming ingest from file source") {
    val identifier = new TableIdentifier("stream_table_file", Option("streaming"))
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
    checkAnswer(
      sql("select count(*) from streaming.stream_table_file"),
      Seq(Row(25))
    )

    val row = sql("select * from streaming.stream_table_file order by id").head()
    val exceptedRow = Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))
    assertResult(exceptedRow)(row)
  }

  // bad records
  test("streaming table with bad records action: fail") {
    executeStreamingIngest(
      tableName = "bad_record_fail",
      batchNums = 2,
      rowNumsEachBatch = 10,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 8,
      generateBadRecords = true,
      badRecordAction = "fail",
      autoHandoff = false
    )
    val result = sql("select count(*) from streaming.bad_record_fail").collect()
    assert(result(0).getLong(0) < 10 + 5)
  }

  // ingest with different interval
  test("1 row per 1 second interval") {
    executeStreamingIngest(
      tableName = "stream_table_1s",
      batchNums = 3,
      rowNumsEachBatch = 1,
      intervalOfSource = 1,
      intervalOfIngest = 1,
      continueSeconds = 6,
      generateBadRecords = false,
      badRecordAction = "force",
      autoHandoff = false
    )
    val result = sql("select count(*) from streaming.stream_table_1s").collect()
    // 20 seconds can't ingest all data, exists data delay
    assert(result(0).getLong(0) > 5)
  }

  test("query on stream table with dictionary, sort_columns") {
    executeStreamingIngest(
      tableName = "stream_table_filter",
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
    val result = sql("select * from streaming.stream_table_filter order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(1).isNullAt(0))
    assert(result(1).getString(1) == "name_6")
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")

    // filter
    checkAnswer(
      sql("select * from stream_table_filter where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name in ('name_9','name_10', 'name_11', 'name_12') and id <> 10 and id not in (11, 12)"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name = 'name_3'"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name like '%me_3%' and id < 30"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(sql("select count(*) from stream_table_filter where name like '%ame%'"),
      Seq(Row(49)))

    checkAnswer(sql("select count(*) from stream_table_filter where name like '%batch%'"),
      Seq(Row(5)))

    checkAnswer(
      sql("select * from stream_table_filter where name >= 'name_3' and id < 4"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and name <> 'name_10' and name not in ('name_11', 'name_12')"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city like '%ty_1%' and ( id < 10 or id >= 100000001)"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(sql("select count(*) from stream_table_filter where city like '%city%'"),
      Seq(Row(54)))

    checkAnswer(
      sql("select * from stream_table_filter where city > 'city_09' and city < 'city_10'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city between 'city_09' and 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and city <> 'city_10' and city not in ('city_11', 'city_12')"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary = 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary > 80000 and salary <= 100000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary between 80001 and 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and salary <> 100000.0 and salary not in (110000.0, 120000.0)"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax = 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax >= 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax < 0.05 and tax > 0.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax between 0.02 and 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and tax <> 0.01"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent = 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent >= 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent < 80.05 and percent > 80.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent between 80.02 and 80.05 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and percent <> 80.01"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday = '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday > '1990-01-03' and birthday <= '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and birthday <> '1990-01-01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and register <> '2010-01-01 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and updated <> '2010-01-01 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id is null order by name"),
      Seq(Row(null, "", "", null, null, null, null, null, null),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and name <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and city <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and tax is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and percent is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and birthday is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and register is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and updated is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    // agg
    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(id) as integer), sum(id) " +
          "from stream_table_filter where id >= 2 and id <= 100000004"),
      Seq(Row(51, 100000004, "batch_1", 7843162, 400001276)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(id) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_filter " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 50000001, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 100000002, 0.2, 0.2),
        Row("city_3", 2, 100000006, 50000003, 30000.0, 0.3)))

    // batch loading
    for(_ <- 0 to 2) {
      executeBatchLoad("stream_table_filter")
    }
    checkAnswer(
      sql("select count(*) from streaming.stream_table_filter"),
      Seq(Row(25 * 2 + 5 + 5 * 3)))

    sql("alter table streaming.stream_table_filter compact 'minor'")
    Thread.sleep(5000)
    val result1 = sql("show segments for table streaming.stream_table_filter").collect()
    result1.foreach { row =>
      if (row.getString(0).equals("1")) {
        assertResult(SegmentStatus.STREAMING.getMessage)(row.getString(1))
        assertResult(FileFormat.ROW_V1.toString)(row.getString(5))
      } else if (row.getString(0).equals("0.1")) {
        assertResult(SegmentStatus.SUCCESS.getMessage)(row.getString(1))
        assertResult(FileFormat.COLUMNAR_V3.toString)(row.getString(5))
      } else {
        assertResult(SegmentStatus.COMPACTED.getMessage)(row.getString(1))
        assertResult(FileFormat.COLUMNAR_V3.toString)(row.getString(5))
      }
    }

  }

  test("query on stream table with dictionary, sort_columns and complex column") {
    executeStreamingIngest(
      tableName = "stream_table_filter_complex",
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
    val result = sql("select * from streaming.stream_table_filter_complex order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(0).isNullAt(0))
    assert(result(0).getString(1) == "")
    assert(result(0).getStruct(9).isNullAt(1))
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")
    assert(result(50).getStruct(9).getInt(1) == 20)

    // filter
    checkAnswer(
      sql("select * from stream_table_filter_complex where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name = 'name_3'"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name like '%me_3%' and id < 30"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where name like '%ame%'"),
      Seq(Row(49)))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where name like '%batch%'"),
      Seq(Row(5)))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name >= 'name_3' and id < 4"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city like '%ty_1%' and ( id < 10 or id >= 100000001)"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where city like '%city%'"),
      Seq(Row(54)))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city > 'city_09' and city < 'city_10'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city between 'city_09' and 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary = 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary > 80000 and salary <= 100000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_10", "school_1010")), 10))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary between 80001 and 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax = 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax >= 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax < 0.05 and tax > 0.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax between 0.02 and 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent = 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent >= 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent < 80.05 and percent > 80.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent between 80.02 and 80.05 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday = '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday > '1990-01-03' and birthday <= '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")),50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null order by name"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null)),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and name <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and city <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and tax is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and birthday is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and register is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null, null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and updated is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    // agg
    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(file.age) as integer), sum(file.age) " +
          "from stream_table_filter_complex where id >= 2 and id <= 100000004"),
      Seq(Row(51, 100000004, "batch_1", 27, 1406)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(file.age) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_filter_complex " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 10, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 30, 0.2, 0.2),
        Row("city_3", 2, 100000006, 21, 30000.0, 0.3)))
  }

  test("test deleting streaming segment by ID while ingesting") {
    executeStreamingIngest(
      tableName = "stream_table_delete_id",
      batchNums = 3,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 18,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1,
      autoHandoff = false
    )
    val beforeDelete = sql("show segments for table streaming.stream_table_delete_id").collect()
    val segmentIds1 = beforeDelete.filter(_.getString(1).equals("Streaming")).map(_.getString(0)).mkString(",")
    val msg = intercept[Exception] {
      sql(s"delete from table streaming.stream_table_delete_id where segment.id in ($segmentIds1) ")
    }
    assertResult(s"Delete segment by Id is failed. Invalid ID is: ${beforeDelete.length -1}")(msg.getMessage)

    val segmentIds2 = beforeDelete.filter(_.getString(1).equals("Streaming Finish"))
      .map(_.getString(0)).mkString(",")
    sql(s"delete from table streaming.stream_table_delete_id where segment.id in ($segmentIds2) ")
    val afterDelete = sql("show segments for table streaming.stream_table_delete_id").collect()
    afterDelete.filter(!_.getString(1).equals("Streaming")).foreach { row =>
      assertResult(SegmentStatus.MARKED_FOR_DELETE.getMessage)(row.getString(1))
    }
  }

  test("test deleting streaming segment by date while ingesting") {
    executeStreamingIngest(
      tableName = "stream_table_delete_date",
      batchNums = 3,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 18,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1,
      autoHandoff = false
    )
    val beforeDelete = sql("show segments for table streaming.stream_table_delete_date").collect()
    sql(s"delete from table streaming.stream_table_delete_date where segment.starttime before " +
        s"'2999-10-01 01:00:00'")
    val segmentIds = beforeDelete.filter(_.getString(1).equals("Streaming"))
    assertResult(1)(segmentIds.length)
    val afterDelete = sql("show segments for table streaming.stream_table_delete_date").collect()
    afterDelete.filter(!_.getString(1).equals("Streaming")).foreach { row =>
      assertResult(SegmentStatus.MARKED_FOR_DELETE.getMessage)(row.getString(1))
    }
  }

  test("reject alter streaming properties and handoff 'streaming finish' segment to columnar segment") {
    try {
      sql("ALTER TABLE streaming.stream_table_handoff UNSET TBLPROPERTIES IF EXISTS ('streaming')")
      assert(false, "unsupport to unset streaming property")
    } catch {
      case _ =>
        assert(true)
    }
    try {
      sql("ALTER TABLE streaming.stream_table_handoff SET TBLPROPERTIES('streaming'='true')")
      executeStreamingIngest(
        tableName = "stream_table_handoff",
        batchNums = 2,
        rowNumsEachBatch = 100,
        intervalOfSource = 5,
        intervalOfIngest = 5,
        continueSeconds = 20,
        generateBadRecords = false,
        badRecordAction = "force",
        handoffSize = 1L,
        autoHandoff = false
      )
    } catch {
      case _ =>
        assert(false, "should support set table to streaming")
    }

    // alter streaming table's streaming property
    val msg = intercept[MalformedCarbonCommandException](sql("alter table streaming.stream_table_handoff set tblproperties('streaming'='false')"))
    assertResult("Streaming property can not be changed once it is 'true'")(msg.getMessage)

    val segments = sql("show segments for table streaming.stream_table_handoff").collect()
    assert(segments.length == 2 || segments.length == 3)
    assertResult("Streaming")(segments(0).getString(1))
    assertResult("Streaming Finish")(segments(1).getString(1))
    checkAnswer(
      sql("select count(*) from streaming.stream_table_handoff"),
      Seq(Row(2 * 100))
    )

    val resultBeforeHandoff = sql("select * from streaming.stream_table_handoff order by id, name").collect()
    sql("alter table streaming.stream_table_handoff compact 'streaming'")
    Thread.sleep(5000)
    val resultAfterHandoff = sql("select * from streaming.stream_table_handoff order by id, name").collect()
    assertResult(resultBeforeHandoff)(resultAfterHandoff)
    val newSegments = sql("show segments for table streaming.stream_table_handoff").collect()
    assert(newSegments.length == 3 || newSegments.length == 5)
    assertResult("Streaming")(newSegments((newSegments.length - 1) / 2).getString(1))
    (0 until (newSegments.length - 1) / 2).foreach{ i =>
      assertResult("Success")(newSegments(i).getString(1))
    }
    ((newSegments.length + 1) / 2 until newSegments.length).foreach{ i =>
      assertResult("Compacted")(newSegments(i).getString(1))
    }

    sql("alter table streaming.stream_table_handoff finish streaming")
    val newSegments1 = sql("show segments for table streaming.stream_table_handoff").collect()
    assertResult("Streaming Finish")(newSegments1((newSegments.length - 1) / 2).getString(1))

    checkAnswer(
      sql("select count(*) from streaming.stream_table_handoff"),
      Seq(Row(2 * 100))
    )

    try {
      sql("ALTER TABLE stream_table_handoff SET TBLPROPERTIES('streaming'='false')")
      assert(false, "unsupport disable streaming properties")
    } catch {
      case _ =>
        assert(true)
    }
  }

  test("auto hand off, close and reopen streaming table") {
    executeStreamingIngest(
      tableName = "stream_table_reopen",
      batchNums = 2,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1L,
      autoHandoff = false
    )
    val table1 =
      CarbonEnv.getCarbonTable(Option("streaming"), "stream_table_reopen")(spark)
    assertResult(true)(table1.isStreamingTable)

    sql("alter table streaming.stream_table_reopen compact 'close_streaming'")

    val segments =
      sql("show segments for table streaming.stream_table_reopen").collect()
    assert(segments.length == 4 || segments.length == 6)
    assertResult(segments.length / 2)(segments.filter(_.getString(1).equals("Success")).length)
    assertResult(segments.length / 2)(segments.filter(_.getString(1).equals("Compacted")).length)

    checkAnswer(
      sql("select count(*) from streaming.stream_table_reopen"),
      Seq(Row(2 * 100))
    )

    val table2 =
      CarbonEnv.getCarbonTable(Option("streaming"), "stream_table_reopen")(spark)
    assertResult(false)(table2.isStreamingTable)

    sql("ALTER TABLE streaming.stream_table_reopen SET TBLPROPERTIES('streaming'='true')")

    val table3 =
      CarbonEnv.getCarbonTable(Option("streaming"), "stream_table_reopen")(spark)
    assertResult(true)(table3.isStreamingTable)

    executeStreamingIngest(
      tableName = "stream_table_reopen",
      batchNums = 2,
      rowNumsEachBatch = 100,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1L,
      autoHandoff = true
    )
    Thread.sleep(10000)
    val newSegments1 =
      sql("show segments for table streaming.stream_table_reopen").collect()
    assert(newSegments1.length == 7 || newSegments1.length == 9 || newSegments1.length == 11)
    assertResult(1)(newSegments1.filter(_.getString(1).equals("Streaming")).length)
    assertResult((newSegments1.length - 1) / 2)(newSegments1.filter(_.getString(1).equals("Success")).length)
    assertResult((newSegments1.length - 1) / 2)(newSegments1.filter(_.getString(1).equals("Compacted")).length)

    sql("alter table streaming.stream_table_reopen compact 'close_streaming'")
    val newSegments =
      sql("show segments for table streaming.stream_table_reopen").collect()
    assert(newSegments.length == 8 || newSegments.length == 10 || newSegments.length == 12)
    assertResult(newSegments.length / 2)(newSegments.filter(_.getString(1).equals("Success")).length)
    assertResult(newSegments.length / 2)(newSegments.filter(_.getString(1).equals("Compacted")).length)

    //Verify MergeTO column entry for compacted Segments
    newSegments.filter(_.getString(1).equals("Compacted")).foreach{ rw =>
      assertResult("Compacted")(rw.getString(1))
      assert(Integer.parseInt(rw.getString(0)) < Integer.parseInt(rw.getString(4)))
    }
    checkAnswer(
      sql("select count(*) from streaming.stream_table_reopen"),
      Seq(Row(2 * 100 * 2))
    )
  }

  test("block drop streaming table while streaming is in progress") {
    val identifier = new TableIdentifier("stream_table_drop", Option("streaming"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket
      val thread1 = createWriteSocketThread(server, 2, 10, 3)
      val thread2 = createSocketStreamingThread(spark, server.getLocalPort, carbonTable, identifier, "force", 5, 1024L * 200, false)
      thread1.start()
      thread2.start()
      Thread.sleep(1000)
      val msg = intercept[ProcessMetaDataException] {
        sql(s"drop table streaming.stream_table_drop")
      }
      assert(msg.getMessage.contains("Dropping table streaming.stream_table_drop failed: Acquire table lock failed after retry, please try after some time"))
      thread1.interrupt()
      thread2.interrupt()
    } finally {
      if (server != null) {
        server.close()
      }
    }
  }

  test("do not support creating datamap on streaming table") {
    assert(
      intercept[MalformedCarbonCommandException](
        sql("CREATE DATAMAP datamap ON TABLE source " +
            "USING 'preaggregate'" +
            " AS SELECT c1, sum(c2) FROM source GROUP BY c1")
      ).getMessage.contains("Streaming table does not support creating datamap"))
  }

  test("check streaming property of table") {
    checkExistence(sql("DESC FORMATTED batch_table"), true, "Streaming")
    val result =
      sql("DESC FORMATTED batch_table").collect().filter(_.getString(0).trim.equals("Streaming"))
    assertResult(1)(result.length)
    assertResult("false")(result(0).getString(1).trim)

    checkExistence(sql("DESC FORMATTED stream_table_file"), true, "Streaming")
    val resultStreaming = sql("DESC FORMATTED stream_table_file").collect()
      .filter(_.getString(0).trim.equals("Streaming"))
    assertResult(1)(resultStreaming.length)
    assertResult("true")(resultStreaming(0).getString(1).trim)
  }

  test("block streaming for 'preaggregate' table") {
    sql("create datamap agg_table_block_agg0 on table streaming.agg_table_block using 'preaggregate' as select city, count(name) from streaming.agg_table_block group by city")
    val msg = intercept[MalformedCarbonCommandException](sql("ALTER TABLE streaming.agg_table_block SET TBLPROPERTIES('streaming'='true')"))
    assertResult("The table has 'preaggregate' DataMap, it doesn't support streaming")(msg.getMessage)
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
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("streaming"))
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
         | 'sort_columns'='name', 'dictionary_include'='city,register')
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
         | 'sort_columns'='name', 'dictionary_include'='id,name,salary,tax,percent,updated')
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
