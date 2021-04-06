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

package org.apache.carbon.flink

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestCarbonWriter extends QueryTest with BeforeAndAfterAll{

  val tableName = "test_flink"
  val bucketTableName = "insert_bucket_table"
  val dataTempPath: String = targetTestClass + "/data/temp/"

  test("Writing flink data to local carbon table") {
    createTable
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.enableCheckpointing(2000L)
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)

      checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(0)))

      // query with stage input
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT, "true")
      checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(1000)))
      sql(s"select * from $tableName limit 10").collect()
      checkAnswer(sql(s"select max(intField) from $tableName"), Seq(Row(999)))
      checkAnswer(sql(s"select count(intField) from $tableName where intField >= 900"),
        Seq(Row(100)))
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT, "false")

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))
      checkAnswer(sql(s"select count(intField) from $tableName where intField >= 900"),
        Seq(Row(100)))
      checkIfStageFilesAreDeleted(tablePath)
      assert(getMergeIndexFileCount(tableName, "0") == 1)
    }
  }

  test("test batch_file_count option") {
    createTable
    try {
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)
      writerProperties.put(CarbonLocalProperty.COMMIT_THRESHOLD, "100")

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)

      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(500)))

      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))
    }
  }

  test("test carbon writer of bucket table") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(s"DROP TABLE IF EXISTS $bucketTableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (
         | stringField string, intField int, shortField short, stringField1 string)
         | STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='stringField')
      """.stripMargin
    ).collect()
    sql(
      s"""
         | CREATE TABLE $bucketTableName (
         | stringField string, intField int, shortField short, stringField1 string)
         | STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='stringField')
      """.stripMargin
    ).collect()
    try {
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)
      writerProperties.put(CarbonLocalProperty.COMMIT_THRESHOLD, "100")

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)

      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")
      val table = CarbonEnv.getCarbonTable(
        Option("default"), s"$tableName")(sqlContext.sparkSession)
      val segmentDir = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_0")
      val dataFiles = segmentDir.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
      })
      assert(dataFiles.length == 10)
      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(500)))
      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")
      val segmentDir2 = FileFactory.getCarbonFile(table.getTablePath + "/Fact/Part0/Segment_1")
      val dataFiles2 = segmentDir2.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".carbondata")
      })
      assert(dataFiles2.length == 10)
      checkAnswer(sql(s"SELECT count(*) FROM $tableName where stringField != 'AAA'"),
        Seq(Row(1000)))
      sql(s"insert into $bucketTableName select * from $tableName").collect()

      val plan = sql(
        s"""
           |select t1.*, t2.*
           |from $tableName t1, $bucketTableName t2
           |where t1.stringField = t2.stringField
      """.stripMargin).queryExecution.executedPlan
      var shuffleExists = false
      plan.collect {
        case s: Exchange if (s.getClass.getName.equals
        ("org.apache.spark.sql.execution.exchange.ShuffleExchange") ||
          s.getClass.getName.equals
          ("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"))
        => shuffleExists = true
      }
      assert(!shuffleExists, "shuffle should not exist on bucket tables")

      checkAnswer(sql(
        s"""select count(*) from
           |(select t1.*, t2.*
           |from $tableName t1, $bucketTableName t2
           |where t1.stringField = t2.stringField) temp
      """.stripMargin), Row(1000))
    }
  }

  test("test insert stage command with secondary index and bloomfilter") {
    createTable
    // create si and bloom index
    sql(s"drop index if exists si_1 on $tableName")
    sql(s"drop index if exists bloom_1 on $tableName")
    sql(s"create index si_1 on $tableName(stringField1) as 'carbondata'")
    sql(s"create index bloom_1 on $tableName(intField) as 'bloomfilter'")
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.enableCheckpointing(2000L)
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)

      // check count before insert stage
      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(0)))
      checkAnswer(sql("select count(*) from si_1"), Seq(Row(0)))

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql("select count(*) from si_1"), Seq(Row(1000)))
      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))
      // check if query hits si
      val df = sql(s"select stringField, intField from $tableName where stringField1 = 'si12'")
      checkAnswer(df, Seq(Row("test12", 12)))
      var isFilterHitSecondaryIndex = false
      df.queryExecution.sparkPlan.transform {
        case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
          isFilterHitSecondaryIndex = true
          broadCastSIFilterPushDown
      }
      assert(isFilterHitSecondaryIndex)

      // check if query hits bloom filter
      checkAnswer(sql(s"select intField,stringField1 from $tableName where intField = 99"),
        Seq(Row(99, "si99")))
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
      val explainBloom =
        sql(s"explain select intField,stringField1 from $tableName where intField = 99").collect()
      assert(explainBloom(0).getString(0).contains(
        """
          |Table Scan on test_flink
          | - total: 1 blocks, 1 blocklets
          | - filter: (intfield is not null and intfield = 99)
          | - pruned by Main Index
          |    - skipped: 0 blocks, 0 blocklets
          | - pruned by CG Index
          |    - name: bloom_1
          |    - provider: bloomfilter
          |    - skipped: 0 blocks, 0 blocklets""".stripMargin))
      checkIfStageFilesAreDeleted(tablePath)
    }
  }

  test("test insert stage command with materilaized view") {
    createTable
    // create materialized view
    sql(s"drop materialized view if exists mv_1")
    sql(s"create materialized view mv_1 " +
        s"as select stringField, shortField from $tableName where intField=99 ")
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.enableCheckpointing(2000L)
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)
      // check count before insert stage
      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(0)))
      checkAnswer(sql(s"SELECT count(*) FROM mv_1"), Seq(Row(0)))

      sql(s"INSERT INTO $tableName STAGE")
      checkAnswer(sql(s"SELECT count(*) FROM mv_1"), Seq(Row(1)))
      val df = sql(s"select stringField, shortField from $tableName where intField=99")
      val tables = df.queryExecution.optimizedPlan collect {
        case l: LogicalRelation => l.catalogTable.get
      }
      assert(tables.exists(_.identifier.table.equalsIgnoreCase("mv_1")))
      checkAnswer(df, Seq(Row("test99", 12345)))
      checkIfStageFilesAreDeleted(tablePath)
    }
  }

  test("Show segments with stage") {
    createTable
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val stagePath = CarbonTablePath.getStageDir(tablePath)
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.enableCheckpointing(2000L)
      executeFlinkStreamingEnvironment(environment, writerProperties, carbonProperties)

      // 1. Test "SHOW SEGMENT ON $tableanme WITH STAGE"
      var rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE").collect()
      var unloadedStageCount = CarbonStore.listStageFiles(stagePath)._1.length
      assert(unloadedStageCount > 0)
      assert(rows.length == unloadedStageCount)
      for (index <- 0 until unloadedStageCount) {
        assert(rows(index).getString(0) == null)
        assert(rows(index).getString(1).equals("Unload"))
        assert(rows(index).getString(2) != null)
        assert(rows(index).getString(3) == null)
        assert(rows(index).getString(4).equals("NA"))
        assert(rows(index).getString(5) != null)
        assert(rows(index).getString(6) != null)
        assert(rows(index).getString(7) == null)
        assertShowStagesCreateTimeDesc(rows, index)
      }

      // 2. Test "SHOW SEGMENT FOR TABLE $tableanme"
      val rowsfortable = sql(s"SHOW SEGMENTS FOR TABLE $tableName WITH STAGE").collect()
      assert(rowsfortable.length > 0)
      assert(rowsfortable.length == rows.length)
      for (index <- 0 until unloadedStageCount) {
        assert(rows(index).toString() == rowsfortable(index).toString())
      }

      // 3. Test "SHOW SEGMENT ON $tableanme WITH STAGE AS (QUERY)"
      rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
        s"(SELECT * FROM $tableName" + "_segments)").collect()
      assert(rows.length > 0)
      for (index <- 0 until rows.length) {
        val row = rows(index)
        assert(rows(index).getString(0) == null)
        assert(rows(index).getString(1).equals("Unload"))
        assert(rows(index).getString(2) != null)
        assert(rows(index).getString(3) == "-1")
        assert(rows(index).get(4).toString.equals("WrappedArray(NA)"))
        assert(rows(index).getString(5) > "0")
        assert(rows(index).getString(6) > "0")
        assert(rows(index).getString(7) == null)
        assert(rows(index).getString(8) == null)
        assert(rows(index).getString(9) == null)
        assert(rows(index).getString(10) == null)
        assert(rows(index).getString(11) == null)
        assertShowStagesCreateTimeDesc(rows, index)
      }

      // 4. Test "SHOW SEGMENT ON $tableanme WITH STAGE LIMIT 1 AS (QUERY)"
      //    Test "SHOW SEGMENT ON $tableanme LIMIT 1 AS (QUERY)"

      if (unloadedStageCount > 1) {
        sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '1')")

        unloadedStageCount = CarbonStore.listStageFiles(stagePath)._1.length
        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE LIMIT 1").collect()
        assert(rows.length == unloadedStageCount + 1)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE LIMIT 0").collect()
        assert(rows.length == unloadedStageCount)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS FOR TABLE $tableName WITH STAGE LIMIT 1").collect()
        assert(rows.length == unloadedStageCount + 1)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE LIMIT 1 AS " +
          s"(SELECT * FROM $tableName" + "_segments)").collect()
        assert(rows.length == unloadedStageCount + 1)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments where status = 'Unload')").collect()
        unloadedStageCount = CarbonStore.listStageFiles(stagePath)._1.length
        assert(rows.length == unloadedStageCount)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments where status = 'Success')").collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1).equals("Success"))
      }
    }
  }

  private def executeFlinkStreamingEnvironment(environment: StreamExecutionEnvironment,
      writerProperties: Properties,
      carbonProperties: Properties): JobExecutionResult = {
    val tablePath = storeLocation + "/" + tableName + "/"
    environment.setParallelism(1)
    environment.setRestartStrategy(RestartStrategies.noRestart)
    val dataCount = 1000
    val source = new TestSource(dataCount) {
      @throws[InterruptedException]
      override def get(index: Int): Array[AnyRef] = {
        Thread.sleep(1L)
        val data = new Array[AnyRef](4)
        data(0) = "test" + index
        data(1) = index.asInstanceOf[AnyRef]
        data(2) = 12345.asInstanceOf[AnyRef]
        data(3) = "si" + index
        data
      }
      @throws[InterruptedException]
      override def onFinish(): Unit = {
        Thread.sleep(5000L)
      }
    }
    val stream = environment.addSource(source)
    val factory = CarbonWriterFactory.builder("Local").build(
      "default",
      tableName,
      tablePath,
      new Properties,
      writerProperties,
      carbonProperties)
    val streamSink = StreamingFileSink
      .forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory)
      .build
    stream.addSink(streamSink)

    try environment.execute
    catch {
      case exception: Exception =>
        // TODO
        throw new UnsupportedOperationException(exception)
    }
  }

  private def checkIfStageFilesAreDeleted(tablePath: String): Unit = {
    // ensure the stage snapshot file and all stage files are deleted
    assertResult(false)(FileFactory.isFileExist(CarbonTablePath.getStageSnapshotFile(tablePath)))
    assertResult(true)(FileFactory
      .getCarbonFile(CarbonTablePath.getStageDir(tablePath))
      .listFiles()
      .isEmpty)
  }

  private def createTable = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(s"""
           | CREATE TABLE $tableName (
           | stringField string, intField int, shortField short, stringField1 string)
           | STORED AS carbondata
      """.stripMargin)
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(s"DROP TABLE IF EXISTS $bucketTableName").collect()
  }

  private def newWriterProperties(dataTempPath: String) = {
    val properties = new Properties
    properties.setProperty(CarbonLocalProperty.DATA_TEMP_PATH, dataTempPath)
    properties
  }

  private def newCarbonProperties(storeLocation: String) = {
    val properties = new Properties
    properties.setProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    properties.setProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    properties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
    properties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024")
    properties
  }

  private def assertShowStagesCreateTimeDesc(rows: Array[Row], index: Int): Unit = {
    if (index > 0) {
      val nowtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
        parse(rows(index).getString(2)).getTime
      val lasttime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
        parse(rows(index - 1).getString(2)).getTime
      assert(nowtime <= lasttime)
    }
  }

  private def getMergeIndexFileCount(tableName: String, segment: String): Int = {
    val table = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    var path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    if (table.isHivePartitionTable) {
      path = table.getAbsoluteTableIdentifier.getTablePath
    }
    val mergeIndexFiles = FileFactory.getCarbonFile(path).listFiles(true, new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean =
        file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
    })
    mergeIndexFiles.size()
  }
}
