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
import java.util.{Base64, Properties}
import java.util.concurrent.Executors

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestCarbonPartitionWriter extends QueryTest with BeforeAndAfterAll {

  val tableName = "test_flink_partition"
  val dataTempPath = targetTestClass + "/data/temp/"

  test("Writing flink data to local partition carbon table") {
    createPartitionTable
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(6)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = getTestSource(dataCount)
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))
    }
  }

  test("Show segments with stage") {
    createPartitionTable
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val tableStagePath = CarbonTablePath.getStageDir(tablePath)
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.enableCheckpointing(2000L)
      val dataCount = 1000
      val source = getTestSource(dataCount)
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      // 1. Test "SHOW SEGMENT ON $tableanme WITH STAGE"
      var rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE").collect()
      var unloadedStageCount = CarbonStore.listStageFiles(tableStagePath)._1.length
      assert(unloadedStageCount > 0)
      assert(rows.length == unloadedStageCount)
      for (index <- 0 until unloadedStageCount) {
        assert(rows(index).getString(0) == null)
        assert(rows(index).getString(1).equals("Unload"))
        assert(rows(index).getString(2) != null)
        assert(rows(index).getString(3) == null)
        assert(!rows(index).getString(4).equals("NA"))
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
      for (index <- 0 until unloadedStageCount) {
        assert(rows(index).getString(0) == null)
        assert(rows(index).getString(1).equals("Unload"))
        assert(rows(index).getString(2) != null)
        assert(rows(index).getString(3) == "-1")
        assert(!rows(index).get(4).toString.equals("WrappedArray(NA)"))
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

        unloadedStageCount = CarbonStore.listStageFiles(tableStagePath)._1.length
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

        unloadedStageCount = CarbonStore.listStageFiles(tableStagePath)._1.length
        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments where status = 'Unload')").collect()
        assert(rows.length == unloadedStageCount)
        assert(rows(0).getString(1).equals("Unload"))

        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments where status = 'Success')").collect()
        assert(rows.length >= 1)
        assert(rows(0).getString(1).equals("Success"))

        // createFakeLoadingStage
        createFakeLoadingStage(CarbonTablePath.getStageDir(tablePath))
        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments where status = 'Loading')").collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1).equals("Loading"))

        var (unloadedFiles, loadingFiles) = CarbonStore.listStageFiles(tableStagePath)
        rows = sql(s"SHOW SEGMENTS ON $tableName WITH STAGE AS " +
          s"(SELECT * FROM $tableName" + "_segments " +
          "where status = 'Unload' or status = 'Loading')").collect()
        assert(rows.length == unloadedFiles.length + loadingFiles.length)
      }
    }
  }

  test("test concurrent insertstage") {
    createPartitionTable
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(6)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = getTestSource(dataCount)
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      Thread.sleep(5000)
      val executorService = Executors.newFixedThreadPool(10)
      for(i <- 1 to 10) {
        executorService.submit(new Runnable {
          override def run(): Unit = {
            sql(s"INSERT INTO $tableName STAGE OPTIONS('batch_file_count'='5')")
          }
        }).get()
      }
      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))
      executorService.shutdownNow()
    }
  }

  test("Test complex type") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short,
         | structField struct<value1:string,value2:int,value3:int>,
         | binaryField struct<value1:binary>)
         | STORED AS carbondata
         | PARTITIONED BY (hour_ string, date_ string)
         | TBLPROPERTIES ('SORT_COLUMNS'='hour_,date_,stringField', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin
    ).collect()
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(6)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
          val data = new Array[AnyRef](7)
          data(0) = "test" + index
          data(1) = index.asInstanceOf[AnyRef]
          data(2) = 12345.asInstanceOf[AnyRef]
          data(3) = "test\0011\0012"
          data(4) = Base64.getEncoder.encodeToString(Array[Byte](2, 3, 4))
          data(5) = Integer.toString(TestSource.randomCache.get().nextInt(24))
          data(6) = "20191218"
          data
        }

        @throws[InterruptedException]
        override def onFinish(): Unit = {
          Thread.sleep(5000L)
        }
      }
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

      checkAnswer(sql(s"SELECT stringField FROM $tableName order by stringField limit 2"),
        Seq(Row("test0"), Row("test1")))

      val rows = sql(s"SELECT * FROM $tableName limit 1").collect()
      assertResult(1)(rows.length)
      assertResult(Array[Byte](2, 3, 4))(rows(0).get(rows(0).fieldIndex("binaryfield"))
        .asInstanceOf[GenericRowWithSchema](0))

    }
  }

  test("test insert stage into partition carbon table with secondary index") {
    createPartitionTable
    // create si index
    sql(s"drop index if exists si_1 on $tableName")
    sql(s"create index si_1 on $tableName(stringField1) as 'carbondata'")
    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(6)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 10
      val source = getTestSource(dataCount)
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql("select count(*) from si_1"), Seq(Row(10))  )
      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(10)))
      // check if query hits si
      val df = sql(s"select stringField, intField from $tableName where stringField1 = 'si1'")
      checkAnswer(df, Seq(Row("test1", 1)))
      var isFilterHitSecondaryIndex = false
      df.queryExecution.sparkPlan.transform {
        case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
          isFilterHitSecondaryIndex = true
          broadCastSIFilterPushDown
      }
      assert(isFilterHitSecondaryIndex)
    }
  }

  test("test insert stage into partition carbon table with materialized view") {
    createPartitionTable
    // create materialized view
    sql(s"drop materialized view if exists mv_1")
    sql("create materialized view mv_1 " +
        s"as select stringField, shortField from $tableName where intField=9")

    try {
      val tablePath = storeLocation + "/" + tableName + "/"
      val writerProperties = newWriterProperties(dataTempPath)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(6)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 10
      val source = getTestSource(dataCount)
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment,
        source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(*) FROM mv_1"), Seq(Row(1)))
      val df = sql(s"select stringField, shortField from $tableName where intField=9")
      val tables = df.queryExecution.optimizedPlan collect {
        case l: LogicalRelation => l.catalogTable.get
      }
      assert(tables.exists(_.identifier.table.equalsIgnoreCase("mv_1")))
      checkAnswer(df, Seq(Row("test9", 12345)))

    }
  }

  private def getTestSource(dataCount: Int): TestSource = {
    new TestSource(dataCount) {
      @throws[InterruptedException]
      override def get(index: Int): Array[AnyRef] = {
        val data = new Array[AnyRef](7)
        data(0) = "test" + index
        data(1) = index.asInstanceOf[AnyRef]
        data(2) = 12345.asInstanceOf[AnyRef]
        data(3) = "si" + index
        data(4) = Integer.toString(TestSource.randomCache.get().nextInt(24))
        data
      }

      @throws[InterruptedException]
      override def onFinish(): Unit = {
        Thread.sleep(5000L)
      }
    }
  }

  private def createPartitionTable = {
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (
         | stringField string, intField int, shortField short, stringField1 string)
         | STORED AS carbondata
         | PARTITIONED BY (hour_ string)
         | TBLPROPERTIES ('SORT_COLUMNS'='hour_,stringField', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  private def executeStreamingEnvironment(tablePath: String,
      writerProperties: Properties,
      carbonProperties: Properties,
      environment: StreamExecutionEnvironment,
      source: TestSource): Unit = {
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

    stream.keyBy(new KeySelector[Array[AnyRef], AnyRef] {
      override def getKey(value: Array[AnyRef]): AnyRef = value(3) // return hour_
    }).addSink(streamSink)

    try environment.execute
    catch {
      case exception: Exception =>
        // TODO
        throw new UnsupportedOperationException(exception)
    }
    assertResult(false)(FileFactory
      .getCarbonFile(CarbonTablePath.getStageDir(tablePath)).listFiles().isEmpty)
  }

  private def newWriterProperties(
     dataTempPath: String) = {
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
    properties.setProperty("binary_decoder", "base64")
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

  private def createFakeLoadingStage(stagePath: String): Unit = {
    var (unloadedFiles, loadingFiles) = CarbonStore.listStageFiles(stagePath)
    assert(unloadedFiles.length > 0)
    val loadingFilesCountBefore = loadingFiles.length
    FileFactory.getCarbonFile(unloadedFiles(0).getAbsolutePath +
      CarbonTablePath.LOADING_FILE_SUFFIX).createNewFile()
    loadingFiles = CarbonStore.listStageFiles(stagePath)._2
    val loadingFilesCountAfter = loadingFiles.length
    assert(loadingFilesCountAfter == loadingFilesCountBefore + 1)
  }
}
