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

import java.io.{File, InputStreamReader}
import java.util
import java.util.{Base64, Collections, Properties}

import com.google.gson.Gson

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.StageInput
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

class TestCarbonPartitionWriter extends QueryTest with BeforeAndAfterAll{

  val tableName = "test_flink_partition"
  val rootPath = System.getProperty("user.dir") + "/target/test-classes"
  val dataTempPath = rootPath + "/data/temp/"

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
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment, source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

    }
  }

  test("Test complex type") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short,
         | structField struct<value1:string,value2:int,value3:int>, binaryField struct<value1:binary>)
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
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment, source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

      checkAnswer(sql(s"SELECT stringField FROM $tableName order by stringField limit 2"),
        Seq(Row("test0"), Row("test1")))

      val rows = sql(s"SELECT * FROM $tableName limit 1").collect()
      assertResult(1)(rows.length)
      assertResult(Array[Byte](2, 3, 4))(rows(0).get(rows(0).fieldIndex("binaryfield")).asInstanceOf[GenericRowWithSchema](0))

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
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment, source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql("select count(*) from si_1"), Seq(Row(10))  )
      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(10)))
      // check if query hits si
      val df = sql(s"select count(intField) from $tableName where stringField1 = 'si1'")
      checkAnswer(df, Seq(Row(1)))
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
    sql(s"create materialized view mv_1 as select stringField, shortField from $tableName where intField=9")

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
      executeStreamingEnvironment(tablePath, writerProperties, carbonProperties, environment, source)

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(*) FROM mv_1"), Seq(Row(1)))
      val df = sql(s"select stringField, shortField from $tableName where intField=9")
      val tables = df.queryExecution.optimizedPlan collect {
        case l: LogicalRelation => l.catalogTable.get
      }
      assert(tables.exists(_.identifier.table.equalsIgnoreCase("mv_1")))
      checkAnswer(df, Seq(Row("test9",12345)))

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
        data(5) = "20191218"
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
         | CREATE TABLE $tableName (stringField string, intField int, shortField short, stringField1 string)
         | STORED AS carbondata
         | PARTITIONED BY (hour_ string, date_ string)
         | TBLPROPERTIES ('SORT_COLUMNS'='hour_,date_,stringField', 'SORT_SCOPE'='GLOBAL_SORT')
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
    val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

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

  private def collectStageInputs(loadDetailsDir: String): Seq[StageInput] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir)
    val stageFiles = if (dir.exists()) {
      val allFiles = dir.listFiles()
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap
      allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.map { file =>
        (file, successFiles(file.getName))
      }
    } else {
      Array.empty
    }

    val output = Collections.synchronizedList(new util.ArrayList[StageInput]())
    val gson = new Gson()
    stageFiles.map { stage =>
      val filePath = stage._1.getAbsolutePath
      val stream = FileFactory.getDataInputStream(filePath)
      try {
        val stageInput = gson.fromJson(new InputStreamReader(stream), classOf[StageInput])
        output.add(stageInput)
      } finally {
        stream.close()
      }
    }
    output.asScala
  }

  private def delDir(dir: File): Boolean = {
    if (dir.isDirectory) {
      val children = dir.list
      if (children != null) {
        val length = children.length
        var i = 0
        while (i < length) {
          if (!delDir(new File(dir, children(i)))) {
              return false
          }
          i += 1
        }
      }
    }
    dir.delete()
  }

}
