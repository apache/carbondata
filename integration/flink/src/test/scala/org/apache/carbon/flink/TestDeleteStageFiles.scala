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

import java.io.File
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestDeleteStageFiles extends QueryTest {

  val tableName = "test_flink_delete_stage_file"

  test("Delete stage file success") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
      """.stripMargin
    ).collect()

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
          Thread.sleep(1L)
          val data = new Array[AnyRef](3)
          data(0) = "test" + index
          data(1) = index.asInstanceOf[AnyRef]
          data(2) = 12345.asInstanceOf[AnyRef]
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
        carbonProperties
      )
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

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

      // ensure the stage snapshot file and all stage files are deleted
      assertResult(false)(FileFactory.isFileExist(CarbonTablePath.getStageSnapshotFile(tablePath)))
      assertResult(true)(FileFactory
        .getCarbonFile(CarbonTablePath.getStageDir(tablePath))
        .listFiles()
        .isEmpty)

      sql(s"DELETE FROM TABLE $tableName STAGE OPTIONS('retain_hour'='0')")
      val dataLocation = new File(CarbonTablePath.getStageDataDir(tablePath))
      assertResult(true)(dataLocation.listFiles() == null || dataLocation.listFiles().length == 0)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  test("Delete stage file success for partition table") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (intField int, shortField short)
         | STORED AS carbondata
         | PARTITIONED BY (stringField string)
      """.stripMargin
    ).collect()

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
          Thread.sleep(1L)
          val data = new Array[AnyRef](3)
          data(0) = index.asInstanceOf[AnyRef]
          data(1) = 12345.asInstanceOf[AnyRef]
          data(2) = "test" + (index % 10)
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
        carbonProperties
      )
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

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

      // ensure the stage snapshot file and all stage files are deleted
      assertResult(false)(FileFactory.isFileExist(CarbonTablePath.getStageSnapshotFile(tablePath)))
      assertResult(true)(FileFactory
        .getCarbonFile(CarbonTablePath.getStageDir(tablePath))
        .listFiles()
        .isEmpty)

      sql(s"DELETE FROM TABLE $tableName STAGE OPTIONS('retain_hour'='0')")
      val dataLocation = new File(CarbonTablePath.getStageDataDir(tablePath))
      assertResult(true)(dataLocation.listFiles() == null || dataLocation.listFiles().length == 0)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  test("All data files are referenced by stage, not delete any data files.") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
      """.stripMargin
    ).collect()

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
          Thread.sleep(1L)
          val data = new Array[AnyRef](3)
          data(0) = "test" + index
          data(1) = index.asInstanceOf[AnyRef]
          data(2) = 12345.asInstanceOf[AnyRef]
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
        carbonProperties
      )
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

      sql(s"DELETE FROM TABLE $tableName STAGE")
      val dataLocation = new File(CarbonTablePath.getStageDataDir(tablePath))
      assertResult(true)(dataLocation.listFiles().length > 0)
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  private def newWriterProperties(
    dataTempPath: String,
    storeLocation: String) = {
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

}
