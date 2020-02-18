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

import java.util.Properties

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.spark.sql.execution.exchange.Exchange

class TestCarbonWriter extends QueryTest {

  val tableName = "test_flink"
  val bucketTableName = "insert_bucket_table"

  test("Writing flink data to local carbon table") {
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
      val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

      stream.addSink(streamSink)

      try environment.execute
      catch {
        case exception: Exception =>
          // TODO
          throw new UnsupportedOperationException(exception)
      }

      checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(0)))

      // query with stage input
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT, "true")
      checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(1000)))
      sql(s"select * from $tableName limit 10").show
      checkAnswer(sql(s"select max(intField) from $tableName"), Seq(Row(999)))
      checkAnswer(sql(s"select count(intField) from $tableName where intField >= 900"), Seq(Row(100)))
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT, "false")

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))
      checkAnswer(sql(s"select count(intField) from $tableName where intField >= 900"), Seq(Row(100)))

      // ensure the stage snapshot file and all stage files are deleted
      assertResult(false)(FileFactory.isFileExist(CarbonTablePath.getStageSnapshotFile(tablePath)))
      assertResult(true)(FileFactory.getCarbonFile(CarbonTablePath.getStageDir(tablePath)).listFiles().isEmpty)

    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  test("test batch_file_count option") {
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

      writerProperties.put(CarbonLocalProperty.COMMIT_THRESHOLD, "100")

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
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
      val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

      stream.addSink(streamSink)

      try environment.execute
      catch {
        case exception: Exception =>
          // TODO
          throw new UnsupportedOperationException(exception)
      }

      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(500)))

      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  test("test carbon writer of bucket table") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(s"DROP TABLE IF EXISTS $bucketTableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='stringField')
      """.stripMargin
    ).collect()
    sql(
      s"""
         | CREATE TABLE $bucketTableName (stringField string, intField int, shortField short)
         | STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='stringField')
      """.stripMargin
    ).collect()

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val flinkTablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      writerProperties.put(CarbonLocalProperty.COMMIT_THRESHOLD, "100")

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 1000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): Array[AnyRef] = {
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
        flinkTablePath,
        new Properties,
        writerProperties,
        carbonProperties
      )
      val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

      stream.addSink(streamSink)

      try environment.execute
      catch {
        case exception: Exception =>
          throw new UnsupportedOperationException(exception)
      }
      sql(s"INSERT INTO $tableName STAGE OPTIONS ('batch_file_count' = '5')")
      val table = CarbonEnv.getCarbonTable(Option("default"), s"$tableName")(sqlContext.sparkSession)
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
      checkAnswer(sql(s"SELECT count(*) FROM $tableName where stringField != 'AAA'"), Seq(Row(1000)))
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
    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
      sql(s"DROP TABLE IF EXISTS $bucketTableName").collect()
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
