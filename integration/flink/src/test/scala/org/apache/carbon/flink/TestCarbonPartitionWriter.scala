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

class TestCarbonPartitionWriter extends QueryTest {

  val tableName = "test_flink_partition"

  test("Writing flink data to local partition carbon table") {
    sql(s"DROP TABLE IF EXISTS $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
         | PARTITIONED BY (hour_ string, date_ string)
         | TBLPROPERTIES ('SORT_COLUMNS'='hour_,date_,stringField', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin
    ).collect()

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
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
          data(3) = Integer.toString(TestSource.randomCache.get().nextInt(24))
          data(4) = "20191218"
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

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

    } finally {
      sql(s"DROP TABLE IF EXISTS $tableName").collect()
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

    val rootPath = System.getProperty("user.dir") + "/target/test-classes"

    val dataTempPath = rootPath + "/data/temp/"

    try {
      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, storeLocation)
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

      sql(s"INSERT INTO $tableName STAGE")

      checkAnswer(sql(s"SELECT count(1) FROM $tableName"), Seq(Row(1000)))

      val rows = sql(s"SELECT * FROM $tableName limit 1").collect()
      assertResult(1)(rows.length)
      assertResult(Array[Byte](2, 3, 4))(rows(0).get(rows(0).fieldIndex("binaryfield")).asInstanceOf[GenericRowWithSchema](0))

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
