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
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest

import org.junit.Test

class TestCarbonWriter extends QueryTest {

  val tableName = "test_flink"

  @Test
  def testLocal(): Unit = {
    sql(s"drop table if exists $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
      """.stripMargin
    ).collect()

    try {
      val rootPath = System.getProperty("user.dir") + "/target/test-classes"

      val dataTempPath = rootPath + "/data/temp/"
      val dataPath = rootPath + "/data/"

      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, dataPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 10000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): String = {
          Thread.sleep(1L)
          "{\"stringField\": \"test" + index + "\", \"intField\": " + index + ", \"shortField\": 12345}"
        }

        @throws[InterruptedException]
        override def onFinish(): Unit = {
          Thread.sleep(10000L)
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
    } finally {
//      sql(s"drop table if exists $tableName").collect()
    }
  }

  private def newWriterProperties(
                                   dataTempPath: String,
                                   dataPath: String,
                                   storeLocation: String) = {
    val properties = new Properties
    properties.setProperty(CarbonLocalProperty.DATA_TEMP_PATH, dataTempPath)
    properties.setProperty(CarbonLocalProperty.DATA_PATH, dataPath)
    properties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
    properties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024")
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
