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

package org.apache.carbondata.streaming

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.streaming.{CarbonAppendableStreamSink, Sink}
import org.apache.spark.streaming.Time

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Interface used to write stream data to stream table
 * when integrate with Spark Streaming.
 *
 * NOTE: Current integration with Spark Streaming is an alpha feature.
 */
class CarbonStreamSparkStreamingWriter(val sparkSession: SparkSession,
    val carbonTable: CarbonTable,
    val configuration: Configuration) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private var isInitialize: Boolean = false

  private var carbonAppendableStreamSink: Sink = null

  /**
   * unlock for stream table
   */
  def unLockStreamTable(): Unit = {
    StreamSinkFactory.unLock(carbonTable.getTableUniqueName)
    LOGGER.info("unlock for stream table: " +
                carbonTable.getDatabaseName + "." +
                carbonTable.getTableName)
  }

  def initialize(): Unit = {
    carbonAppendableStreamSink = StreamSinkFactory.createStreamTableSink(
      sparkSession,
      configuration,
      carbonTable,
      extraOptions.toMap).asInstanceOf[CarbonAppendableStreamSink]

    isInitialize = true
  }

  def writeStreamData(dataFrame: DataFrame, time: Time): Unit = {
    if (!isInitialize) {
      initialize()
    }
    carbonAppendableStreamSink.addBatch(time.milliseconds, dataFrame)
  }

  private val extraOptions = new scala.collection.mutable.HashMap[String, String]
  private var mode: SaveMode = SaveMode.ErrorIfExists

  this.option("dbName", carbonTable.getDatabaseName)
  this.option("tableName", carbonTable.getTableName)

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `SaveMode.Overwrite`: overwrite the existing data.
   *   - `SaveMode.Append`: append the data.
   *   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
   *   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
   */
  def mode(saveMode: SaveMode): CarbonStreamSparkStreamingWriter = {
    if (mode == SaveMode.ErrorIfExists) {
      mode = saveMode
    }
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error or default`: default option, throw an exception at runtime.
   */
  def mode(saveMode: String): CarbonStreamSparkStreamingWriter = {
    if (mode == SaveMode.ErrorIfExists) {
      mode = saveMode.toLowerCase(util.Locale.ROOT) match {
        case "overwrite" => SaveMode.Overwrite
        case "append" => SaveMode.Append
        case "ignore" => SaveMode.Ignore
        case "error" | "default" => SaveMode.ErrorIfExists
        case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
          "Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
      }
    }
    this
  }

  /**
   * Adds an output option
   */
  def option(key: String, value: String): CarbonStreamSparkStreamingWriter = {
    if (!extraOptions.contains(key)) {
      extraOptions += (key -> value)
    }
    this
  }

  /**
   * Adds an output option
   */
  def option(key: String, value: Boolean): CarbonStreamSparkStreamingWriter =
    option(key, value.toString)

  /**
   * Adds an output option
   */
  def option(key: String, value: Long): CarbonStreamSparkStreamingWriter =
    option(key, value.toString)

  /**
   * Adds an output option
   */
  def option(key: String, value: Double): CarbonStreamSparkStreamingWriter =
    option(key, value.toString)
}

object CarbonStreamSparkStreaming {

  @transient private val tableMap =
    new util.HashMap[String, CarbonStreamSparkStreamingWriter]()

  def getTableMap: util.Map[String, CarbonStreamSparkStreamingWriter] = tableMap

  /**
   * remove all stream lock.
   */
  def cleanAllLockAfterStop(): Unit = {
    tableMap.asScala.values.foreach { writer => writer.unLockStreamTable() }
    tableMap.clear()
  }
}
