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
package org.apache.carbondata.streamer

import com.beust.jcommander.JCommander
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Carbondata streamer, which is a spark streaming application to pull data from different
 * sources and merge onto target cabondata table.
 */
object CarbonDataStreamer {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def createConfig(streamerConfig: CarbonStreamerConfig,
      args: Array[String]): Unit = {
    JCommander.newBuilder().addObject(streamerConfig).build().parse(args: _*)
  }

  /**
   * This method creates streaming context for the first time if no checkpoint directory present for
   * the table.
   *
   * @param sparkSession          Spark Session.
   * @param targetCarbonDataTable target carbondata table to merge.
   * @return Spark StreamingContext
   */
  def createStreamingContext(sparkSession: SparkSession,
      targetCarbonDataTable: CarbonTable): StreamingContext = {
    val batchDuration = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL,
        CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL_DEFAULT).toLong
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))

    // get the source Dstream based on source type
    val sourceType = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE,
        CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE_DEFAULT)
    val sourceCarbonDStream = SourceFactory.apply(sourceType,
      ssc,
      sparkSession,
      targetCarbonDataTable)
    // Perform merge on source stream
    SourceFactory.source.prepareDFAndMerge(sourceCarbonDStream)
    // set the checkpoint directory for spark streaming
    ssc.checkpoint(CarbonTablePath.getStreamingCheckpointDir(targetCarbonDataTable.getTablePath))
    ssc
  }

  def main(args: Array[String]): Unit = {
    // parse the incoming arguments and prepare the configurations
    val streamerConfigs = new CarbonStreamerConfig()
    createConfig(streamerConfigs, args)
    streamerConfigs.setConfigsToCarbonProperty(streamerConfigs)

    val spark = SparkSession
      .builder()
      .master(streamerConfigs.sparkMaster)
      .appName("CarbonData Streamer tool")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .config("spark.streaming.driver.writeAheadLog.allowBatching", "true")
      .config("spark.streaming.driver.writeAheadLog.batchingTimeout", 15000)
      .enableHiveSupport()
      .getOrCreate()
    CarbonEnv.getInstance(spark)

    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    val targetTableName = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_TABLE_NAME)

    var databaseName = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_DATABASE_NAME)
    databaseName = if (databaseName.equalsIgnoreCase("")) {
      spark.sessionState.catalog.getCurrentDatabase
    } else {
      databaseName
    }

    // if the target table is non-carbondata table, throw exception
    if (!CarbonPlanHelper.isCarbonTable(TableIdentifier(targetTableName, Some(databaseName)))) {
      throw new UnsupportedOperationException("The merge operation using CarbonData Streamer tool" +
                                              " for non carbondata table is not supported.")
    }

    val targetCarbonDataTable = CarbonEnv.getCarbonTable(Some(databaseName), targetTableName)(spark)
    val dbAndTb = targetCarbonDataTable.getQualifiedName
    val segmentProperties = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
    if (!(segmentProperties.equals("") || segmentProperties.trim.equals("*"))) {
      throw new CarbonDataStreamerException(
        s"carbon.input.segments.$dbAndTb should not be set for table during merge operation. " +
        s"Please reset the property to carbon.input.segments.dbAndTb=*")
    }

    val ssc = StreamingContext.getOrCreate(CarbonTablePath.getStreamingCheckpointDir(
      targetCarbonDataTable.getTablePath),
      () => createStreamingContext(spark, targetCarbonDataTable))

    try {
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case ex: Exception =>
        LOGGER.error("streaming failed. Stopping the streaming application gracefully.", ex)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }

  }
}
