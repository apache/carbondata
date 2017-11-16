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

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CarbonAppendableStreamSink, Sink}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.hadoop.streaming.CarbonStreamOutputFormat
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.DataLoadingUtil
import org.apache.carbondata.streaming.segment.StreamSegment

/**
 * Stream sink factory
 */
object StreamSinkFactory {

  def createStreamTableSink(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      parameters: Map[String, String]): Sink = {
    validateParameters(parameters)

    // prepare the stream segment
    val segmentId = getStreamSegmentId(carbonTable)
    // build load model
    val carbonLoadModel = buildCarbonLoadModelForStream(
      sparkSession,
      carbonTable,
      parameters,
      segmentId)
    // start server if necessary
    val server = startDictionaryServer(
      sparkSession,
      carbonTable,
      carbonLoadModel.getDictionaryServerPort)
    if (server.isDefined) {
      carbonLoadModel.setUseOnePass(true)
      carbonLoadModel.setDictionaryServerPort(server.get.getPort)
    } else {
      carbonLoadModel.setUseOnePass(false)
    }
    // default is carbon appended stream sink
    new CarbonAppendableStreamSink(
      sparkSession,
      carbonTable,
      segmentId,
      parameters,
      carbonLoadModel,
      server)
  }

  private def validateParameters(parameters: Map[String, String]): Unit = {
    val segmentSize = parameters.get(CarbonStreamOutputFormat.HANDOFF_SIZE)
    if (segmentSize.isDefined) {
      try {
        val value = java.lang.Long.parseLong(segmentSize.get)
        if (value < CarbonStreamOutputFormat.HANDOFF_SIZE_MIN) {
          new CarbonStreamException(CarbonStreamOutputFormat.HANDOFF_SIZE +
                                    "should be bigger than or equal " +
                                    CarbonStreamOutputFormat.HANDOFF_SIZE_MIN)
        }
      } catch {
        case ex: NumberFormatException =>
          new CarbonStreamException(CarbonStreamOutputFormat.HANDOFF_SIZE +
                                    s" $segmentSize is an illegal number")
      }
    }
  }

  /**
   * get current stream segment id
   * @return
   */
  private def getStreamSegmentId(carbonTable: CarbonTable): String = {
    val segmentId = StreamSegment.open(carbonTable)
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val segmentDir = carbonTablePath.getSegmentDir("0", segmentId)
    val fileType = FileFactory.getFileType(segmentDir)
    if (FileFactory.isFileExist(segmentDir, fileType)) {
      // recover fault
      StreamSegment.recoverSegmentIfRequired(segmentDir)
    } else {
      FileFactory.mkdirs(segmentDir, fileType)
    }
    segmentId
  }

  def startDictionaryServer(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      port: Int): Option[DictionaryServer] = {
    // start dictionary server when use one pass load and dimension with DICTIONARY
    // encoding is present.
    val allDimensions = carbonTable.getAllDimensions.asScala.toList
    val createDictionary = allDimensions.exists {
      carbonDimension => carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
                         !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
    }
    val server: Option[DictionaryServer] = if (createDictionary) {
      val dictionaryServer = DictionaryServer.getInstance(port, carbonTable)
      sparkSession.sparkContext.addSparkListener(new SparkListener() {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
          dictionaryServer.shutdown()
        }
      })
      Some(dictionaryServer)
    } else {
      None
    }
    server
  }

  private def buildCarbonLoadModelForStream(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      parameters: Map[String, String],
      segmentId: String): CarbonLoadModel = {
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val optionsFinal = DataLoadingUtil.getDataLoadingOptions(carbonProperty, parameters)
    optionsFinal.put("sort_scope", "no_sort")
    if (parameters.get("fileheader").isEmpty) {
      optionsFinal.put("fileheader", carbonTable.getCreateOrderColumn(carbonTable.getTableName)
        .asScala.map(_.getColName).mkString(","))
    }
    val carbonLoadModel = new CarbonLoadModel()
    DataLoadingUtil.buildCarbonLoadModel(
      carbonTable,
      carbonProperty,
      parameters,
      optionsFinal,
      carbonLoadModel
    )
    carbonLoadModel.setSegmentId(segmentId)
    // stream should use one pass
    val dictionaryServerPort = parameters.getOrElse(
      CarbonCommonConstants.DICTIONARY_SERVER_PORT,
      carbonProperty.getProperty(
        CarbonCommonConstants.DICTIONARY_SERVER_PORT,
        CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT))
    val sparkDriverHost = sparkSession.sqlContext.sparkContext.
      getConf.get("spark.driver.host")
    carbonLoadModel.setDictionaryServerHost(sparkDriverHost)
    carbonLoadModel.setDictionaryServerPort(dictionaryServerPort.toInt)
    carbonLoadModel
  }
}
