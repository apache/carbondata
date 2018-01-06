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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CarbonAppendableStreamSink, Sink}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.{DictionaryServer, NonSecureDictionaryServer}
import org.apache.carbondata.core.dictionary.service.NonSecureDictionaryServiceProvider
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.dictionary.provider.SecureDictionaryServiceProvider
import org.apache.carbondata.spark.dictionary.server.SecureDictionaryServer
import org.apache.carbondata.spark.util.DataLoadingUtil
import org.apache.carbondata.streaming.segment.StreamSegment

/**
 * Stream sink factory
 */
object StreamSinkFactory {

  def createStreamTableSink(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      carbonTable: CarbonTable,
      parameters: Map[String, String]): Sink = {
    validateParameters(parameters)

    // build load model
    val carbonLoadModel = buildCarbonLoadModelForStream(
      sparkSession,
      hadoopConf,
      carbonTable,
      parameters,
      "")
    // fire pre event before streamin is started
    // in case of streaming options and optionsFinal can be same
    val operationContext = new OperationContext
    val loadTablePreExecutionEvent = new LoadTablePreExecutionEvent(
      carbonTable.getCarbonTableIdentifier,
      carbonLoadModel,
      carbonLoadModel.getFactFilePath,
      false,
      parameters.asJava,
      parameters.asJava,
      false
    )
    OperationListenerBus.getInstance().fireEvent(loadTablePreExecutionEvent, operationContext)
    // prepare the stream segment
    val segmentId = getStreamSegmentId(carbonTable)
    carbonLoadModel.setSegmentId(segmentId)

    // start server if necessary
    val server = startDictionaryServer(
      sparkSession,
      carbonTable,
      carbonLoadModel)
    if (server.isDefined) {
      carbonLoadModel.setUseOnePass(true)
    } else {
      carbonLoadModel.setUseOnePass(false)
    }
    // default is carbon appended stream sink
    val carbonAppendableStreamSink = new CarbonAppendableStreamSink(
      sparkSession,
      carbonTable,
      segmentId,
      parameters,
      carbonLoadModel,
      server)

    // fire post event before streamin is started
    val loadTablePostExecutionEvent = new LoadTablePostExecutionEvent(
      carbonTable.getCarbonTableIdentifier,
      carbonLoadModel
    )
    OperationListenerBus.getInstance().fireEvent(loadTablePostExecutionEvent, operationContext)
    carbonAppendableStreamSink
  }

  private def validateParameters(parameters: Map[String, String]): Unit = {
    val segmentSize = parameters.get(CarbonCommonConstants.HANDOFF_SIZE)
    if (segmentSize.isDefined) {
      try {
        val value = java.lang.Long.parseLong(segmentSize.get)
        if (value < CarbonCommonConstants.HANDOFF_SIZE_MIN) {
          new CarbonStreamException(CarbonCommonConstants.HANDOFF_SIZE +
                                    "should be bigger than or equal " +
                                    CarbonCommonConstants.HANDOFF_SIZE_MIN)
        }
      } catch {
        case _: NumberFormatException =>
          new CarbonStreamException(CarbonCommonConstants.HANDOFF_SIZE +
                                    s" $segmentSize is an illegal number")
      }
    }
  }

  /**
   * get current stream segment id
   * @return
   */
  private def getStreamSegmentId(carbonTable: CarbonTable): String = {
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val fileType = FileFactory.getFileType(carbonTablePath.getMetadataDirectoryPath)
    if (!FileFactory.isFileExist(carbonTablePath.getMetadataDirectoryPath, fileType)) {
      // Create table directory path, in case of enabling hive metastore first load may not have
      // table folder created.
      FileFactory.mkdirs(carbonTablePath.getMetadataDirectoryPath, fileType)
    }
    val segmentId = StreamSegment.open(carbonTable)
    val segmentDir = carbonTablePath.getSegmentDir(segmentId)
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
      carbonLoadModel: CarbonLoadModel): Option[DictionaryServer] = {
    // start dictionary server when use one pass load and dimension with DICTIONARY
    // encoding is present.
    val allDimensions = carbonTable.getAllDimensions.asScala.toList
    val createDictionary = allDimensions.exists {
      carbonDimension => carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
                         !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
    }
    val carbonSecureModeDictServer = CarbonProperties.getInstance.
      getProperty(CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER,
        CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER_DEFAULT)

    val sparkConf = sparkSession.sqlContext.sparkContext.getConf
    val sparkDriverHost = sparkSession.sqlContext.sparkContext.
      getConf.get("spark.driver.host")

    val server: Option[DictionaryServer] = if (createDictionary) {
      if (sparkConf.get("spark.authenticate", "false").equalsIgnoreCase("true") &&
          carbonSecureModeDictServer.toBoolean) {
        val dictionaryServer = SecureDictionaryServer.getInstance(sparkConf,
          sparkDriverHost.toString, carbonLoadModel.getDictionaryServerPort, carbonTable)
        carbonLoadModel.setDictionaryServerPort(dictionaryServer.getPort)
        carbonLoadModel.setDictionaryServerHost(dictionaryServer.getHost)
        carbonLoadModel.setDictionaryServerSecretKey(dictionaryServer.getSecretKey)
        carbonLoadModel.setDictionaryEncryptServerSecure(dictionaryServer.isEncryptSecureServer)
        carbonLoadModel.setDictionaryServiceProvider(new SecureDictionaryServiceProvider())
        sparkSession.sparkContext.addSparkListener(new SparkListener() {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
            dictionaryServer.shutdown()
          }
        })
        Some(dictionaryServer)
      } else {
        val dictionaryServer = NonSecureDictionaryServer
          .getInstance(carbonLoadModel.getDictionaryServerPort, carbonTable)
        carbonLoadModel.setDictionaryServerPort(dictionaryServer.getPort)
        carbonLoadModel.setDictionaryServerHost(dictionaryServer.getHost)
        carbonLoadModel.setDictionaryEncryptServerSecure(false)
        carbonLoadModel
          .setDictionaryServiceProvider(new NonSecureDictionaryServiceProvider(dictionaryServer
            .getPort))
        sparkSession.sparkContext.addSparkListener(new SparkListener() {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
            dictionaryServer.shutdown()
          }
        })
        Some(dictionaryServer)
      }
    } else {
      None
    }
    server
  }

  private def buildCarbonLoadModelForStream(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
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
      carbonLoadModel,
      hadoopConf)
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
