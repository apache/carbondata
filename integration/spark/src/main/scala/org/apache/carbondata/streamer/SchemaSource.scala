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

import java.io.FileInputStream
import java.net.URL

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.Schema

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

/**
 * The Schema Source class to read the schema files.
 */
abstract class SchemaSource {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def getSchema: Schema

}

/**
 * Reads schema from the Schema Registry when the schema provider type is SchemaRegistry.
 */
case class SchemaRegistry() extends SchemaSource {
  override
  def getSchema: Schema = {
    var schemaRegistryURL = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_SCHEMA_REGISTRY_URL)
    val topicName = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_KAFKA_INPUT_TOPIC)
    val topics = topicName.split(CarbonCommonConstants.COMMA).map(_.trim)
    val topicToConsider = if (topics.length > 0) {
      topics(0)
    } else {
      topicName
    }
    schemaRegistryURL = s"$schemaRegistryURL/subjects/$topicToConsider-value/versions/latest"
    val registry = new URL(schemaRegistryURL)
    val connection = registry.openConnection
    val mapper = new ObjectMapper
    val node = mapper.readTree(connection.getInputStream)
    if (!node.elements().hasNext) {
      throw new CarbonDataStreamerException(
        "The Schema registry URL is not valid, please check and retry.")
    }
    new Schema.Parser().parse(node.get("schema").asText)
  }
}

/**
 * Reads schema from the directory or filepath provider by user when the schema provider type is
 * FileSchema.
 */
case class FileSchema() extends SchemaSource {
  override
  def getSchema: Schema = {
    val schemaPath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_SOURCE_SCHEMA_PATH)
    LOGGER.info(s"Reading the schema file from the path: $schemaPath")
    val updatedSchemaFilePath = if (FileFactory.getCarbonFile(schemaPath).isDirectory) {
      val files = FileFactory.getCarbonFile(schemaPath)
        .listFiles(new CarbonFileFilter {
          override def accept(file: CarbonFile): Boolean = {
            !file.isDirectory && file.getName.endsWith(".avsc")
          }
        })
      (files max Ordering[Long].on { file: CarbonFile => file.getLastModifiedTime }).getAbsolutePath
    } else {
      schemaPath
    }
    var inputStream: FileInputStream = null
    val jsonSchema = try {
      inputStream = new FileInputStream(updatedSchemaFilePath)
      val mapper = new ObjectMapper
      mapper.readTree(inputStream)
    } catch {
      case ex: Exception =>
        LOGGER.error("Read schema failed in File based Schema provider, ", ex)
        throw ex
    } finally {
      CarbonUtil.closeStream(inputStream)
    }
    new Schema.Parser().parse(jsonSchema.asInstanceOf[JsonNode].toString)
  }
}
