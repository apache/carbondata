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

import com.beust.jcommander.Parameter

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * The config class to parse the program arguments, validate and prepare the required configuration.
 */
class CarbonStreamerConfig() extends Serializable {

  @Parameter(names = Array("--master"), description = "Spark master")
  var sparkMaster: String = "local[2]"

  @Parameter(names = Array("--target-table"),
    description = "The target carbondata table where the data has to be merged. If this is not " +
                  "configured by user, the operation will fail.",
    required = true)
  var targetTableName: String = ""

  @Parameter(names = Array("--database-name"),
    description = "The database name where the target table is present to merge the incoming data" +
                  ". If not given by user, system will take the current database in the spark " +
                  "session.",
    required = false)
  var databaseName: String = ""

  @Parameter(names = Array("--source-type"),
    description = "Source type to ingest data from. It can be kafka or DFS",
    required = false)
  var sourceType: String = CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE_DEFAULT

  @Parameter(names = Array("--dfs-source-input-path"),
    description = "An absolute path on a given file system from where data needs to be read to " +
                  "ingest into the target carbondata table. Mandatory if the ingestion source " +
                  "type is DFS.",
    required = false)
  var dfsSourceInputPth: String = ""

  // ----------- kafka properties ----------------------
  @Parameter(names = Array("--input-kafka-topic"),
    description = "Kafka topics to consume data from. Mandatory if Kafka is selected as the " +
                  "ingestion source. If multiple topic are present, the varue of the property can" +
                  " be comma separated topic names. If not present in case of kafka source, " +
                  "operation will fail.",
    required = false)
  var inputKafkaTopic: String = ""

  @Parameter(names = Array("--brokers"),
    description = "Kafka brokers to connect to in case Kafka is selected as an ingestion source. " +
                  "If not present in case of kafka source, operation will fail.",
    required = false)
  var kafkaBrokerList: String = ""

  @Parameter(names = Array("--kafka-initial-offset-type"),
    description = "Kafka offset to fall back to in case no checkpoint is available for starting " +
                  "ingestion. Valid values - Latest and Earliest.",
    required = false)
  var kafkaInitialOffsetType: String = CarbonCommonConstants
    .CARBON_STREAMER_KAFKA_INITIAL_OFFSET_TYPE_DEFAULT

  @Parameter(names = Array("--schema-registry-url"),
    description = "Schema registry url, in case schema registry is selected as schema provider.",
    required = false)
  var schemaRegistryURL: String = ""

  @Parameter(names = Array("--group-id"),
    description = "This property is required if the consumer uses either the group management " +
                  "functionality by using subscribe(topic) or the Kafka-based offset management " +
                  "strategy.",
    required = false)
  var groupId: String = ""

  // -------------------------------------------------------------------- //

  @Parameter(names = Array("--input-payload-format"),
    description = "Format of the incoming data/payload.",
    required = false)
  var inputPayloadFormat: String = CarbonCommonConstants
    .CARBON_STREAMER_INPUT_PAYLOAD_FORMAT_DEFAULT

  @Parameter(names = Array("--schema-provider-type"),
    description = "Schema provider for the incoming batch of data. Currently, 2 types of schema " +
                  "providers are supported - FileBasedProvider and SchemaRegistryProvider",
    required = false)
  var schemaProviderType: String = CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER_DEFAULT

  @Parameter(names = Array("--source-schema-file-path"),
    description = "Absolute Path to file containing the schema of incoming data. Mandatory if " +
                  "file-based schema provider is selected.",
    required = false)
  var sourceSchemaFilePath: String = ""

  @Parameter(names = Array("--merge-operation-type"),
    description = "Different merge operations are supported - INSERT, UPDATE, DELETE, UPSERT",
    required = false)
  var mergeOperationType: String = CarbonCommonConstants
    .CARBON_STREAMER_MERGE_OPERATION_TYPE_DEFAULT

  @Parameter(names = Array("--delete-operation-field"),
    description = "Name of the field in source schema reflecting the IUD operation types on " +
                  "source data rows.",
    required = false)
  var deleteOperationField: String = ""

  @Parameter(names = Array("--delete-field-value"),
    description = "Name of the field in source schema reflecting the IUD operation types on " +
                  "source data rows.",
    required = false)
  var deleteFieldValue: String = ""

  @Parameter(names = Array("--source-ordering-field"),
    description = "Name of the field from source schema whose value can be used for picking the " +
                  "latest updates for a particular record in the incoming batch in case of " +
                  "duplicates record keys. Useful if the write operation type is UPDATE or UPSERT" +
                  ". This will be used only if carbon.streamer.upsert.deduplicate is enabled.",
    required = true)
  var sourceOrderingField: String = CarbonCommonConstants
    .CARBON_STREAMER_SOURCE_ORDERING_FIELD_DEFAULT

  @Parameter(names = Array("--record-key-field"),
    description = "Join key/record key for a particular record. Will be used for deduplication of" +
                  " the incoming batch. If not present operation will fail.",
    required = true)
  var keyColumn: String = ""

  @Parameter(names = Array("--deduplicate"),
    description = "This property specifies if the incoming batch needs to be deduplicated in case" +
                  " of INSERT operation type. If set to true, the incoming batch will be " +
                  "deduplicated against the existing data in the target carbondata table.",
    required = false)
  var deduplicateEnabled: String = CarbonCommonConstants.CARBON_STREAMER_INSERT_DEDUPLICATE_DEFAULT

  @Parameter(names = Array("--combine-before-upsert"),
    description = "This property specifies if the incoming batch needs to be deduplicated (when " +
                  "multiple updates for the same record key are present in the incoming batch) in" +
                  " case of UPSERT/UPDATE operation type. If set to true, the user needs to " +
                  "provide proper value for the source ordering field as well.",
    required = false)
  var isCombineBeforeUpsert: String = CarbonCommonConstants
    .CARBON_STREAMER_UPSERT_DEDUPLICATE_DEFAULT

  @Parameter(names = Array("--min-batch-interval"),
    description = "Minimum batch interval time between 2 continuous ingestion in continuous mode." +
                  " Should be specified in seconds.",
    required = false)
  var batchInterval: String = CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL_DEFAULT

  @Parameter(names = Array("--meta-columns"),
    description = "Metadata columns added in source dataset. Please mention all the metadata" +
                  " columns as comma separated values which should not be written to the " +
                  "final carbondata table",
    required = false)
  var metaColumnsAdded: String = ""

  /**
   * This method set the configuration to carbonproperties which are passed as a arguments while
   * starting the streamer application
   */
  def setConfigsToCarbonProperty(streamerConfig: CarbonStreamerConfig): Unit = {
    val carbonPropertiesInstance = CarbonProperties.getInstance()

    if (streamerConfig.targetTableName.equalsIgnoreCase("")) {
      throw new CarbonDataStreamerException(
        "Target carbondata table is not configured. Please configure and retry.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_TABLE_NAME,
      streamerConfig.targetTableName)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_DATABASE_NAME,
      streamerConfig.databaseName)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE,
      streamerConfig.sourceType)
    if (sourceType.equalsIgnoreCase(SourceFactory.DFS.toString) &&
        dfsSourceInputPth.equalsIgnoreCase("")) {
      throw new CarbonDataStreamerException(
        "The DFS source path to read and ingest data onto target carbondata table is must in case" +
        " of DFS source type.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_DFS_INPUT_PATH,
      streamerConfig.dfsSourceInputPth)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER,
      streamerConfig.schemaProviderType)
    if (schemaProviderType.equalsIgnoreCase(CarbonCommonConstants
      .CARBON_STREAMER_SCHEMA_PROVIDER_DEFAULT) &&
        streamerConfig.schemaRegistryURL.equalsIgnoreCase("")) {
      throw new CarbonDataStreamerException(
        "Schema registry URL is must when the schema provider is set as SchemaRegistry. Please " +
        "configure and retry.")
    } else if (schemaProviderType.equalsIgnoreCase(CarbonCommonConstants
      .CARBON_STREAMER_FILE_SCHEMA_PROVIDER) &&
               streamerConfig.sourceSchemaFilePath.equalsIgnoreCase("")) {
      throw new CarbonDataStreamerException(
        "Schema file path is must when the schema provider is set as FileSchema. Please " +
        "configure and retry.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_SCHEMA_REGISTRY_URL,
      streamerConfig.schemaRegistryURL)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_SOURCE_SCHEMA_PATH,
      streamerConfig.sourceSchemaFilePath)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_INPUT_PAYLOAD_FORMAT,
      streamerConfig.inputPayloadFormat)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_MERGE_OPERATION_TYPE,
      streamerConfig.mergeOperationType)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants
      .CARBON_STREAMER_MERGE_OPERATION_FIELD, streamerConfig.deleteOperationField)
    if ((deleteOperationField.isEmpty && deleteFieldValue.nonEmpty) ||
        (deleteOperationField.nonEmpty && deleteFieldValue.isEmpty)) {
      throw new CarbonDataStreamerException(
        "Either both the values of --delete-operation-field and --delete-field-value should not " +
        "be configured or both must be configured. Please configure and retry.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants
      .CARBON_STREAMER_SOURCE_ORDERING_FIELD, streamerConfig.sourceOrderingField)
    if (streamerConfig.keyColumn.isEmpty) {
      throw new CarbonDataStreamerException(
        "The key column is must for the merge operation. Please configure and retry.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_KEY_FIELD,
      streamerConfig.keyColumn)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_INSERT_DEDUPLICATE,
      streamerConfig.deduplicateEnabled)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_UPSERT_DEDUPLICATE,
      streamerConfig.isCombineBeforeUpsert)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.KAFKA_INITIAL_OFFSET_TYPE,
      streamerConfig.kafkaInitialOffsetType)
    if (sourceType.equalsIgnoreCase(SourceFactory.KAFKA.toString) &&
        streamerConfig.inputKafkaTopic.isEmpty) {
      throw new CarbonDataStreamerException(
        "Kafka topics is must to consume and ingest data onto target carbondata table, in case" +
        " of KAFKA source type.")
    }
    if (sourceType.equalsIgnoreCase(SourceFactory.KAFKA.toString) &&
        streamerConfig.kafkaBrokerList.isEmpty) {
      throw new CarbonDataStreamerException(
        "Kafka broker list is must to consume and ingest data onto target carbondata table," +
        "in case of KAFKA source type.")
    }
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_KAFKA_INPUT_TOPIC,
      streamerConfig.inputKafkaTopic)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.KAFKA_BROKERS,
      streamerConfig.kafkaBrokerList)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.KAFKA_ENABLE_AUTO_COMMIT,
      CarbonCommonConstants.KAFKA_ENABLE_AUTO_COMMIT_DEFAULT)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.KAFKA_GROUP_ID,
      streamerConfig.groupId)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL,
      streamerConfig.batchInterval)
    carbonPropertiesInstance.addProperty(CarbonCommonConstants.CARBON_STREAMER_META_COLUMNS,
      streamerConfig.metaColumnsAdded)
  }
}
