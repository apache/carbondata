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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CausedBy, DataLoadUtil, FileUtils}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.exception.NoRetryException
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.{CommonUtil, GlobalDictionaryUtil}

case class LoadTable(
    tableIdentifier: TableIdentifier,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteTable: Boolean,
    var inputSqlString: String = null,
    dataFrame: Option[DataFrame] = None,
    updateModel: Option[UpdateTableModel] = None)
  extends RunnableCommand with DataProcessCommand {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val carbonLoadModel: CarbonLoadModel = new CarbonLoadModel
  var carbonRelation: CarbonRelation = _
  val carbonProperty: CarbonProperties = CarbonProperties.getInstance()

  def run(sparkSession: SparkSession): Seq[Row] = {
    if (dataFrame.isDefined && updateModel.isEmpty) {
      val rdd = dataFrame.get.rdd
      if (rdd.partitions == null || rdd.partitions.length == 0) {
        LOGGER.warn("DataLoading finished. No data was loaded.")
        return Seq.empty
      }
    }
    carbonRelation = DataLoadUtil
      .validateCarbonRelation(tableIdentifier.database, tableIdentifier.table)(sparkSession)
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val optionsFinal = DataLoadUtil.getFinalOptions(carbonProperty, options)
    DataLoadUtil.prepareCarbonLoadModel(carbonLoadModel, carbonRelation, optionsFinal)
    if (updateModel.isEmpty) {
      CommonUtil.
        readAndUpdateLoadProgressInTableMeta(carbonLoadModel,
          carbonRelation.tableMeta.storePath, isOverwriteTable)
    }
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }
    val factPath = if (dataFrame.isDefined) {
      ""
    } else {
      FileUtils.getPaths(
        CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
    }
    carbonLoadModel.setFactFilePath(factPath)
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val dbName = tableIdentifier.database
    val tableName = tableIdentifier.table
    val partitionLocation = DataLoadUtil.getPartitionLocation(carbonRelation.tableMeta)
    try {
      // First system has to partition the data first and then call the load data
      LOGGER
        .info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
      GlobalDictionaryUtil.updateTableMetadataFunc = DataLoadUtil.updateTableMetadata
      val storePath = carbonRelation.tableMeta.storePath
      // add the start entry for the new load in the table status file
      if (updateModel.isEmpty) {
        CommonUtil.
          readAndUpdateLoadProgressInTableMeta(carbonLoadModel, storePath, isOverwriteTable)
      }
      if (isOverwriteTable) {
        LOGGER.info(s"Overwrite of carbon table with ${
          tableIdentifier.database
        }.${ tableIdentifier.table } is in progress")
      }
      // Create table and metadata folders if not exist
      val metadataDirectoryPath = carbonLoadModel.getMetaDataDirectoryPath
      DataLoadUtil.createMetaDataDirectoryIfNotExist(metadataDirectoryPath)
      // start dictionary server when use one pass load and dimension with DICTIONARY
      // encoding is present.
      startDataLoadProcess(sparkSession)
    } catch {
      case CausedBy(ex: NoRetryException) =>
        LOGGER
          .error(ex,
            s"Dataload failure for $dbName.$tableName")
        throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ ex.getMessage }")
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
      case ex: Exception =>
        LOGGER.error(ex)
        LOGGER.audit(s"Dataload failure for $dbName.$tableName. ${ ex.getMessage }")
        throw ex
    } finally {
      // Once the data load is successful delete the unwanted partition files
      try {
        val fileType = FileFactory.getFileType(partitionLocation)
        if (FileFactory
          .isFileExist(partitionLocation, FileFactory.getFileType(partitionLocation))) {
          val file = FileFactory
            .getCarbonFile(partitionLocation, fileType)
          CarbonUtil.deleteFoldersAndFiles(file)
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. " +
                       "Problem deleting the partition folder")
          throw ex
      }
    }
    Seq.empty[Row]
  }

  private def startDataLoadProcess(sparkSession: SparkSession) = {
    DataLoadUtil
      .startDataLoadProcess(carbonLoadModel,
        carbonRelation,
        isOverwriteTable,
        dataFrame,
        updateModel)(sparkSession)
  }
}
