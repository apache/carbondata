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

package org.apache.spark.sql.execution.command.management

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.command.{DataLoadTableFileMapping, DataProcessCommand, RunnableCommand, UpdateTableModel}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CausedBy, FileUtils}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, DictionaryLoadModel}
import org.apache.carbondata.spark.util.{CommonUtil, DataLoadingUtil, GlobalDictionaryUtil}

case class LoadTableCommand(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteTable: Boolean,
    var inputSqlString: String = null,
    dataFrame: Option[DataFrame] = None,
    updateModel: Option[UpdateTableModel] = None,
    var tableInfoOp: Option[TableInfo] = None)
  extends RunnableCommand with DataProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    if (dataFrame.isDefined && updateModel.isEmpty) {
      val rdd = dataFrame.get.rdd
      if (rdd.partitions == null || rdd.partitions.length == 0) {
        LOGGER.warn("DataLoading finished. No data was loaded.")
        return Seq.empty
      }
    }

    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)

    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    carbonProperty.addProperty("zookeeper.enable.lock", "false")

    // get the value of 'spark.executor.cores' from spark conf, default value is 1
    val sparkExecutorCores = sparkSession.sparkContext.conf.get("spark.executor.cores", "1")
    // get the value of 'carbon.number.of.cores.while.loading' from carbon properties,
    // default value is the value of 'spark.executor.cores'
    val numCoresLoading =
      try {
        CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.NUM_CORES_LOADING, sparkExecutorCores)
      } catch {
        case exc: NumberFormatException =>
          LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_LOADING
              + " is wrong. Falling back to the default value "
              + sparkExecutorCores)
          sparkExecutorCores
      }

    // update the property with new value
    carbonProperty.addProperty(CarbonCommonConstants.NUM_CORES_LOADING, numCoresLoading)

    try {
      val table = if (tableInfoOp.isDefined) {
        CarbonTable.buildFromTableInfo(tableInfoOp.get)
      } else {
        val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        if (relation == null) {
          sys.error(s"Table $dbName.$tableName does not exist")
        }
        if (null == relation.carbonTable) {
          LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
          LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
          sys.error(s"Data loading failed. table not found: $dbName.$tableName")
        }
        relation.carbonTable
      }

      val tableProperties = table.getTableInfo.getFactTable.getTableProperties
      val optionsFinal = DataLoadingUtil.getDataLoadingOptions(carbonProperty, options)
      optionsFinal.put("sort_scope", tableProperties.getOrDefault("sort_scope",
        carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
          carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
            CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))

      val carbonLoadModel = new CarbonLoadModel()
      val factPath = if (dataFrame.isDefined) {
        ""
      } else {
        FileUtils.getPaths(
          CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      }
      carbonLoadModel.setFactFilePath(factPath)
      DataLoadingUtil.buildCarbonLoadModel(
        table,
        carbonProperty,
        options,
        optionsFinal,
        carbonLoadModel
      )

      try{
        // First system has to partition the data first and then call the load data
        LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
        GlobalDictionaryUtil.updateTableMetadataFunc = updateTableMetadata
        // add the start entry for the new load in the table status file
        if (updateModel.isEmpty) {
          CommonUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, isOverwriteTable)
        }
        if (isOverwriteTable) {
          LOGGER.info(s"Overwrite of carbon table with $dbName.$tableName is in progress")
        }
        if (carbonLoadModel.getLoadMetadataDetails.isEmpty && carbonLoadModel.getUseOnePass &&
            StringUtils.isEmpty(carbonLoadModel.getColDictFilePath) &&
            StringUtils.isEmpty(carbonLoadModel.getAllDictPath)) {
          LOGGER.info(s"Cannot use single_pass=true for $dbName.$tableName during the first load")
          LOGGER.audit(s"Cannot use single_pass=true for $dbName.$tableName during the first load")
          carbonLoadModel.setUseOnePass(false)
        }
        // if table is an aggregate table then disable single pass.
        if (carbonLoadModel.isAggLoadRequest) {
          carbonLoadModel.setUseOnePass(false)
        }
        // Create table and metadata folders if not exist
        val carbonTablePath = CarbonStorePath.getCarbonTablePath(table.getAbsoluteTableIdentifier)
        val metadataDirectoryPath = carbonTablePath.getMetadataDirectoryPath
        val fileType = FileFactory.getFileType(metadataDirectoryPath)
        if (!FileFactory.isFileExist(metadataDirectoryPath, fileType)) {
          FileFactory.mkdirs(metadataDirectoryPath, fileType)
        }
        val partitionStatus = SegmentStatus.SUCCESS
        val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
        if (carbonLoadModel.getUseOnePass) {
          loadDataUsingOnePass(
            sparkSession,
            carbonProperty,
            carbonLoadModel,
            columnar,
            partitionStatus)
        } else {
          loadData(
            sparkSession,
            carbonLoadModel,
            columnar,
            partitionStatus)
        }
      } catch {
        case CausedBy(ex: NoRetryException) =>
          LOGGER.error(ex, s"Dataload failure for $dbName.$tableName")
          throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ex.getMessage}")
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. Please check the logs")
          throw ex
      } finally {
        // Once the data load is successful delete the unwanted partition files
        try {
          val partitionLocation = CarbonProperties.getStorePath + "/partition/" +
                                  table.getDatabaseName + "/" +
                                  table.getTableName + "/"
          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
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
    } catch {
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
    }
    Seq.empty
  }

  private def loadDataUsingOnePass(
      sparkSession: SparkSession,
      carbonProperty: CarbonProperties,
      carbonLoadModel: CarbonLoadModel,
      columnar: Boolean,
      partitionStatus: SegmentStatus): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      .getCarbonTableIdentifier
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(carbonLoadModel.getTablePath, carbonTableIdentifier)
    val dictFolderPath = carbonTablePath.getMetadataDirectoryPath
    val dimensions = carbonTable.getDimensionByTableName(
      carbonTable.getTableName).asScala.toArray
    val colDictFilePath = carbonLoadModel.getColDictFilePath
    if (!StringUtils.isEmpty(colDictFilePath)) {
      carbonLoadModel.initPredefDictMap()
      // generate predefined dictionary
      GlobalDictionaryUtil.generatePredefinedColDictionary(
        colDictFilePath,
        carbonTableIdentifier,
        dimensions,
        carbonLoadModel,
        sparkSession.sqlContext,
        carbonLoadModel.getTablePath,
        dictFolderPath)
    }
    if (!StringUtils.isEmpty(carbonLoadModel.getAllDictPath)) {
      carbonLoadModel.initPredefDictMap()
      GlobalDictionaryUtil
        .generateDictionaryFromDictionaryFiles(sparkSession.sqlContext,
          carbonLoadModel,
          carbonLoadModel.getTablePath,
          carbonTableIdentifier,
          dictFolderPath,
          dimensions,
          carbonLoadModel.getAllDictPath)
    }
    // dictionaryServerClient dictionary generator
    val dictionaryServerPort = carbonProperty
      .getProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
        CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT)
    val sparkDriverHost = sparkSession.sqlContext.sparkContext.
      getConf.get("spark.driver.host")
    carbonLoadModel.setDictionaryServerHost(sparkDriverHost)
    // start dictionary server when use one pass load and dimension with DICTIONARY
    // encoding is present.
    val allDimensions =
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAllDimensions.asScala.toList
    val createDictionary = allDimensions.exists {
      carbonDimension => carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
                         !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
    }
    val server: Option[DictionaryServer] = if (createDictionary) {
      val dictionaryServer = DictionaryServer
        .getInstance(dictionaryServerPort.toInt, carbonTable)
      carbonLoadModel.setDictionaryServerPort(dictionaryServer.getPort)
      sparkSession.sparkContext.addSparkListener(new SparkListener() {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
          dictionaryServer.shutdown()
        }
      })
      Some(dictionaryServer)
    } else {
      None
    }
    CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
      carbonLoadModel,
      carbonLoadModel.getTablePath,
      columnar,
      partitionStatus,
      server,
      isOverwriteTable,
      dataFrame,
      updateModel)
  }

  private def loadData(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      columnar: Boolean,
      partitionStatus: SegmentStatus): Unit = {
    val (dictionaryDataFrame, loadDataFrame) = if (updateModel.isDefined) {
      val fields = dataFrame.get.schema.fields
      import org.apache.spark.sql.functions.udf
      // extracting only segment from tupleId
      val getSegIdUDF = udf((tupleId: String) =>
        CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
      // getting all fields except tupleId field as it is not required in the value
      var otherFields = fields.toSeq
        .filter(field => !field.name
          .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
        .map(field => new Column(field.name))

      // extract tupleId field which will be used as a key
      val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
        .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).
        as(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_SEGMENTID)
      // use dataFrameWithoutTupleId as dictionaryDataFrame
      val dataFrameWithoutTupleId = dataFrame.get.select(otherFields: _*)
      otherFields = otherFields :+ segIdColumn
      // use dataFrameWithTupleId as loadDataFrame
      val dataFrameWithTupleId = dataFrame.get.select(otherFields: _*)
      (Some(dataFrameWithoutTupleId), Some(dataFrameWithTupleId))
    } else {
      (dataFrame, dataFrame)
    }

    GlobalDictionaryUtil.generateGlobalDictionary(
      sparkSession.sqlContext,
      carbonLoadModel,
      carbonLoadModel.getTablePath,
      dictionaryDataFrame)
    CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
      carbonLoadModel,
      carbonLoadModel.getTablePath,
      columnar,
      partitionStatus,
      None,
      isOverwriteTable,
      loadDataFrame,
      updateModel)
  }

  private def updateTableMetadata(
      carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      model: DictionaryLoadModel,
      noDictDimension: Array[CarbonDimension]): Unit = {
    val sparkSession = sqlContext.sparkSession
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(model.table)

    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    // read TableInfo
    val tableInfo: format.TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)

    // modify TableInfo
    val columns = tableInfo.getFact_table.getTable_columns
    for (i <- 0 until columns.size) {
      if (noDictDimension.exists(x => columns.get(i).getColumn_id.equals(x.getColumnId))) {
        columns.get(i).encoders.remove(org.apache.carbondata.format.Encoding.DICTIONARY)
      }
    }
    val entry = tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
    entry.setTime_stamp(System.currentTimeMillis())

    // write TableInfo
    metastore.updateTableSchemaForAlter(carbonTablePath.getCarbonTableIdentifier,
      carbonTablePath.getCarbonTableIdentifier,
      tableInfo, entry, carbonTablePath.getPath)(sparkSession)

    // update the schema modified time
    metastore.updateAndTouchSchemasUpdatedTime()

    val identifier = model.table.getCarbonTableIdentifier
    // update CarbonDataLoadSchema
    val carbonTable = metastore.lookupRelation(Option(identifier.getDatabaseName),
      identifier.getTableName)(sqlContext.sparkSession).asInstanceOf[CarbonRelation].carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
  }
}
