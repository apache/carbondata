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

import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataLoadTableFileMapping, UpdateTableModel}
import org.apache.spark.sql.execution.datasources.{CarbonFileFormat, CatalogFileIndex, FindDataSourceTable, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, FileUtils}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.{DictionaryServer, NonSecureDictionaryServer}
import org.apache.carbondata.core.dictionary.service.NonSecureDictionaryServiceProvider
import org.apache.carbondata.core.metadata.PartitionMapFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.dictionary.provider.SecureDictionaryServiceProvider
import org.apache.carbondata.spark.dictionary.server.SecureDictionaryServer
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, CarbonDropPartitionCommitRDD, CarbonDropPartitionRDD}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataLoadingUtil, GlobalDictionaryUtil}

case class CarbonLoadDataCommand(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteTable: Boolean,
    var inputSqlString: String = null,
    var dataFrame: Option[DataFrame] = None,
    updateModel: Option[UpdateTableModel] = None,
    var tableInfoOp: Option[TableInfo] = None,
    var internalOptions: Map[String, String] = Map.empty,
    partition: Map[String, Option[String]] = Map.empty,
    logicalPlan: Option[LogicalPlan] = None,
    var operationContext: OperationContext = new OperationContext) extends AtomicRunnableCommand {

  var table: CarbonTable = _

  var logicalPartitionRelation: LogicalRelation = _

  var sizeInBytes: Long = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    table = if (tableInfoOp.isDefined) {
        CarbonTable.buildFromTableInfo(tableInfoOp.get)
      } else {
        val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        if (relation == null) {
          throw new NoSuchTableException(dbName, tableName)
        }
        if (null == relation.carbonTable) {
          LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
          LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
          throw new NoSuchTableException(dbName, tableName)
        }
        relation.carbonTable
      }
    if (table.isHivePartitionTable) {
      logicalPartitionRelation =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
            TableIdentifier(tableName, databaseNameOp))).collect {
          case l: LogicalRelation => l
        }.head
      sizeInBytes = logicalPartitionRelation.relation.sizeInBytes
    }
    operationContext.setProperty("isOverwrite", isOverwriteTable)
    if(CarbonUtil.hasAggregationDataMap(table)) {
      val loadMetadataEvent = new LoadMetadataEvent(table, false)
      OperationListenerBus.getInstance().fireEvent(loadMetadataEvent, operationContext)
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
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

    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val carbonLoadModel = new CarbonLoadModel()
    try {
      val tableProperties = table.getTableInfo.getFactTable.getTableProperties
      val optionsFinal = DataLoadingUtil.getDataLoadingOptions(carbonProperty, options)
      optionsFinal.put("sort_scope", tableProperties.asScala.getOrElse("sort_scope",
        carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
          carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
            CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))

      val factPath = if (dataFrame.isDefined) {
        ""
      } else {
        FileUtils.getPaths(factPathFromUser, hadoopConf)
      }
      carbonLoadModel.setFactFilePath(factPath)
      carbonLoadModel.setAggLoadRequest(
        internalOptions.getOrElse(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL, "false").toBoolean)
      carbonLoadModel.setSegmentId(internalOptions.getOrElse("mergedSegmentName", ""))
      DataLoadingUtil.buildCarbonLoadModel(
        table,
        carbonProperty,
        options,
        optionsFinal,
        carbonLoadModel,
        hadoopConf
      )
      // Delete stale segment folders that are not in table status but are physically present in
      // the Fact folder
      LOGGER.info(s"Deleting stale folders if present for table $dbName.$tableName")
      TableProcessingOperations.deletePartialLoadDataIfExist(table, false)
      var isUpdateTableStatusRequired = false
      // if the table is child then extract the uuid from the operation context and the parent would
      // already generated UUID.
      // if parent table then generate a new UUID else use empty.
      val uuid = if (table.isChildDataMap) {
        Option(operationContext.getProperty("uuid")).getOrElse("").toString
      } else if (table.hasAggregationDataMap) {
        UUID.randomUUID().toString
      } else {
        ""
      }
      try {
        operationContext.setProperty("uuid", uuid)
        val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
          new LoadTablePreExecutionEvent(
            table.getCarbonTableIdentifier,
            carbonLoadModel,
            factPath,
            dataFrame.isDefined,
            optionsFinal.asJava,
            options.asJava,
            isOverwriteTable)
        operationContext.setProperty("isOverwrite", isOverwriteTable)
        OperationListenerBus.getInstance.fireEvent(loadTablePreExecutionEvent, operationContext)
        // First system has to partition the data first and then call the load data
        LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
        // Clean up the old invalid segment data before creating a new entry for new load.
        DataLoadingUtil.deleteLoadsAndUpdateMetadata(isForceDeletion = false, table)
        // add the start entry for the new load in the table status file
        if (updateModel.isEmpty && !table.isHivePartitionTable) {
          CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(
            carbonLoadModel,
            isOverwriteTable)
          isUpdateTableStatusRequired = true
        }
        if (isOverwriteTable) {
          LOGGER.info(s"Overwrite of carbon table with $dbName.$tableName is in progress")
        }
        // if table is an aggregate table then disable single pass.
        if (carbonLoadModel.isAggLoadRequest) {
          carbonLoadModel.setUseOnePass(false)
        }

        // start dictionary server when use one pass load and dimension with DICTIONARY
        // encoding is present.
        val allDimensions =
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAllDimensions.asScala.toList
        val createDictionary = allDimensions.exists {
          carbonDimension => carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
                             !carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
        }
        if (!createDictionary) {
          carbonLoadModel.setUseOnePass(false)
        }
        // Create table and metadata folders if not exist
        val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
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
            partitionStatus,
            hadoopConf,
            operationContext)
        } else {
          loadData(
            sparkSession,
            carbonLoadModel,
            columnar,
            partitionStatus,
            hadoopConf,
            operationContext)
        }
        val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
          new LoadTablePostExecutionEvent(
            table.getCarbonTableIdentifier,
            carbonLoadModel)
        OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
      } catch {
        case CausedBy(ex: NoRetryException) =>
          // update the load entry in table status file for changing the status to marked for delete
          if (isUpdateTableStatusRequired) {
            CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
          }
          LOGGER.error(ex, s"Dataload failure for $dbName.$tableName")
          throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ex.getMessage}")
        // In case of event related exception
        case preEventEx: PreEventException =>
          throw new AnalysisException(preEventEx.getMessage)
        case ex: Exception =>
          LOGGER.error(ex)
          // update the load entry in table status file for changing the status to marked for delete
          if (isUpdateTableStatusRequired) {
            CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
          }
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
            val file = FileFactory.getCarbonFile(partitionLocation, fileType)
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
      partitionStatus: SegmentStatus,
      hadoopConf: Configuration,
      operationContext: OperationContext): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      .getCarbonTableIdentifier
    val dictFolderPath = CarbonTablePath.getMetadataPath(carbonLoadModel.getTablePath)
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
        dictFolderPath)
    }
    if (!StringUtils.isEmpty(carbonLoadModel.getAllDictPath)) {
      carbonLoadModel.initPredefDictMap()
      GlobalDictionaryUtil
        .generateDictionaryFromDictionaryFiles(sparkSession.sqlContext,
          carbonLoadModel,
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

    val carbonSecureModeDictServer = CarbonProperties.getInstance.
      getProperty(CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER,
      CarbonCommonConstants.CARBON_SECURE_DICTIONARY_SERVER_DEFAULT)

    val sparkConf = sparkSession.sqlContext.sparkContext.getConf
    // For testing.
    // sparkConf.set("spark.authenticate", "true")
    // sparkConf.set("spark.authenticate.secret", "secret")

    val server: Option[DictionaryServer] = if (sparkConf.get("spark.authenticate", "false").
      equalsIgnoreCase("true") && carbonSecureModeDictServer.toBoolean) {
      val dictionaryServer = SecureDictionaryServer.getInstance(sparkConf,
        sparkDriverHost.toString, dictionaryServerPort.toInt, carbonTable)
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
        .getInstance(dictionaryServerPort.toInt, carbonTable)
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
    val loadDataFrame = if (updateModel.isDefined) {
       Some(getDataFrameWithTupleID())
    } else {
      dataFrame
    }

    if (carbonTable.isHivePartitionTable) {
      try {
        loadDataWithPartition(
          sparkSession,
          carbonLoadModel,
          hadoopConf,
          loadDataFrame,
          operationContext)
      } finally {
        server match {
          case Some(dictServer) =>
            try {
              dictServer.writeTableDictionary(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
                .getCarbonTableIdentifier.getTableId)
            } catch {
              case _: Exception =>
                throw new Exception("Dataload failed due to error while writing dictionary file!")
            }
          case _ =>
        }
      }
    } else {
      CarbonDataRDDFactory.loadCarbonData(
        sparkSession.sqlContext,
        carbonLoadModel,
        columnar,
        partitionStatus,
        server,
        isOverwriteTable,
        hadoopConf,
        loadDataFrame,
        updateModel,
        operationContext)
    }
  }

  private def loadData(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      columnar: Boolean,
      partitionStatus: SegmentStatus,
      hadoopConf: Configuration,
      operationContext: OperationContext): Unit = {
    val (dictionaryDataFrame, loadDataFrame) = if (updateModel.isDefined) {
      val dataFrameWithTupleId: DataFrame = getDataFrameWithTupleID()
      // getting all fields except tupleId field as it is not required in the value
      val otherFields = CarbonScalaUtil.getAllFieldsWithoutTupleIdField(dataFrame.get.schema.fields)
      // use dataFrameWithoutTupleId as dictionaryDataFrame
      val dataFrameWithoutTupleId = dataFrame.get.select(otherFields: _*)
      (Some(dataFrameWithoutTupleId), Some(dataFrameWithTupleId))
    } else {
      (dataFrame, dataFrame)
    }
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    if (!table.isChildDataMap) {
      GlobalDictionaryUtil.generateGlobalDictionary(
        sparkSession.sqlContext,
        carbonLoadModel,
        hadoopConf,
        dictionaryDataFrame)
    }
    if (table.isHivePartitionTable) {
      loadDataWithPartition(
        sparkSession,
        carbonLoadModel,
        hadoopConf,
        loadDataFrame,
        operationContext)
    } else {
      CarbonDataRDDFactory.loadCarbonData(
        sparkSession.sqlContext,
        carbonLoadModel,
        columnar,
        partitionStatus,
        None,
        isOverwriteTable,
        hadoopConf,
        loadDataFrame,
        updateModel,
        operationContext)
    }
  }

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitoned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  private def loadDataWithPartition(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration,
      dataFrame: Option[DataFrame],
      operationContext: OperationContext) = {
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val identifier = TableIdentifier(table.getTableName, Some(table.getDatabaseName))
    val catalogTable: CatalogTable = logicalPartitionRelation.catalogTable.get
    val currentPartitions =
      CarbonFilters.getPartitions(Seq.empty[Expression], sparkSession, identifier)
    // Clean up the alreday dropped partitioned data
    new PartitionMapFileStore().cleanSegments(table, currentPartitions.asJava, false)
    // Converts the data to carbon understandable format. The timestamp/date format data needs to
    // converted to hive standard fomat to let spark understand the data to partition.
    val serializationNullFormat =
      carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
    val badRecordAction =
      carbonLoadModel.getBadRecordsAction.split(",")(1)
    var timeStampformatString = carbonLoadModel.getTimestampformat
    if (timeStampformatString.isEmpty) {
      timeStampformatString = carbonLoadModel.getDefaultTimestampFormat
    }
    val timeStampFormat = new SimpleDateFormat(timeStampformatString)
    var dateFormatString = carbonLoadModel.getDateFormat
    if (dateFormatString.isEmpty) {
      dateFormatString = carbonLoadModel.getDefaultDateFormat
    }
    val dateFormat = new SimpleDateFormat(dateFormatString)
    CarbonSession.threadSet(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT, dateFormatString)
    CarbonSession.threadSet(
      CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT,
      timeStampformatString)
    CarbonSession.threadSet(
      CarbonLoadOptionConstants.CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT,
      serializationNullFormat)
    CarbonSession.threadSet(
      CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION,
      badRecordAction)
    val isEmptyBadRecord = carbonLoadModel.getIsEmptyDataBadRecord.split(",")(1)
    CarbonSession.threadSet(
      CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
      isEmptyBadRecord)
    CarbonSession.threadSet("partition.operationcontext", operationContext)
    try {
      val query: LogicalPlan = if (dataFrame.isDefined) {
        val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
        val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2
        val attributes =
          StructType(dataFrame.get.schema.fields.map(_.copy(dataType = StringType))).toAttributes
        val len = attributes.length
        val rdd = dataFrame.get.rdd.map { f =>
          val data = new Array[Any](len)
          var i = 0
          while (i < len) {
            data(i) =
              UTF8String.fromString(
                CarbonScalaUtil.getString(f.get(i),
                  serializationNullFormat,
                  delimiterLevel1,
                  delimiterLevel2,
                  timeStampFormat,
                  dateFormat))
            i = i + 1
          }
          InternalRow.fromSeq(data)
        }
        if (updateModel.isDefined) {
          // Get the updated query plan in case of update scenario
          getLogicalQueryForUpdate(sparkSession, catalogTable, attributes, rdd)
        } else {
          LogicalRDD(attributes, rdd)(sparkSession)
        }

      } else {
        // input data from csv files. Convert to logical plan
        val attributes =
          StructType(carbonLoadModel.getCsvHeaderColumns.map(
            StructField(_, StringType))).toAttributes
        val rowDataTypes = attributes.map { attribute =>
          catalogTable.schema.find(_.name.equalsIgnoreCase(attribute.name)) match {
            case Some(attr) => attr.dataType
            case _ => StringType
          }
        }
        // Find the partition columns from the csv header attributes
        val partitionColumns = attributes.map { attribute =>
          catalogTable.partitionSchema.find(_.name.equalsIgnoreCase(attribute.name)) match {
            case Some(attr) => true
            case _ => false
          }
        }
        val len = rowDataTypes.length
        var rdd =
          DataLoadingUtil.csvFileScanRDD(
            sparkSession,
            model = carbonLoadModel,
            hadoopConf)
            .map { row =>
              val data = new Array[Any](len)
              var i = 0
              val input = row.asInstanceOf[GenericInternalRow].values.asInstanceOf[Array[String]]
              val inputLen = Math.min(input.length, len)
              while (i < inputLen) {
                data(i) = UTF8String.fromString(input(i))
                // If partition column then update empty value with special string otherwise spark
                // makes it as null so we cannot internally handle badrecords.
                if (partitionColumns(i)) {
                  if (input(i) != null && input(i).isEmpty) {
                    data(i) = UTF8String.fromString(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
                  }
                }
                i = i + 1
              }
              InternalRow.fromSeq(data)

          }
        // Only select the required columns
        val output = if (partition.nonEmpty) {
          val lowerCasePartition = partition.map{case(key, value) => (key.toLowerCase, value)}
          catalogTable.schema.map { attr =>
            attributes.find(_.name.equalsIgnoreCase(attr.name)).get
          }.filter(attr => lowerCasePartition.getOrElse(attr.name.toLowerCase, None).isEmpty)
        } else {
          catalogTable.schema.map(f => attributes.find(_.name.equalsIgnoreCase(f.name)).get)
        }
        Project(output, LogicalRDD(attributes, rdd)(sparkSession))
      }
      val convertRelation = convertToLogicalRelation(
        catalogTable,
        sizeInBytes,
        isOverwriteTable,
        carbonLoadModel,
        sparkSession,
        operationContext)
      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = partition,
          query = query,
          overwrite = false,
          ifPartitionNotExists = false)
      if (isOverwriteTable && partition.nonEmpty) {
        overwritePartition(
          sparkSession,
          table,
          convertedPlan,
          serializationNullFormat,
          badRecordAction,
          isEmptyBadRecord.toBoolean)
      } else {
        Dataset.ofRows(sparkSession, convertedPlan)
      }
    } finally {
      CarbonSession.threadUnset(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT)
      CarbonSession.threadUnset(CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT)
      CarbonSession.threadUnset(CarbonLoadOptionConstants.CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT)
      CarbonSession.threadUnset(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION)
      CarbonSession.threadUnset(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD)
      CarbonSession.threadUnset("partition.operationcontext")
    }
    try {
      // Trigger auto compaction
      CarbonDataRDDFactory.handleSegmentMerging(
        sparkSession.sqlContext,
        carbonLoadModel,
        table,
        operationContext)
    } catch {
      case e: Exception =>
        throw new Exception(
          "Dataload is success. Auto-Compaction has failed. Please check logs.",
          e)
    }
  }

  /**
   * Create the logical plan for update scenario. Here we should drop the segmentid column from the
   * input rdd.
   */
  private def getLogicalQueryForUpdate(
      sparkSession: SparkSession,
      catalogTable: CatalogTable,
      attributes: Seq[AttributeReference],
      rdd: RDD[InternalRow]): LogicalPlan = {
    sparkSession.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
    // In case of update, we don't need the segmrntid column in case of partitioning
    val dropAttributes = attributes.dropRight(1)
    val finalOutput = catalogTable.schema.map { attr =>
      dropAttributes.find { d =>
        val index = d.name.lastIndexOf("-updatedColumn")
        if (index > 0) {
          d.name.substring(0, index).equalsIgnoreCase(attr.name)
        } else {
          d.name.equalsIgnoreCase(attr.name)
        }
      }.get
    }
    Project(finalOutput, LogicalRDD(attributes, rdd)(sparkSession))
  }

  private def convertToLogicalRelation(
      catalogTable: CatalogTable,
      sizeInBytes: Long,
      overWrite: Boolean,
      loadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext): LogicalRelation = {
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val metastoreSchema = StructType(catalogTable.schema.fields.map(_.copy(dataType = StringType)))
    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val catalog = new CatalogFileIndex(
      sparkSession, catalogTable, sizeInBytes)
    if (lazyPruningEnabled) {
      catalog
    } else {
      catalog.filterPartitions(Nil) // materialize all the partitions in memory
    }
    val partitionSchema =
      StructType(table.getPartitionInfo(table.getTableName).getColumnSchemaList.asScala.map(field =>
        metastoreSchema.fields.find(_.name.equalsIgnoreCase(field.getColumnName))).map(_.get))
    val overWriteLocal = if (overWrite && partition.nonEmpty) {
      false
    } else {
      overWrite
    }
    val dataSchema =
      StructType(metastoreSchema
        .filterNot(field => partitionSchema.contains(field.name)))
    val options = new mutable.HashMap[String, String]()
    options ++= catalogTable.storage.properties
    options += (("overwrite", overWriteLocal.toString))
    options += (("onepass", loadModel.getUseOnePass.toString))
    options += (("dicthost", loadModel.getDictionaryServerHost))
    options += (("dictport", loadModel.getDictionaryServerPort.toString))
    if (partition.nonEmpty) {
      val staticPartitionStr = ObjectSerializationUtil.convertObjectToString(
        new util.HashMap[String, Boolean](
          partition.map{case (col, value) => (col.toLowerCase, value.isDefined)}.asJava))
      options += (("staticpartition", staticPartitionStr))
    }
    options ++= this.options
    if (updateModel.isDefined) {
      options += (("updatetimestamp", updateModel.get.updatedTimeStamp.toString))
      if (updateModel.get.deletedSegments.nonEmpty) {
        options += (("segmentsToBeDeleted", updateModel.get.deletedSegments.mkString(",")))
      }
    }
    val hdfsRelation = HadoopFsRelation(
      location = catalog,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = catalogTable.bucketSpec,
      fileFormat = new CarbonFileFormat,
      options = options.toMap)(sparkSession = sparkSession)

    CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
      hdfsRelation.schema.toAttributes,
      Some(catalogTable))
  }

  /**
   * Overwrite the partition data if static partitions are specified.
   * @param sparkSession
   * @param table
   * @param logicalPlan
   */
  private def overwritePartition(
      sparkSession: SparkSession,
      table: CarbonTable,
      logicalPlan: LogicalPlan,
      serializationNullFormat: String,
      badRecordAction: String,
      isEmptyBadRecord: Boolean): Unit = {
    val identifier = TableIdentifier(table.getTableName, Some(table.getDatabaseName))

    // Update the partitions as per the datatype expect for time and datetype as we
    // expect user provides the format in standard spark/hive formats.
    val updatedPartitions = CarbonScalaUtil.updatePartitions(
      partition.filter(_._2.isDefined).map(f => (f._1, f._2.get)),
      table,
      timeFormat = null,
      dateFormat = null,
      serializationNullFormat,
      badRecordAction,
      isEmptyBadRecord)
    val existingPartitions = sparkSession.sessionState.catalog.listPartitions(
      identifier,
      Some(updatedPartitions))
    val partitionNames = existingPartitions.toList.flatMap { partition =>
      partition.spec.seq.map{case (column, value) => column + "=" + value}
    }.toSet
    val uniqueId = System.currentTimeMillis().toString
    val segments = new SegmentStatusManager(
      table.getAbsoluteTableIdentifier).getValidAndInvalidSegments.getValidSegments
    // If any existing partitions need to be overwritten then drop from partitionmap
    if (partitionNames.nonEmpty) {
      try {
        // First drop the partitions from partition mapper files of each segment
        new CarbonDropPartitionRDD(
          sparkSession.sparkContext,
          table.getTablePath,
          segments.asScala,
          partitionNames.toSeq,
          uniqueId,
          partialMatch = false).collect()
      } catch {
        case e: Exception =>
          // roll back the drop partitions from carbon store
          new CarbonDropPartitionCommitRDD(
            sparkSession.sparkContext,
            table.getTablePath,
            segments.asScala,
            success = false,
            uniqueId,
            partitionNames.toSeq).collect()
          throw e
      }

      try {
        Dataset.ofRows(sparkSession, logicalPlan)
      } catch {
        case e: Exception =>
          // roll back the drop partitions from carbon store
          new CarbonDropPartitionCommitRDD(
            sparkSession.sparkContext,
            table.getTablePath,
            segments.asScala,
            success = false,
            uniqueId,
            partitionNames.toSeq).collect()
          throw e
      }
      // Commit the removed partitions in carbon store.
      new CarbonDropPartitionCommitRDD(
        sparkSession.sparkContext,
        table.getTablePath,
        segments.asScala,
        success = true,
        uniqueId,
        partitionNames.toSeq).collect()
      // get valid segments
      val validsegments =
        new SegmentStatusManager(
          table.getAbsoluteTableIdentifier).getValidAndInvalidSegments.getValidSegments
      // Update the loadstatus with update time to clear cache from driver.
      CarbonUpdateUtil.updateTableMetadataStatus(
        new util.HashSet[String](validsegments),
        table,
        uniqueId,
        true,
        new util.ArrayList[String])
      DataMapStoreManager.getInstance().clearDataMaps(table.getAbsoluteTableIdentifier)
      // Clean the overwriting segments if any.
      new PartitionMapFileStore().cleanSegments(
        table,
        CarbonFilters.getPartitions(Seq.empty, sparkSession, identifier).asJava,
        false)
    } else {
      // Otherwise its a normal load
      Dataset.ofRows(sparkSession, logicalPlan)
    }
  }

  def getDataFrameWithTupleID(): DataFrame = {
    val fields = dataFrame.get.schema.fields
    import org.apache.spark.sql.functions.udf
    // extracting only segment from tupleId
    val getSegIdUDF = udf((tupleId: String) =>
      CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
    // getting all fields except tupleId field as it is not required in the value
    val otherFields = CarbonScalaUtil.getAllFieldsWithoutTupleIdField(fields)
    // extract tupleId field which will be used as a key
    val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
      .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).
      as(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_SEGMENTID)
    val fieldWithTupleId = otherFields :+ segIdColumn
    // use dataFrameWithTupleId as loadDataFrame
    val dataFrameWithTupleId = dataFrame.get.select(fieldWithTupleId: _*)
    (dataFrameWithTupleId)
  }

}
