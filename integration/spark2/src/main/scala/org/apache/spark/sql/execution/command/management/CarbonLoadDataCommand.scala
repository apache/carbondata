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
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataLoadTableFileMapping, UpdateTableModel}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FindDataSourceTable, HadoopFsRelation, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, FileUtils, SparkUtil}

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.{DictionaryServer, NonSecureDictionaryServer}
import org.apache.carbondata.core.dictionary.service.NonSecureDictionaryServiceProvider
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util._
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.processing.loading.{ComplexDelimitersEnum, TableProcessingOperations}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.{CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.dictionary.provider.SecureDictionaryServiceProvider
import org.apache.carbondata.spark.dictionary.server.SecureDictionaryServer
import org.apache.carbondata.spark.load.{CsvRDDHelper, DataLoadProcessorStepOnSpark}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.{CarbonScalaUtil, GlobalDictionaryUtil}

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

  var currPartitions: util.List[PartitionSpec] = _

  var parentTablePath: String = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    setAuditTable(dbName, tableName)
    table = if (tableInfoOp.isDefined) {
        CarbonTable.buildFromTableInfo(tableInfoOp.get)
      } else {
        val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        if (relation == null) {
          throw new NoSuchTableException(dbName, tableName)
        }
        if (null == relation.carbonTable) {
          LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
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
    if (table.isChildDataMap) {
      val parentTableIdentifier = table.getTableInfo.getParentRelationIdentifiers.get(0)
      parentTablePath = CarbonEnv
        .getCarbonTable(Some(parentTableIdentifier.getDatabaseName),
          parentTableIdentifier.getTableName)(sparkSession).getTablePath
    }
    operationContext.setProperty("isOverwrite", isOverwriteTable)
    if(CarbonUtil.hasAggregationDataMap(table)) {
      val loadMetadataEvent = new LoadMetadataEvent(table, false, options.asJava)
      OperationListenerBus.getInstance().fireEvent(loadMetadataEvent, operationContext)
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    var concurrentLoadLock: Option[ICarbonLock] = None
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    currPartitions = if (table.isHivePartitionTable) {
      CarbonFilters.getCurrentPartitions(
        sparkSession,
        table) match {
        case Some(parts) => new util.ArrayList(parts.toList.asJava)
        case _ => null
      }
    } else {
      null
    }

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
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    optionsFinal
      .put("complex_delimiter_level_4",
        ComplexDelimitersEnum.COMPLEX_DELIMITERS_LEVEL_4.value())

    /**
    * Priority of sort_scope assignment :
    * -----------------------------------
    *
    * 1. Load Options  ->
    *     LOAD DATA INPATH 'data.csv' INTO TABLE tableName OPTIONS('sort_scope'='no_sort')
    *
    * 2. Session property CARBON_TABLE_LOAD_SORT_SCOPE  ->
    *     SET CARBON.TABLE.LOAD.SORT.SCOPE.database.table=local_sort
    *
    * 3. Sort Scope provided in TBLPROPERTIES
    * 4. Session property CARBON_OPTIONS_SORT_SCOPE
    * 5. Default Sort Scope LOAD_SORT_SCOPE
    */
    if (table.getNumberOfSortColumns == 0) {
      // If tableProperties.SORT_COLUMNS is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        SortScopeOptions.SortScope.NO_SORT.name)
    } else if (StringUtils.isBlank(tableProperties.get(CarbonCommonConstants.SORT_SCOPE))) {
      // If tableProperties.SORT_COLUMNS is not null
      // and tableProperties.SORT_SCOPE is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE +
            table.getDatabaseName + "." + table.getTableName,
            carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
              carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                SortScopeOptions.SortScope.LOCAL_SORT.name)))))
    } else {
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(
            CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE + table.getDatabaseName + "." +
            table.getTableName,
            tableProperties.asScala.getOrElse(CarbonCommonConstants.SORT_SCOPE,
              carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
                carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                  CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))))
    }

    optionsFinal
      .put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options.asJava, table))
    val factPath = if (dataFrame.isDefined) {
      ""
    } else {
      FileUtils.getPaths(factPathFromUser, hadoopConf)
    }
    carbonLoadModel.setParentTablePath(parentTablePath)
    carbonLoadModel.setFactFilePath(factPath)
    carbonLoadModel.setCarbonTransactionalTable(table.getTableInfo.isTransactionalTable)
    carbonLoadModel.setAggLoadRequest(
      internalOptions.getOrElse(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL,
        CarbonCommonConstants.IS_INTERNAL_LOAD_CALL_DEFAULT).toBoolean)
    carbonLoadModel.setSegmentId(internalOptions.getOrElse("mergedSegmentName", ""))
    val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel.setRangePartitionColumn(table.getRangeColumn)

    val javaPartition = mutable.Map[String, String]()
    partition.foreach { case (k, v) =>
      if (v.isEmpty) javaPartition(k) = null else javaPartition(k) = v.get
    }

    new CarbonLoadModelBuilder(table).build(
      options.asJava,
      optionsFinal,
      carbonLoadModel,
      hadoopConf,
      javaPartition.asJava,
      dataFrame.isDefined)
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
          carbonLoadModel)
      operationContext.setProperty("isOverwrite", isOverwriteTable)
      OperationListenerBus.getInstance.fireEvent(loadTablePreExecutionEvent, operationContext)
      // Add pre event listener for index datamap
      val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(table)
      val dataMapOperationContext = new OperationContext()
      if (tableDataMaps.size() > 0) {
        val dataMapNames: mutable.Buffer[String] =
          tableDataMaps.asScala.map(dataMap => dataMap.getDataMapSchema.getDataMapName)
        val buildDataMapPreExecutionEvent: BuildDataMapPreExecutionEvent =
          new BuildDataMapPreExecutionEvent(sparkSession,
            table.getAbsoluteTableIdentifier, dataMapNames)
        OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent,
          dataMapOperationContext)
      }
      // First system has to partition the data first and then call the load data
      LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
      concurrentLoadLock = acquireConcurrentLoadLock()
      // Clean up the old invalid segment data before creating a new entry for new load.
      SegmentStatusManager.deleteLoadsAndUpdateMetadata(table, false, currPartitions)
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
      if (carbonLoadModel.isCarbonTransactionalTable) {
        val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
        val fileType = FileFactory.getFileType(metadataDirectoryPath)
        if (!FileFactory.isFileExist(metadataDirectoryPath, fileType)) {
          FileFactory.mkdirs(metadataDirectoryPath, fileType)
        }
      } else {
        carbonLoadModel.setSegmentId(System.currentTimeMillis().toString)
      }
      val partitionStatus = SegmentStatus.SUCCESS
      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      if (carbonLoadModel.getUseOnePass) {
        loadDataUsingOnePass(
          sparkSession,
          carbonProperty,
          carbonLoadModel,
          columnar,
          partitionStatus,
          hadoopConf,
          operationContext,
          LOGGER)
      } else {
        loadData(
          sparkSession,
          carbonLoadModel,
          columnar,
          partitionStatus,
          hadoopConf,
          operationContext,
          LOGGER)
      }
      val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
        new LoadTablePostExecutionEvent(
          table.getCarbonTableIdentifier,
          carbonLoadModel)
      OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
      if (tableDataMaps.size() > 0) {
        val buildDataMapPostExecutionEvent = BuildDataMapPostExecutionEvent(sparkSession,
          table.getAbsoluteTableIdentifier, null, Seq(carbonLoadModel.getSegmentId), false)
        OperationListenerBus.getInstance()
          .fireEvent(buildDataMapPostExecutionEvent, dataMapOperationContext)
      }

    } catch {
      case CausedBy(ex: NoRetryException) =>
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        LOGGER.error(s"Dataload failure for $dbName.$tableName", ex)
        throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ex.getMessage}")
      // In case of event related exception
      case preEventEx: PreEventException =>
        LOGGER.error(s"Dataload failure for $dbName.$tableName", preEventEx)
        throw new AnalysisException(preEventEx.getMessage)
      case ex: Exception =>
        LOGGER.error(ex)
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired && !table.isChildDataMap) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        throw ex
    } finally {
      releaseConcurrentLoadLock(concurrentLoadLock, LOGGER)
      // Once the data load is successful delete the unwanted partition files
      val partitionLocation = CarbonProperties.getStorePath + "/partition/" +
                              table.getDatabaseName + "/" +
                              table.getTableName + "/"
      val fileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, fileType)) {
        val file = FileFactory.getCarbonFile(partitionLocation, fileType)
        CarbonUtil.deleteFoldersAndFiles(file)
      }
    }
    Seq.empty
  }

  private def acquireConcurrentLoadLock(): Option[ICarbonLock] = {
    val isConcurrentLockRequired = table.getAllDimensions.asScala
      .exists(cd => cd.hasEncoding(Encoding.DICTIONARY) &&
                    !cd.hasEncoding(Encoding.DIRECT_DICTIONARY))

    if (isConcurrentLockRequired) {
      var concurrentLoadLock: ICarbonLock = CarbonLockFactory.getCarbonLockObj(
        table.getTableInfo().getOrCreateAbsoluteTableIdentifier(),
        LockUsage.CONCURRENT_LOAD_LOCK)
      val retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
          CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT)
      val maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
          CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT)
      if (!(isConcurrentLockRequired &&
            concurrentLoadLock.lockWithRetries(retryCount, maxTimeout))) {
        throw new RuntimeException(table.getDatabaseName + "." + table.getTableName +
                                   " having dictionary column. so concurrent load is not supported")
      }
      return Some(concurrentLoadLock)
    }
    return None
  }

  private def releaseConcurrentLoadLock(concurrentLoadLock: Option[ICarbonLock],
      LOGGER: Logger): Unit = {
    if (concurrentLoadLock.isDefined) {
      if (concurrentLoadLock.get.unlock()) {
        LOGGER.info("concurrent_load lock for table" + table.getTablePath +
                    "has been released successfully")
      } else {
        LOGGER.error(
          "Unable to unlock concurrent_load lock for table" + table.getTablePath);
      }
    }
  }

  private def loadDataUsingOnePass(
      sparkSession: SparkSession,
      carbonProperty: CarbonProperties,
      carbonLoadModel: CarbonLoadModel,
      columnar: Boolean,
      partitionStatus: SegmentStatus,
      hadoopConf: Configuration,
      operationContext: OperationContext,
      LOGGER: Logger): Seq[Row] = {
    var rows = Seq.empty[Row]
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
        rows = loadDataWithPartition(
          sparkSession,
          carbonLoadModel,
          hadoopConf,
          loadDataFrame,
          operationContext,
          LOGGER)
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
      val loadResult = CarbonDataRDDFactory.loadCarbonData(
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
      if (loadResult != null) {
        val info = makeAuditInfo(loadResult)
        setAuditInfo(info)
      }
    }
    rows
  }

  private def makeAuditInfo(loadResult: LoadMetadataDetails): Map[String, String] = {
    if (loadResult != null) {
      Map(
        "SegmentId" -> loadResult.getLoadName,
        "DataSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getDataSize)),
        "IndexSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getIndexSize)))
    } else {
      Map()
    }
  }

  private def loadData(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      columnar: Boolean,
      partitionStatus: SegmentStatus,
      hadoopConf: Configuration,
      operationContext: OperationContext,
      LOGGER: Logger): Seq[Row] = {
    var rows = Seq.empty[Row]
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
    GlobalDictionaryUtil.generateGlobalDictionary(
      sparkSession.sqlContext,
      carbonLoadModel,
      hadoopConf,
      dictionaryDataFrame)
    if (table.isHivePartitionTable) {
      rows = loadDataWithPartition(
        sparkSession,
        carbonLoadModel,
        hadoopConf,
        loadDataFrame,
        operationContext, LOGGER)
    } else {
      val loadResult = CarbonDataRDDFactory.loadCarbonData(
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
      val info = makeAuditInfo(loadResult)
      setAuditInfo(info)
    }
    rows
  }

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitioned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  private def loadDataWithPartition(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration,
      dataFrame: Option[DataFrame],
      operationContext: OperationContext,
      LOGGER: Logger): Seq[Row] = {
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val catalogTable: CatalogTable = logicalPartitionRelation.catalogTable.get
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
    // Clean up the alreday dropped partitioned data
    SegmentFileStore.cleanSegments(table, null, false)
    CarbonSession.threadSet("partition.operationcontext", operationContext)
    // input data from csv files. Convert to logical plan
    val allCols = new ArrayBuffer[String]()
    // get only the visible dimensions from table
    allCols ++= table.getDimensionByTableName(table.getTableName).asScala.map(_.getColName)
    allCols ++= table.getMeasureByTableName(table.getTableName).asScala.map(_.getColName)
    var attributes =
      StructType(
        allCols.filterNot(_.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)).map(
          StructField(_, StringType))).toAttributes

    var partitionsLen = 0
    val sortScope = CarbonDataProcessorUtil.getSortScope(carbonLoadModel.getSortScope)
    val partitionValues = if (partition.nonEmpty) {
      partition.filter(_._2.nonEmpty).map { case (col, value) =>
        catalogTable.schema.find(_.name.equalsIgnoreCase(col)) match {
          case Some(c) =>
            CarbonScalaUtil.convertToDateAndTimeFormats(
              value.get,
              c.dataType,
              timeStampFormat,
              dateFormat)
          case None =>
            throw new AnalysisException(s"$col is not a valid partition column in table ${
              carbonLoadModel
                .getDatabaseName
            }.${ carbonLoadModel.getTableName }")
        }
      }.toArray
    } else {
      Array[String]()
    }
    var persistedRDD: Option[RDD[InternalRow]] = None
    try {
      val query: LogicalPlan = if (dataFrame.isDefined) {
        val (rdd, dfAttributes) = if (updateModel.isDefined) {
          // Get the updated query plan in case of update scenario
          val updatedFrame = Dataset.ofRows(
            sparkSession,
            getLogicalQueryForUpdate(
              sparkSession,
              catalogTable,
              dataFrame.get,
              carbonLoadModel))
          (updatedFrame.rdd, updatedFrame.schema)
        } else {
          if (partition.nonEmpty) {
            val headers = carbonLoadModel.getCsvHeaderColumns.dropRight(partition.size)
            val updatedHeader = headers ++ partition.keys.map(_.toLowerCase)
            carbonLoadModel.setCsvHeader(updatedHeader.mkString(","))
            carbonLoadModel.setCsvHeaderColumns(carbonLoadModel.getCsvHeader.split(","))
          }
          (dataFrame.get.rdd, dataFrame.get.schema)
        }

        val expectedColumns = {
          val staticPartCols = partition.filter(_._2.isDefined).keySet
          attributes.filterNot(a => staticPartCols.contains(a.name))
        }
        if (expectedColumns.length != dfAttributes.length) {
          throw new AnalysisException(
            s"Cannot insert into table $tableName because the number of columns are different: " +
            s"need ${expectedColumns.length} columns, " +
            s"but query has ${dfAttributes.length} columns.")
        }
        val nonPartitionBounds = expectedColumns.zipWithIndex.map(_._2).toArray
        val partitionBounds = new Array[Int](partitionValues.length)
        if (partition.nonEmpty) {
          val nonPartitionSchemaLen = attributes.length - partition.size
          var i = nonPartitionSchemaLen
          var index = 0
          var partIndex = 0
          partition.values.foreach { p =>
            if (p.isDefined) {
              partitionBounds(partIndex) = nonPartitionSchemaLen + index
              partIndex = partIndex + 1
            } else {
              nonPartitionBounds(i) = nonPartitionSchemaLen + index
              i = i + 1
            }
            index = index + 1
          }
        }

        val len = dfAttributes.length + partitionValues.length
        val transRdd = rdd.map { f =>
          val data = new Array[Any](len)
          var i = 0
          while (i < f.length) {
            data(nonPartitionBounds(i)) = f.get(i)
            i = i + 1
          }
          var j = 0
          while (j < partitionBounds.length) {
            data(partitionBounds(j)) = UTF8String.fromString(partitionValues(j))
            j = j + 1
          }
          Row.fromSeq(data)
        }

        val (transformedPlan, partitions, persistedRDDLocal) =
          transformQuery(
            transRdd,
            sparkSession,
            carbonLoadModel,
            partitionValues,
            catalogTable,
            attributes,
            sortScope,
            isDataFrame = true)
        partitionsLen = partitions
        persistedRDD = persistedRDDLocal
        transformedPlan
      } else {
        val columnCount = carbonLoadModel.getCsvHeaderColumns.length
        val rdd = CsvRDDHelper.csvFileScanRDD(
          sparkSession,
          model = carbonLoadModel,
          hadoopConf).map(DataLoadProcessorStepOnSpark.toStringArrayRow(_, columnCount))
        val (transformedPlan, partitions, persistedRDDLocal) =
          transformQuery(
            rdd.asInstanceOf[RDD[Row]],
            sparkSession,
            carbonLoadModel,
            partitionValues,
            catalogTable,
            attributes,
            sortScope,
            isDataFrame = false)
        partitionsLen = partitions
        persistedRDD = persistedRDDLocal
        transformedPlan
      }
      if (updateModel.isDefined) {
        carbonLoadModel.setFactTimeStamp(updateModel.get.updatedTimeStamp)
      } else if (carbonLoadModel.getFactTimeStamp == 0L) {
        carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      }
      // Create and ddd the segment to the tablestatus.
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, isOverwriteTable)
      val convertRelation = convertToLogicalRelation(
        catalogTable,
        sizeInBytes,
        isOverwriteTable,
        carbonLoadModel,
        sparkSession,
        operationContext)
      val logicalPlan = if (sortScope == SortScopeOptions.SortScope.GLOBAL_SORT) {
        var numPartitions =
          CarbonDataProcessorUtil.getGlobalSortPartitions(carbonLoadModel.getGlobalSortPartitions)
        if (numPartitions <= 0) {
          numPartitions = partitionsLen
        }
        if (numPartitions > 0) {
          Dataset.ofRows(sparkSession, query).repartition(numPartitions).logicalPlan
        } else {
          query
        }
      } else {
        query
      }

      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = partition,
          query = logicalPlan,
          overwrite = false,
          ifPartitionNotExists = false)
      SparkUtil.setNullExecutionId(sparkSession)
      Dataset.ofRows(sparkSession, convertedPlan)
    } catch {
      case ex: Throwable =>
        val (executorMessage, errorMessage) = CarbonScalaUtil.retrieveAndLogErrorMsg(ex, LOGGER)
        if (updateModel.isDefined) {
          CarbonScalaUtil.updateErrorInUpdateModel(updateModel.get, executorMessage)
        }
        LOGGER.info(errorMessage)
        LOGGER.error(ex)
        throw ex
    } finally {
      CarbonSession.threadUnset("partition.operationcontext")
      if (isOverwriteTable) {
        DataMapStoreManager.getInstance().clearDataMaps(table.getAbsoluteTableIdentifier)
        // Clean the overwriting segments if any.
        SegmentFileStore.cleanSegments(
          table,
          null,
          false)
      }
      if (partitionsLen > 1) {
        // clean cache only if persisted and keeping unpersist non-blocking as non-blocking call
        // will not have any functional impact as spark automatically monitors the cache usage on
        // each node and drops out old data partitions in a least-recently used (LRU) fashion.
        persistedRDD match {
          case Some(rdd) => rdd.unpersist(false)
          case _ =>
        }
      }
    }
    try {
      val compactedSegments = new util.ArrayList[String]()
      // Trigger auto compaction
      CarbonDataRDDFactory.handleSegmentMerging(
        sparkSession.sqlContext,
        carbonLoadModel
          .getCopyWithPartition(carbonLoadModel.getCsvHeader, carbonLoadModel.getCsvDelimiter),
        table,
        compactedSegments,
        operationContext)
      carbonLoadModel.setMergedSegmentIds(compactedSegments)
    } catch {
      case e: Exception =>
        LOGGER.error(
          "Auto-Compaction has failed. Ignoring this exception because the " +
          "load is passed.", e)
    }
    val specs =
      SegmentFileStore.getPartitionSpecs(carbonLoadModel.getSegmentId, carbonLoadModel.getTablePath,
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(table.getTablePath)))
    if (specs != null) {
      specs.asScala.map{ spec =>
        Row(spec.getPartitions.asScala.mkString("/"), spec.getLocation.toString, spec.getUuid)
      }
    } else {
      Seq.empty[Row]
    }
  }

  /**
   * Transform the rdd to logical plan as per the sortscope. If it is global sort scope then it
   * will convert to sort logical plan otherwise project plan.
   */
  private def transformQuery(rdd: RDD[Row],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      isDataFrame: Boolean): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    // Converts the data as per the loading steps before give it to writer or sorter
    val updatedRdd = convertData(
      rdd,
      sparkSession,
      loadModel,
      isDataFrame,
      partitionValues)
    val catalogAttributes = catalogTable.schema.toAttributes
    var attributes = curAttributes.map(a => {
      catalogAttributes.find(_.name.equalsIgnoreCase(a.name)).get
    })
    attributes = attributes.map { attr =>
      // Update attribute datatypes in case of dictionary columns, in case of dictionary columns
      // datatype is always int
      val column = table.getColumnByName(table.getTableName, attr.name)
      if (column.hasEncoding(Encoding.DICTIONARY)) {
        CarbonToSparkAdapter.createAttributeReference(attr.name,
          IntegerType,
          attr.nullable,
          attr.metadata,
          attr.exprId,
          attr.qualifier,
          attr)
      } else if (attr.dataType == TimestampType || attr.dataType == DateType) {
        CarbonToSparkAdapter.createAttributeReference(attr.name,
          LongType,
          attr.nullable,
          attr.metadata,
          attr.exprId,
          attr.qualifier,
          attr)
      } else {
        attr
      }
    }
    // Only select the required columns
    var output = if (partition.nonEmpty) {
      val lowerCasePartition = partition.map { case (key, value) => (key.toLowerCase, value) }
      catalogTable.schema.map { attr =>
        attributes.find(_.name.equalsIgnoreCase(attr.name)).get
      }.filter(attr => lowerCasePartition.getOrElse(attr.name.toLowerCase, None).isEmpty)
    } else {
      catalogTable.schema.map(f => attributes.find(_.name.equalsIgnoreCase(f.name)).get)
    }
    // Rearrange the partition column at the end of output list
    if (catalogTable.partitionColumnNames.nonEmpty &&
        (loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildTable ||
         loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildDataMap) && output.nonEmpty) {
      val partitionOutPut =
        catalogTable.partitionColumnNames.map(col => output.find(_.name.equalsIgnoreCase(col)).get)
      output = output.filterNot(partitionOutPut.contains(_)) ++ partitionOutPut
    }
    val partitionsLen = rdd.partitions.length

    // If it is global sort scope then appl sort logical plan on the sort columns
    if (sortScope == SortScopeOptions.SortScope.GLOBAL_SORT) {
      // Because if the number of partitions greater than 1, there will be action operator(sample)
      // in sortBy operator. So here we cache the rdd to avoid do input and convert again.
      if (partitionsLen > 1) {
        updatedRdd.persist(StorageLevel.fromString(
          CarbonProperties.getInstance().getGlobalSortRddStorageLevel))
      }
      val child = Project(output, LogicalRDD(attributes, updatedRdd)(sparkSession))
      val sortColumns = table.getSortColumns(table.getTableName)
      val sortPlan =
        Sort(
          output.filter(f => sortColumns.contains(f.name)).map(SortOrder(_, Ascending)),
          global = true,
          child)
      (sortPlan, partitionsLen, Some(updatedRdd))
    } else {
      (Project(output, LogicalRDD(attributes, updatedRdd)(sparkSession)), partitionsLen, None)
    }
  }

  /**
   * Convert the rdd as per steps of data loading inputprocessor step and converter step
   * @param originRDD
   * @param sparkSession
   * @param model
   * @param isDataFrame
   * @param partitionValues
   * @return
   */
  private def convertData(
      originRDD: RDD[Row],
      sparkSession: SparkSession,
      model: CarbonLoadModel,
      isDataFrame: Boolean,
      partitionValues: Array[String]): RDD[InternalRow] = {
    val sc = sparkSession.sparkContext
    val info =
      model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo.getFactTable.getPartitionInfo
    info.setColumnSchemaList(new util.ArrayList[ColumnSchema](info.getColumnSchemaList))
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.accumulator(0, "Partial Success Accumulator")

    val inputStepRowCounter = sc.accumulator(0, "Input Processor Accumulator")
    // 1. Input
    val convertRDD =
      if (isDataFrame) {
        originRDD.mapPartitions{rows =>
          DataLoadProcessorStepOnSpark.toRDDIterator(rows, modelBroadcast)
        }
      } else {
        // Append the partition columns in case of static partition scenario
        val partitionLen = partitionValues.length
        val len = model.getCsvHeaderColumns.length - partitionLen
        originRDD.map{ row =>
          val array = new Array[AnyRef](len + partitionLen)
          var i = 0
          while (i < len) {
            array(i) = row.get(i).asInstanceOf[AnyRef]
            i = i + 1
          }
          if (partitionLen > 0) {
            System.arraycopy(partitionValues, 0, array, i, partitionLen)
          }
          array
        }
      }
    val conf = SparkSQLUtil
      .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
    val finalRDD = convertRDD.mapPartitionsWithIndex { case(index, rows) =>
        DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
        DataLoadProcessorStepOnSpark.inputAndconvertFunc(
          rows,
          index,
          modelBroadcast,
          partialSuccessAccum,
          inputStepRowCounter,
          keepActualData = true)
      }.filter(_ != null).map(row => InternalRow.fromSeq(row.getData))

    finalRDD
  }

  /**
   * Create the logical plan for update scenario. Here we should drop the segmentId column from the
   * input rdd.
   */
  private def getLogicalQueryForUpdate(
      sparkSession: SparkSession,
      catalogTable: CatalogTable,
      df: DataFrame,
      carbonLoadModel: CarbonLoadModel): LogicalPlan = {
    SparkUtil.setNullExecutionId(sparkSession)
    // In case of update, we don't need the segmrntid column in case of partitioning
    val dropAttributes = df.logicalPlan.output.dropRight(1)
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
    carbonLoadModel.setCsvHeader(catalogTable.schema.map(_.name.toLowerCase).mkString(","))
    carbonLoadModel.setCsvHeaderColumns(carbonLoadModel.getCsvHeader.split(","))
    Project(finalOutput, df.logicalPlan)
  }

  private def convertToLogicalRelation(
      catalogTable: CatalogTable,
      sizeInBytes: Long,
      overWrite: Boolean,
      loadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext): LogicalRelation = {
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val metastoreSchema = StructType(catalogTable.schema.fields.map{f =>
      val column = table.getColumnByName(table.getTableName, f.name)
      if (column.hasEncoding(Encoding.DICTIONARY)) {
        f.copy(dataType = IntegerType)
      } else if (f.dataType == TimestampType || f.dataType == DateType) {
        f.copy(dataType = LongType)
      } else {
        f
      }
    })
    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val catalog = new CatalogFileIndex(
      sparkSession, catalogTable, sizeInBytes)
    if (lazyPruningEnabled) {
      catalog
    } else {
      catalog.filterPartitions(Nil) // materialize all the partitions in memory
    }
    var partitionSchema =
      StructType(table.getPartitionInfo(table.getTableName).getColumnSchemaList.asScala.map(field =>
        metastoreSchema.fields.find(_.name.equalsIgnoreCase(field.getColumnName))).map(_.get))
    val dataSchema =
      StructType(metastoreSchema
        .filterNot(field => partitionSchema.contains(field)))
    if (partition.nonEmpty) {
      partitionSchema = StructType(partitionSchema.fields.map(_.copy(dataType = StringType)))
    }
    val options = new mutable.HashMap[String, String]()
    options ++= catalogTable.storage.properties
    options += (("overwrite", overWrite.toString))
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
        options += (("segmentsToBeDeleted",
          updateModel.get.deletedSegments.map(_.getSegmentNo).mkString(",")))
      }
    }
    if (currPartitions != null) {
      val currPartStr = ObjectSerializationUtil.convertObjectToString(currPartitions)
      options += (("currentpartition", currPartStr))
    }
    if (loadModel.getSegmentId != null) {
      val currLoadEntry =
        ObjectSerializationUtil.convertObjectToString(loadModel.getCurrentLoadMetadataDetail)
      options += (("currentloadentry", currLoadEntry))
    }
    val hdfsRelation = HadoopFsRelation(
      location = catalog,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = catalogTable.bucketSpec,
      fileFormat = new SparkCarbonTableFormat,
      options = options.toMap)(sparkSession = sparkSession)

    CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
      hdfsRelation.schema.toAttributes,
      Some(catalogTable),
      false)
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

  override protected def opName: String = {
    if (isOverwriteTable) {
      "LOAD DATA OVERWRITE"
    } else {
      "LOAD DATA"
    }
  }
}
