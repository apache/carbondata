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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataLoadTableFileMapping, UpdateTableModel}
import org.apache.spark.sql.execution.datasources._
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
import org.apache.carbondata.core.indexstore.PartitionSpec
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
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.{ComplexDelimitersEnum, TableProcessingOperations}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.load.{CsvRDDHelper, DataLoadProcessorStepOnSpark, GlobalSortHelper}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

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

  var finalPartition : Map[String, Option[String]] = Map.empty

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    setAuditTable(dbName, tableName)
    table = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      CarbonEnv.getCarbonTable(Option(dbName), tableName)(sparkSession)
    }
    if (table.isHivePartitionTable) {
      logicalPartitionRelation =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
            TableIdentifier(tableName, databaseNameOp))).collect {
          case l: LogicalRelation => l
        }.head
      sizeInBytes = logicalPartitionRelation.relation.sizeInBytes
      finalPartition = getCompletePartitionValues(sparkSession)
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
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
    EnvHelper.setDefaultHeader(sparkSession, optionsFinal)

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
      if (!StringUtils.isBlank(tableProperties.get("global_sort_partitions"))) {
        if (options.get("global_sort_partitions").isEmpty) {
          optionsFinal.put(
            "global_sort_partitions",
            tableProperties.get("global_sort_partitions"))
        }
      }
    }

    optionsFinal
      .put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options.asJava, table))
    val factPath = if (dataFrame.isDefined) {
      ""
    } else {
      FileUtils.getPaths(factPathFromUser, hadoopConf)
    }
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
    val uuid = ""
    try {
      operationContext.setProperty("uuid", uuid)
      if (updateModel.isDefined && updateModel.get.isUpdate) {
        operationContext.setProperty("isLoadOrCompaction", false)
      }
      val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
        new LoadTablePreExecutionEvent(
          table.getCarbonTableIdentifier,
          carbonLoadModel,
          factPath,
          dataFrame.isDefined,
          optionsFinal,
          options.asJava,
          isOverwriteTable,
          sparkSession)
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

      // Create table and metadata folders if not exist
      if (carbonLoadModel.isCarbonTransactionalTable) {
        val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
        if (!FileFactory.isFileExist(metadataDirectoryPath)) {
          FileFactory.mkdirs(metadataDirectoryPath)
        }
      } else {
        carbonLoadModel.setSegmentId(System.nanoTime().toString)
      }
      val partitionStatus = SegmentStatus.SUCCESS
      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean
      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      loadData(
        sparkSession,
        carbonLoadModel,
        columnar,
        partitionStatus,
        hadoopConf,
        operationContext,
        LOGGER)
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
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        throw ex
    }
    Seq.empty
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
    val loadDataFrame = if (updateModel.isDefined) {
      Some(getDataFrameWithTupleID())
    } else {
      dataFrame
    }
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
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
    CarbonUtils.threadSet("partition.operationcontext", operationContext)
    // input data from csv files. Convert to logical plan
    val allCols = new ArrayBuffer[String]()
    // get only the visible dimensions from table
    allCols ++= table.getVisibleDimensions().asScala.filterNot(_.isIndexColumn).map(_.getColName)
    allCols ++= table.getVisibleMeasures.asScala.map(_.getColName)
    var attributes =
      StructType(
        allCols.filterNot(_.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)).map(
          StructField(_, StringType))).toAttributes

    var partitionsLen = 0
    val sortScope = CarbonDataProcessorUtil.getSortScope(carbonLoadModel.getSortScope)
    val partitionValues = if (finalPartition.nonEmpty) {
      finalPartition.filter(_._2.nonEmpty).map { case (col, value) =>
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
          if (finalPartition.nonEmpty) {
            val headers = carbonLoadModel.getCsvHeaderColumns.dropRight(finalPartition.size)
            val updatedHeader = headers ++ finalPartition.keys.map(_.toLowerCase)
            carbonLoadModel.setCsvHeader(updatedHeader.mkString(","))
            carbonLoadModel.setCsvHeaderColumns(carbonLoadModel.getCsvHeader.split(","))
          }
          (dataFrame.get.rdd, dataFrame.get.schema)
        }

        val expectedColumns = {
          val staticPartCols = finalPartition.filter(_._2.isDefined).keySet
            .map(columnName => columnName.toLowerCase())
          attributes.filterNot(a => staticPartCols.contains(a.name.toLowerCase))
        }
        if (expectedColumns.length != dfAttributes.length) {
          throw new AnalysisException(
            s"Cannot insert into table $tableName because the number of columns are different: " +
            s"need ${expectedColumns.length} columns, " +
            s"but query has ${dfAttributes.length} columns.")
        }
        val nonPartitionBounds = expectedColumns.zipWithIndex.map(_._2).toArray
        val partitionBounds = new Array[Int](partitionValues.length)
        if (finalPartition.nonEmpty) {
          val nonPartitionSchemaLen = attributes.length - finalPartition.size
          var i = nonPartitionSchemaLen
          var index = 0
          var partIndex = 0
          finalPartition.values.foreach { p =>
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
          val length = f.length
          while (i < length) {
            data(nonPartitionBounds(i)) = f.get(i)
            i = i + 1
          }
          var j = 0
          val boundLength = partitionBounds.length
          while (j < boundLength) {
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
      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = finalPartition,
          query = query,
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
      CarbonUtils.threadUnset("partition.operationcontext")
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

      // Prepriming for Partition table here
      if (!StringUtils.isEmpty(carbonLoadModel.getSegmentId)) {
        DistributedRDDUtils.triggerPrepriming(sparkSession,
          table,
          Seq(),
          operationContext,
          hadoopConf,
          List(carbonLoadModel.getSegmentId))
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
    val catalogAttributes = catalogTable.schema.toAttributes
    // Converts the data as per the loading steps before give it to writer or sorter
    val updatedRdd = convertData(
      rdd,
      sparkSession,
      loadModel,
      isDataFrame,
      partitionValues)
    var attributes = curAttributes.map(a => {
      catalogAttributes.find(_.name.equalsIgnoreCase(a.name)).get
    })
    attributes = attributes.map { attr =>
      // Update attribute datatypes in case of dictionary columns, in case of dictionary columns
      // datatype is always int
      val column = table.getColumnByName(attr.name)
      val updatedDataType = if (column.getDataType ==
                                org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
        IntegerType
      } else {
        attr.dataType match {
          case TimestampType | DateType =>
            LongType
          case _: StructType | _: ArrayType | _: MapType =>
            BinaryType
          case _ =>
            attr.dataType
        }
      }
      CarbonToSparkAdapter.createAttributeReference(
        attr.name,
        updatedDataType,
        attr.nullable,
        attr.metadata,
        attr.exprId,
        attr.qualifier)
    }
    // Only select the required columns
    var output = if (finalPartition.nonEmpty) {
      val lowerCasePartition = finalPartition.map {
        case (key, value) => (key.toLowerCase, value)
      }
      catalogTable.schema.map { attr =>
        attributes.find(_.name.equalsIgnoreCase(attr.name)).get
      }.filter(attr => lowerCasePartition.getOrElse(attr.name.toLowerCase, None).isEmpty)
    } else {
      catalogTable.schema.map(f => attributes.find(_.name.equalsIgnoreCase(f.name)).get)
    }
    // Rearrange the partition column at the end of output list
    if (catalogTable.partitionColumnNames.nonEmpty &&
        (loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildTableForMV) && output.nonEmpty) {
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
      var numPartitions =
        CarbonDataProcessorUtil.getGlobalSortPartitions(loadModel.getGlobalSortPartitions)
      if (numPartitions <= 0) {
        numPartitions = partitionsLen
      }
      val sortColumns = attributes.take(table.getSortColumns().size())
      val sortedRDD: RDD[InternalRow] =
        GlobalSortHelper.sortBy(updatedRdd, numPartitions, sortColumns)
      val outputOrdering = sortColumns.map(SortOrder(_, Ascending))
      (
        Project(
          output,
          LogicalRDD(attributes, sortedRDD, outputOrdering = outputOrdering)(sparkSession)
        ),
        partitionsLen,
        Some(updatedRdd)
      )
    } else {
      (
        Project(output, LogicalRDD(attributes, updatedRdd)(sparkSession)),
        partitionsLen,
        None
      )
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
    val partialSuccessAccum = sc.longAccumulator("Partial Success Accumulator")

    val inputStepRowCounter = sc.longAccumulator("Input Processor Accumulator")
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
        val index = d.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION)
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
      val column = table.getColumnByName(f.name)
      val updatedDataType = if (column.getDataType ==
                                org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
        IntegerType
      } else {
        f.dataType match {
          case TimestampType | DateType =>
            LongType
          case _: StructType | _: ArrayType | _: MapType =>
            BinaryType
          case _ =>
            f.dataType
        }
      }
      f.copy(dataType = updatedDataType)
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
      StructType(table.getPartitionInfo().getColumnSchemaList.asScala.map(field =>
        metastoreSchema.fields.find(_.name.equalsIgnoreCase(field.getColumnName))).map(_.get))
    val dataSchema =
      StructType(metastoreSchema
        .filterNot(field => partitionSchema.contains(field)))
    if (finalPartition.nonEmpty) {
      partitionSchema = StructType(partitionSchema.fields.map(_.copy(dataType = StringType)))
    }
    val options = new mutable.HashMap[String, String]()
    options ++= catalogTable.storage.properties
    options += (("overwrite", overWrite.toString))
    if (finalPartition.nonEmpty) {
      val staticPartitionStr = ObjectSerializationUtil.convertObjectToString(
        new util.HashMap[String, Boolean](
          finalPartition.map{case (col, value) => (col.toLowerCase, value.isDefined)}.asJava))
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

  def getCompletePartitionValues(sparkSession: SparkSession): Map[String, Option[String]] = {
    if (partition.nonEmpty) {
      val lowerCasePartitionMap = partition.map { entry =>
        (entry._1.toLowerCase, entry._2)
      }
      val partitionsColumns = table.getPartitionInfo.getColumnSchemaList
      if (partition.size != partitionsColumns.size()) {
        val dynamicPartitions = {
          partitionsColumns
            .asScala
            .filter { column =>
              !lowerCasePartitionMap.contains(column.getColumnName)
            }
            .map { column =>
              (column.getColumnName, None)
            }
        }
        partition ++ dynamicPartitions
      } else {
        partition
      }
    } else {
      partition
    }
  }

  override protected def opName: String = {
    if (isOverwriteTable) {
      "LOAD DATA OVERWRITE"
    } else {
      "LOAD DATA"
    }
  }
}
