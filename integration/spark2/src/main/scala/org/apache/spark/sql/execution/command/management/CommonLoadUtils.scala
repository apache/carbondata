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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.UpdateTableModel
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FindDataSourceTable, HadoopFsRelation, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.{DataMapStoreManager, TableDataMap}
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util._
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.{CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.load.{CsvRDDHelper, DataLoadProcessorStepOnSpark, GlobalSortHelper}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

object CommonLoadUtils {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def makeAuditInfo(loadResult: LoadMetadataDetails): Map[String, String] = {
    if (loadResult != null) {
      Map(
        "SegmentId" -> loadResult.getLoadName,
        "DataSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getDataSize)),
        "IndexSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getIndexSize)))
    } else {
      Map()
    }
  }

  def getDataFrameWithTupleID(dataFrame: Option[DataFrame]): DataFrame = {
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

  def processMetadataCommon(sparkSession: SparkSession,
      databaseNameOp: Option[String],
      tableName: String,
      tableInfoOp: Option[TableInfo],
      partition: Map[String, Option[String]]): (Long, CarbonTable, String, LogicalRelation,
      Map[String, Option[String]]) = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    var table: CarbonTable = null
    var logicalPartitionRelation: LogicalRelation = null
    var sizeInBytes: Long = 0L

    table = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      CarbonEnv.getCarbonTable(Option(dbName), tableName)(sparkSession)
    }
    var finalPartition: Map[String, Option[String]] = Map.empty
    if (table.isHivePartitionTable) {
      logicalPartitionRelation =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
            TableIdentifier(tableName, databaseNameOp))).collect {
          case l: LogicalRelation => l
        }.head
      sizeInBytes = logicalPartitionRelation.relation.sizeInBytes
      finalPartition = getCompletePartitionValues(partition, table)
    }
    (sizeInBytes, table, dbName, logicalPartitionRelation, finalPartition)
  }

  def prepareLoadModel(hadoopConf: Configuration,
      factPath: String,
      optionsFinal: util.Map[String, String],
      parentTablePath: String,
      table: CarbonTable,
      isDataFrame: Boolean,
      internalOptions: Map[String, String],
      partition: Map[String, Option[String]],
      options: Map[java.lang.String, String]): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel()
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
      if (v.isEmpty) {
        javaPartition(k) = null
      } else {
        javaPartition(k) = v.get
      }
    }
    new CarbonLoadModelBuilder(table).build(
      options.asJava,
      optionsFinal,
      carbonLoadModel,
      hadoopConf,
      javaPartition.asJava,
      isDataFrame)
    carbonLoadModel
  }

  def setNumberOfCoresWhileLoading(sparkSession: SparkSession): CarbonProperties = {
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.NUM_CORES_LOADING, numCoresLoading)
  }

  def getCurrentParitions(sparkSession: SparkSession,
      table: CarbonTable): util.List[PartitionSpec] = {
    val currPartitions = if (table.isHivePartitionTable) {
      CarbonFilters.getCurrentPartitions(
        sparkSession,
        table) match {
        case Some(parts) => new util.ArrayList(parts.toList.asJava)
        case _ => null
      }
    } else {
      null
    }
    currPartitions
  }

  def getFinalLoadOptions(
      table: CarbonTable,
      options: Map[String, String]): util.Map[String, String] = {
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    //    EnvHelper.setDefaultHeader(sparkSession, optionsFinal)
    /**
     * Priority of sort_scope assignment :
     * -----------------------------------
     *
     * 1. Load Options  ->
     * LOAD DATA INPATH 'data.csv' INTO TABLE tableName OPTIONS('sort_scope'='no_sort')
     *
     * 2. Session property CARBON_TABLE_LOAD_SORT_SCOPE  ->
     * SET CARBON.TABLE.LOAD.SORT.SCOPE.database.table=local_sort
     *
     * 3. Sort Scope provided in TBLPROPERTIES
     * 4. Session property CARBON_OPTIONS_SORT_SCOPE
     * 5. Default Sort Scope LOAD_SORT_SCOPE
     */
    val carbonProperty = CarbonProperties.getInstance
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
    optionsFinal
  }

  def firePreLoadEvents(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      uuid: String,
      factPath: String,
      optionsFinal: util.Map[String, String],
      options: util.Map[String, String],
      isOverwriteTable: Boolean,
      isDataFrame: Boolean,
      updateModel: Option[UpdateTableModel],
      operationContext: OperationContext): (util.List[TableDataMap], OperationContext) = {
    operationContext.setProperty("uuid", uuid)
    if (updateModel.isDefined && updateModel.get.isUpdate) {
      operationContext.setProperty("isLoadOrCompaction", false)
    }
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
      new LoadTablePreExecutionEvent(
        table.getCarbonTableIdentifier,
        carbonLoadModel,
        factPath,
        isDataFrame,
        optionsFinal,
        options,
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
        BuildDataMapPreExecutionEvent(sparkSession,
          table.getAbsoluteTableIdentifier, dataMapNames)
      OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent,
        dataMapOperationContext)
    }
    (tableDataMaps, dataMapOperationContext)
  }

  def firePostLoadEvents(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      tableDataMaps: util.List[TableDataMap],
      dataMapOperationContext: OperationContext,
      table: CarbonTable,
      operationContext: OperationContext): Unit = {
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
   * Transform the rdd to logical plan as per the sortscope. If it is global sort scope then it
   * will convert to sort logical plan otherwise project plan.
   */
  def transformQueryWithRow(rdd: RDD[Row],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      isDataFrame: Boolean,
      table: CarbonTable,
      partition: Map[String, Option[String]]): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    // Converts the data as per the loading steps before give it to writer or sorter
    val updatedRdd = convertData(
      rdd,
      sparkSession,
      loadModel,
      isDataFrame,
      partitionValues)
    transformQuery(updatedRdd,
      sparkSession,
      loadModel,
      partitionValues,
      catalogTable,
      curAttributes,
      sortScope,
      isNoRearrangeFlow = false,
      table,
      partition)
  }

  def transformQuery(updatedRdd: RDD[InternalRow],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      isNoRearrangeFlow: Boolean,
      table: CarbonTable,
      partition: Map[String, Option[String]]): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    val catalogAttributes = catalogTable.schema.toAttributes
    // Converts the data as per the loading steps before give it to writer or sorter
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
        if (isNoRearrangeFlow) {
          attr.dataType match {
            case TimestampType | DateType =>
              LongType
            // complex type will be converted in CarbonOutputWriter.writeCarbon
            case _ =>
              attr.dataType
          }
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

      }
      CarbonToSparkAdapter.createAttributeReference(attr.name,
        updatedDataType,
        attr.nullable,
        attr.metadata,
        attr.exprId,
        attr.qualifier)
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
        (loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildTableForMV) && output.nonEmpty) {
      val partitionOutPut =
        catalogTable.partitionColumnNames.map(col => output.find(_.name.equalsIgnoreCase(col)).get)
      output = output.filterNot(partitionOutPut.contains(_)) ++ partitionOutPut
    }
    val partitionsLen = updatedRdd.partitions.length

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
      val sortColumns = attributes.take(table.getSortColumns.size())
      val dataTypes = sortColumns.map(_.dataType)
      val sortedRDD: RDD[InternalRow] =
        GlobalSortHelper.sortBy(updatedRdd, numPartitions, dataTypes)
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
   * Transform the rdd to logical plan as per the sortscope. If it is global sort scope then it
   * will convert to sort logical plan otherwise project plan.
   */
  def transformQueryWithInternalRow(rdd: RDD[InternalRow],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      table: CarbonTable,
      partition: Map[String, Option[String]]): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    // keep partition column to end if exists
    var colSchema = table.getTableInfo
      .getFactTable
      .getListOfColumns
      .asScala
    if (table.getPartitionInfo != null) {
      colSchema = colSchema.filterNot(x => x.isInvisible || x.getColumnName.contains(".") ||
                                           x.getSchemaOrdinal == -1 ||
                                           table.getPartitionInfo.getColumnSchemaList.contains(x))
      colSchema = colSchema ++ table
        .getPartitionInfo
        .getColumnSchemaList.toArray(new Array[ColumnSchema](table
        .getPartitionInfo
        .getColumnSchemaList.size()))
        .asInstanceOf[Array[ColumnSchema]]
    } else {
      colSchema = colSchema.filterNot(x => x.isInvisible || x.getColumnName.contains(".") ||
                                           x.getSchemaOrdinal == -1)
    }
    val updatedRdd: RDD[InternalRow] = CommonLoadUtils.getConvertedInternalRow(colSchema, rdd)
    transformQuery(updatedRdd,
      sparkSession,
      loadModel,
      partitionValues,
      catalogTable,
      curAttributes,
      sortScope,
      isNoRearrangeFlow = true,
      table,
      partition)
  }

  /**
   * Create the logical plan for update scenario. Here we should drop the segmentId column from the
   * input rdd.
   */
  def getLogicalQueryForUpdate(
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

  def convertToLogicalRelation(
      catalogTable: CatalogTable,
      sizeInBytes: Long,
      overWrite: Boolean,
      loadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext,
      partition: Map[String, Option[String]],
      updateModel: Option[UpdateTableModel],
      optionsOriginal: mutable.Map[String, String],
      currPartitions: util.List[PartitionSpec]): LogicalRelation = {
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val metastoreSchema = if (optionsOriginal.contains("no_rearrange_of_rows")) {
      StructType(catalogTable.schema.fields.map{f =>
        val column = table.getColumnByName(f.name)
        val updatedDataType = if (column.getDataType ==
                                  org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
          IntegerType
        } else {
          f.dataType match {
            case TimestampType | DateType =>
              LongType
            // complex type will be converted in CarbonOutputWriter.writeCarbon
            case _ =>
              f.dataType
          }
        }
        f.copy(dataType = updatedDataType)
      })
    } else {
      StructType(catalogTable.schema.fields.map{f =>
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
    }
    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val catalog = new CatalogFileIndex(
      sparkSession, catalogTable, sizeInBytes)
    if (!lazyPruningEnabled) {
      catalog.filterPartitions(Nil) // materialize all the partitions in memory
    }
    var partitionSchema =
      StructType(table.getPartitionInfo.getColumnSchemaList.asScala.map(field =>
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
    if (partition.nonEmpty) {
      val staticPartitionStr = ObjectSerializationUtil.convertObjectToString(
        new util.HashMap[String, Boolean](
          partition.map { case (col, value) => (col.toLowerCase, value.isDefined) }.asJava))
      options += (("staticpartition", staticPartitionStr))
    }
    options ++= optionsOriginal
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

    if (options.contains("no_rearrange_of_rows")) {
      CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
        metastoreSchema.toAttributes,
        Some(catalogTable),
        false)
    } else {
      CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
        hdfsRelation.schema.toAttributes,
        Some(catalogTable),
        false)
    }
  }

  def getConvertedInternalRow(columnSchema: Seq[ColumnSchema],
      rdd: RDD[InternalRow]): RDD[InternalRow] = {
    // Converts the data as per the loading steps before give it to writer or sorter
    var timeStampIndex = scala.collection.mutable.Set[Int]()
    var dateIndex = scala.collection.mutable.Set[Int]()
    var doubleIndex = scala.collection.mutable.Set[Int]()
    var i: Int = 0
    for (col <- columnSchema) {
      if (col.getDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
        timeStampIndex += i
      } else if (col.getDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
        dateIndex += i;
      } else if (col.getDataType ==
                 org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
        doubleIndex += i
      }
      i = i + 1
    }
    val updatedRdd: RDD[InternalRow] = rdd.map { internalRow =>
      for (index <- timeStampIndex) {
        internalRow.setLong(index, internalRow.getLong(index) / 1000)
      }
      var doubleValue: Double = 0
      for (index <- doubleIndex) {
        doubleValue = internalRow.getDouble(index)
        if (doubleValue.isNaN || doubleValue.isInfinite) {
          // converter used to set null for NAN or infinite
          internalRow.setNullAt(index)
        }
      }
      for (index <- dateIndex) {
        if (internalRow.isNullAt(index)) {
          // null values must be replaced with direct dictionary null values
          internalRow.setInt(index, CarbonCommonConstants.DIRECT_DICT_VALUE_NULL)
        } else {
          internalRow.setInt(index,
            internalRow.getInt(index) + DateDirectDictionaryGenerator.cutOffDate)
        }
      }
      internalRow
    }
    updatedRdd
  }

  def getTimeAndDateFormatFromLoadModel(loadModel: CarbonLoadModel): (SimpleDateFormat,
    SimpleDateFormat) = {
    var timeStampformatString = loadModel.getTimestampformat
    if (timeStampformatString.isEmpty) {
      timeStampformatString = loadModel.getDefaultTimestampFormat
    }
    val timeStampFormat = new SimpleDateFormat(timeStampformatString)
    var dateFormatString = loadModel.getDateFormat
    if (dateFormatString.isEmpty) {
      dateFormatString = loadModel.getDefaultDateFormat
    }
    val dateFormat = new SimpleDateFormat(dateFormatString)
    (timeStampFormat, dateFormat)
  }

  def getCompletePartitionValues(partition: Map[String, Option[String]],
      table: CarbonTable): Map[String, Option[String]] = {
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

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitioned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  def loadDataWithPartition(loadParams: CarbonLoadParams): Seq[Row] = {
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val catalogTable: CatalogTable = loadParams.logicalPartitionRelation.catalogTable.get
    // Clean up the already dropped partitioned data
    SegmentFileStore.cleanSegments(table, null, false)
    CarbonUtils.threadSet("partition.operationcontext", loadParams.operationContext)
    val attributes = if (loadParams.scanResultRDD.isDefined) {
      // take the already re-arranged attributes
      catalogTable.schema.toAttributes
    } else {
      // input data from csv files. Convert to logical plan
      val allCols = new ArrayBuffer[String]()
      // get only the visible dimensions from table
      allCols ++= table.getVisibleDimensions.asScala.filterNot(_.isIndexColumn).map(_.getColName)
      allCols ++= table.getVisibleMeasures.asScala.map(_.getColName)
      StructType(
        allCols.filterNot(_.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)).map(
          StructField(_, StringType))).toAttributes
    }
    var partitionsLen = 0
    val sortScope = CarbonDataProcessorUtil.getSortScope(loadParams.carbonLoadModel.getSortScope)
    val partitionValues = if (loadParams.finalPartition.nonEmpty) {
      loadParams.finalPartition.filter(_._2.nonEmpty).map { case (col, value) =>
        catalogTable.schema.find(_.name.equalsIgnoreCase(col)) match {
          case Some(c) =>
            CarbonScalaUtil.convertToDateAndTimeFormats(
              value.get,
              c.dataType,
              loadParams.timeStampFormat,
              loadParams.dateFormat)
          case None =>
            throw new AnalysisException(s"$col is not a valid partition column in table ${
              loadParams.carbonLoadModel
                .getDatabaseName
            }.${ loadParams.carbonLoadModel.getTableName }")
        }
      }.toArray
    } else {
      Array[String]()
    }
    var persistedRDD: Option[RDD[InternalRow]] = None
    try {
      val query: LogicalPlan = if ((loadParams.dataFrame.isDefined) ||
                                   loadParams.scanResultRDD.isDefined) {
        val (rdd, dfAttributes) = if (loadParams.updateModel.isDefined) {
          // Get the updated query plan in case of update scenario
          val updatedFrame = Dataset.ofRows(
            loadParams.sparkSession,
            getLogicalQueryForUpdate(
              loadParams.sparkSession,
              catalogTable,
              loadParams.dataFrame.get,
              loadParams.carbonLoadModel))
          (updatedFrame.rdd, updatedFrame.schema)
        } else {
          if (loadParams.finalPartition.nonEmpty) {
            val headers = loadParams.carbonLoadModel
              .getCsvHeaderColumns
              .dropRight(loadParams.finalPartition.size)
            val updatedHeader = headers ++ loadParams.finalPartition.keys.map(_.toLowerCase)
            loadParams.carbonLoadModel.setCsvHeader(updatedHeader.mkString(","))
            loadParams.carbonLoadModel
              .setCsvHeaderColumns(loadParams.carbonLoadModel.getCsvHeader.split(","))
          }
          if (loadParams.dataFrame.isDefined) {
            (loadParams.dataFrame.get.rdd, loadParams.dataFrame.get.schema)
          } else {
            (null, null)
          }
        }
        if (loadParams.dataFrame.isDefined) {
          val expectedColumns = {
            val staticPartCols = loadParams.finalPartition.filter(_._2.isDefined).keySet
              .map(columnName => columnName.toLowerCase())
            attributes.filterNot(a => staticPartCols.contains(a.name.toLowerCase))
          }
          if (expectedColumns.length != dfAttributes.length) {
            throw new AnalysisException(
              s"Cannot insert into table $loadParams.tableName because the number of columns are " +
              s"different: " +
              s"need ${ expectedColumns.length } columns, " +
              s"but query has ${ dfAttributes.length } columns.")
          }
          val nonPartitionBounds = expectedColumns.zipWithIndex.map(_._2).toArray
          val partitionBounds = new Array[Int](partitionValues.length)
          if (loadParams.finalPartition.nonEmpty) {
            val nonPartitionSchemaLen = attributes.length - loadParams.finalPartition.size
            var i = nonPartitionSchemaLen
            var index = 0
            var partIndex = 0
            loadParams.finalPartition.values.foreach { p =>
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
            transformQueryWithRow(
              transRdd,
              loadParams.sparkSession,
              loadParams.carbonLoadModel,
              partitionValues,
              catalogTable,
              attributes,
              sortScope,
              isDataFrame = true, table, loadParams.finalPartition)
          partitionsLen = partitions
          persistedRDD = persistedRDDLocal
          transformedPlan
        } else {
          val (transformedPlan, partitions, persistedRDDLocal) =
            CommonLoadUtils.transformQueryWithInternalRow(
              loadParams.scanResultRDD.get,
              loadParams.sparkSession,
              loadParams.carbonLoadModel,
              partitionValues,
              catalogTable,
              attributes,
              sortScope,
              table,
              loadParams.finalPartition)
          partitionsLen = partitions
          persistedRDD = persistedRDDLocal
          transformedPlan
        }
      } else {
        val columnCount = loadParams.carbonLoadModel.getCsvHeaderColumns.length
        val rdd = CsvRDDHelper.csvFileScanRDD(
          loadParams.sparkSession,
          model = loadParams.carbonLoadModel,
          loadParams.hadoopConf).map(DataLoadProcessorStepOnSpark.toStringArrayRow(_, columnCount))
        val (transformedPlan, partitions, persistedRDDLocal) =
          transformQueryWithRow(
            rdd.asInstanceOf[RDD[Row]],
            loadParams.sparkSession,
            loadParams.carbonLoadModel,
            partitionValues,
            catalogTable,
            attributes,
            sortScope,
            isDataFrame = false,
            table,
            loadParams.finalPartition)
        partitionsLen = partitions
        persistedRDD = persistedRDDLocal
        transformedPlan
      }
      if (loadParams.updateModel.isDefined) {
        loadParams.carbonLoadModel.setFactTimeStamp(loadParams.updateModel.get.updatedTimeStamp)
      } else if (loadParams.carbonLoadModel.getFactTimeStamp == 0L) {
        loadParams.carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      }
      val opt = collection.mutable.Map() ++ loadParams.optionsOriginal
      if (loadParams.scanResultRDD.isDefined) {
        opt += (("no_rearrange_of_rows", "true"))
      }
      // Create and ddd the segment to the tablestatus.
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadParams.carbonLoadModel,
        loadParams.isOverwriteTable)
      val convertRelation = convertToLogicalRelation(
        catalogTable,
        loadParams.sizeInBytes,
        loadParams.isOverwriteTable,
        loadParams.carbonLoadModel,
        loadParams.sparkSession,
        loadParams.operationContext,
        loadParams.finalPartition,
        loadParams.updateModel,
        opt,
        loadParams.currPartitions)
      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = loadParams.finalPartition,
          query = query,
          overwrite = false,
          ifPartitionNotExists = false)
      SparkUtil.setNullExecutionId(loadParams.sparkSession)
      Dataset.ofRows(loadParams.sparkSession, convertedPlan)
    } catch {
      case ex: Throwable =>
        val (executorMessage, errorMessage) = CarbonScalaUtil.retrieveAndLogErrorMsg(ex, LOGGER)
        if (loadParams.updateModel.isDefined) {
          CarbonScalaUtil.updateErrorInUpdateModel(loadParams.updateModel.get, executorMessage)
        }
        LOGGER.info(errorMessage)
        LOGGER.error(ex)
        throw ex
    } finally {
      CarbonUtils.threadUnset("partition.operationcontext")
      if (loadParams.isOverwriteTable) {
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
      if (!StringUtils.isEmpty(loadParams.carbonLoadModel.getSegmentId)) {
        DistributedRDDUtils.triggerPrepriming(loadParams.sparkSession,
          table,
          Seq(),
          loadParams.operationContext,
          loadParams.hadoopConf,
          List(loadParams.carbonLoadModel.getSegmentId))
      }
    }
    try {
      val compactedSegments = new util.ArrayList[String]()
      // Trigger auto compaction
      CarbonDataRDDFactory.handleSegmentMerging(
        loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel
          .getCopyWithPartition(loadParams.carbonLoadModel.getCsvHeader,
            loadParams.carbonLoadModel.getCsvDelimiter),
        table,
        compactedSegments,
        loadParams.operationContext)
      loadParams.carbonLoadModel.setMergedSegmentIds(compactedSegments)
    } catch {
      case e: Exception =>
        LOGGER.error(
          "Auto-Compaction has failed. Ignoring this exception because the " +
          "load is passed.", e)
    }
    val specs =
      SegmentFileStore.getPartitionSpecs(loadParams.carbonLoadModel.getSegmentId,
        loadParams.carbonLoadModel.getTablePath,
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(table.getTablePath)))
    if (specs != null) {
      specs.asScala.map { spec =>
        Row(spec.getPartitions.asScala.mkString("/"), spec.getLocation.toString, spec.getUuid)
      }
    } else {
      Seq.empty[Row]
    }
  }

}
