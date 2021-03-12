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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonToSparkAdapter, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, UpdateTableModel}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.CausedBy

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

/*
* insert into without df, by just using logical plan
*
*/
case class CarbonInsertIntoCommand(databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String],
    isOverwriteTable: Boolean,
    var logicalPlan: LogicalPlan,
    var tableInfo: TableInfo,
    var internalOptions: Map[String, String] = Map.empty,
    var partition: Map[String, Option[String]] = Map.empty,
    var operationContext: OperationContext = new OperationContext,
    var updateModel: Option[UpdateTableModel] = None)
  extends AtomicRunnableCommand {

  var table: CarbonTable = _

  var logicalPartitionRelation: LogicalRelation = _

  var sizeInBytes: Long = _

  var currPartitions: util.List[PartitionSpec] = _

  var parentTablePath: String = _

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  var scanResultRdd: RDD[InternalRow] = _

  var timeStampFormat: SimpleDateFormat = _

  var dateFormat: SimpleDateFormat = _

  var finalPartition: Map[String, Option[String]] = Map.empty

  var isInsertIntoWithConverterFlow: Boolean = false

  var dataFrame: DataFrame = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (tableInfo == null) {
      throw new RuntimeException(" table info must be present when logical relation exist")
    }
    // If logical plan is unresolved, need to convert it to resolved.
    dataFrame = Dataset.ofRows(sparkSession, logicalPlan)
    logicalPlan = dataFrame.queryExecution.analyzed
    if (CarbonProperties.isBadRecordHandlingEnabledForInsert) {
      // use old converter flow
      isInsertIntoWithConverterFlow = true
      return Seq.empty
    }
    setAuditTable(CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession), tableName)
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    val (sizeInBytes, table, dbName, logicalPartitionRelation, finalPartition) = CommonLoadUtils
      .processMetadataCommon(
        sparkSession,
        databaseNameOp,
        tableName,
        Some(tableInfo),
        partition)
    this.sizeInBytes = sizeInBytes
    this.table = table
    this.logicalPartitionRelation = logicalPartitionRelation
    this.finalPartition = finalPartition
    setAuditTable(dbName, tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (isInsertIntoWithConverterFlow) {
      return CarbonInsertIntoWithDf(
        databaseNameOp = databaseNameOp,
        tableName = tableName,
        options = options,
        isOverwriteTable = isOverwriteTable,
        dataFrame = dataFrame,
        updateModel = None,
        tableInfoOp = Some(tableInfo),
        internalOptions = internalOptions,
        partition = partition).process(sparkSession)
    }
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val factPath = ""
    currPartitions = CommonLoadUtils.getCurrentPartitions(sparkSession, table)
    CommonLoadUtils.setNumberOfCoresWhileLoading(sparkSession)
    val optionsFinal: util.Map[String, String] = CommonLoadUtils.getFinalLoadOptions(table, options)
    val carbonLoadModel: CarbonLoadModel = CommonLoadUtils.prepareLoadModel(
      hadoopConf = hadoopConf,
      factPath = factPath,
      optionsFinal = optionsFinal,
      parentTablePath = parentTablePath,
      table = table,
      isDataFrame = true,
      internalOptions = internalOptions,
      partition = partition,
      options = options)
    val (tf, df) = CommonLoadUtils.getTimeAndDateFormatFromLoadModel(carbonLoadModel)
    timeStampFormat = tf
    dateFormat = df
    val partitionInfo = tableInfo.getFactTable.getPartitionInfo
    val partitionColumnSchema =
      if (partitionInfo != null && partitionInfo.getColumnSchemaList.size() != 0) {
        partitionInfo.getColumnSchemaList.asScala
      } else {
        null
      }
    val convertedStaticPartition = getConvertedStaticPartitionMap(partitionColumnSchema)
    val (reArrangedIndex, reArrangedMVIndex, selectedColumnSchema) =
      getReArrangedIndexAndSelectedSchema(
      tableInfo,
      partitionColumnSchema,
      carbonLoadModel)
    val newLogicalPlan = getReArrangedLogicalPlan(
      reArrangedIndex,
      selectedColumnSchema,
      convertedStaticPartition)
    scanResultRdd = sparkSession.sessionState.executePlan(newLogicalPlan).toRdd
    if (logicalPartitionRelation != null) {
      if (selectedColumnSchema.length != logicalPartitionRelation.output.length) {
        throw new RuntimeException(" schema length doesn't match partition length")
      }
      val isNotReArranged = selectedColumnSchema.zipWithIndex.exists {
        case (cs, i) => !cs.getColumnName.equals(logicalPartitionRelation.output(i).name)
      }
      if (isNotReArranged) {
        // Re-arrange the catalog table schema and output for partition relation
        logicalPartitionRelation =
          if (carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isMV) {
            // Re-arrange non-partition columns in the catalog table schema based on rearranged
            // mv index order. Example: MV columns: c1,c2(partition_column),c3(sort_column),c4.
            // Based on this order, rearranged index will be like (2,0,3,1). Catalog table schema
            // order will be (c1,c3,c4,c2) where the partition column will be always at last. If we
            // rearrange the logical relation based on above order, catalog table schema will be
            // changed to (c4,c1,c2,c3), which will be wrong. Hence, Reorder MV create column
            // order to (c1,c3,c4,c2) and use rearranged mv index (1,0,2,3) to rearrange
            // logical relation schema.
            getReArrangedSchemaLogicalRelation(reArrangedMVIndex, logicalPartitionRelation)
          } else {
            getReArrangedSchemaLogicalRelation(reArrangedIndex, logicalPartitionRelation)
          }
      }
    }
    var isUpdateTableStatusRequired = false
    val uuid = ""
    var loadResultForReturn: LoadMetadataDetails = null
    var rowsForReturn: Seq[Row] = Seq.empty
    try {
      val (tableIndexes, indexOperationContext) =
        CommonLoadUtils.firePreLoadEvents(
          sparkSession = sparkSession,
          carbonLoadModel = carbonLoadModel,
          uuid = uuid,
          factPath = factPath,
          optionsFinal = optionsFinal,
          options = options.asJava,
          isOverwriteTable = isOverwriteTable,
          isDataFrame = true,
          updateModel = updateModel,
          operationContext = operationContext)

      // add the start entry for the new load in the table status file
      if ((updateModel.isEmpty || updateModel.isDefined)
          && !table.isHivePartitionTable) {
        if (updateModel.isDefined) {
          carbonLoadModel.setFactTimeStamp(updateModel.get.updatedTimeStamp)
        }
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(
          carbonLoadModel,
          isOverwriteTable)
        isUpdateTableStatusRequired = true
      } else if (!table.isHivePartitionTable) {
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
      val loadParams = CarbonLoadParams(sparkSession,
        tableName,
        sizeInBytes,
        isOverwriteTable,
        carbonLoadModel,
        hadoopConf,
        logicalPartitionRelation,
        dateFormat,
        timeStampFormat,
        options,
        finalPartition,
        currPartitions,
        partitionStatus,
        None,
        Some(scanResultRdd),
        updateModel,
        operationContext)
      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      val (rows, loadResult) = insertData(loadParams)
      loadResultForReturn = loadResult
      rowsForReturn = rows
      val info = CommonLoadUtils.makeAuditInfo(loadResult)
      setAuditInfo(info)
      CommonLoadUtils.firePostLoadEvents(sparkSession,
        carbonLoadModel,
        tableIndexes,
        indexOperationContext,
        table,
        operationContext)
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
    if (loadResultForReturn != null && loadResultForReturn.getLoadName != null) {
      Seq(Row(loadResultForReturn.getLoadName))
    } else {
      // return the segment id in partition table case
      rowsForReturn
    }
  }

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Segment ID", StringType, nullable = false)())
  }

  private def isAlteredSchema(tableSchema: TableSchema): Boolean = {
    if (tableInfo.getFactTable.getSchemaEvolution != null) {
      tableInfo
        .getFactTable
        .getSchemaEvolution
        .getSchemaEvolutionEntryList.asScala.exists(entry =>
        (entry.getAdded != null && entry.getAdded.size() > 0) ||
        (entry.getRemoved != null && entry.getRemoved.size() > 0)
      )
    } else {
      false
    }
  }

  def getReArrangedLogicalPlan(
      reArrangedIndex: Seq[Int],
      selectedColumnSchema: Seq[ColumnSchema],
      convertedStaticPartition: mutable.Map[String, AnyRef]): LogicalPlan = {
    var processedProject: Boolean = false
    // check first node is the projection or not
    logicalPlan match {
      case _: Project =>
      // project is already present as first node
      case _ =>
        // If project is not present, add the projection to re-arrange it
        logicalPlan = Project(logicalPlan.output, logicalPlan)
    }
    // Re-arrange the project as per columnSchema
    val newLogicalPlan = logicalPlan.transformDown {
      case p: Project if !processedProject =>
        var oldProjectionList = p.projectList
        if (partition.nonEmpty) {
          // partition keyword is present in insert and
          // select query partition projections may not be same as create order.
          // So, bring to create table order
          val dynamicPartition = partition.filterNot(entry => entry._2.isDefined)
          var index = 0
          val map = mutable.Map[String, Int]()
          for (part <- dynamicPartition) {
            map(part._1) = index
            index = index + 1
          }
          var tempList = oldProjectionList.take(oldProjectionList.size - dynamicPartition.size)
          val partitionList = oldProjectionList.takeRight(dynamicPartition.size)
          val partitionSchema = table.getPartitionInfo.getColumnSchemaList.asScala
          for (partitionCol <- partitionSchema) {
            if (map.get(partitionCol.getColumnName).isDefined) {
              tempList = tempList :+ partitionList(map(partitionCol.getColumnName))
            }
          }
          oldProjectionList = tempList
        }
        if (reArrangedIndex.size != oldProjectionList.size) {
          // for non-partition table columns must match
          if (partition.isEmpty) {
            throw new AnalysisException(
              s"Cannot insert into table $tableName because the number of columns are " +
              s"different: " +
              s"need ${ reArrangedIndex.size } columns, " +
              s"but query has ${ oldProjectionList.size } columns.")
          } else {
            if (reArrangedIndex.size - oldProjectionList.size !=
                convertedStaticPartition.size) {
              throw new AnalysisException(
                s"Cannot insert into table $tableName because the number of columns are " +
                s"different: need ${ reArrangedIndex.size } columns, " +
                s"but query has ${ oldProjectionList.size } columns.")
            } else {
              // TODO: For partition case, remaining projections need to validate ?
            }
          }
        }
        var newProjectionList: Seq[NamedExpression] = Seq.empty
        var i = 0
        while (i < reArrangedIndex.size) {
          // column schema is already has sortColumns-dimensions-measures. Collect the ordinal &
          // re-arrange the projection in the same order
          if (partition.nonEmpty &&
              convertedStaticPartition.contains(selectedColumnSchema(i).getColumnName
                .toLowerCase())) {
            // If column schema present in partitionSchema means it is a static partition,
            // then add a value literal expression in the project.
            val value = convertedStaticPartition(selectedColumnSchema(i).getColumnName
              .toLowerCase())
            newProjectionList = newProjectionList :+
                                CarbonToSparkAdapter.createAliasRef(
                                  new Literal(value,
                                    SparkDataTypeConverterImpl.convertCarbonToSparkDataType(
                                      selectedColumnSchema(i).getDataType)),
                                  value.toString,
                                  NamedExpression.newExprId).asInstanceOf[NamedExpression]
          } else {
            // If column schema NOT present in partition column,
            // get projection column mapping its ordinal.
            if (partition.contains(selectedColumnSchema(i).getColumnName.toLowerCase())) {
              // static partition + dynamic partition case,
              // here dynamic partition ordinal will be more than projection size
              newProjectionList = newProjectionList :+
                                  oldProjectionList(
                                    reArrangedIndex(i) - convertedStaticPartition.size)
            } else {
              newProjectionList = newProjectionList :+
                                  oldProjectionList(reArrangedIndex(i))
            }
          }
          i = i + 1
        }
        processedProject = true
        Project(newProjectionList, p.child)
    }
    newLogicalPlan
  }

  private def getConvertedStaticPartitionMap(partitionColumnSchema: mutable.Buffer[ColumnSchema])
  : mutable.Map[String, AnyRef] = {
    val convertedStaticPartition = mutable.Map[String, AnyRef]()
    // setting a converter will remove the thread local entries of previous load configurations.
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)

    if (partition.nonEmpty) {
      for (col <- partitionColumnSchema) {
        if (partition(col.getColumnName.toLowerCase).isDefined) {
          convertedStaticPartition(col.getColumnName.toLowerCase) =
            CarbonScalaUtil.convertStaticPartitionToValues(
              partition(col.getColumnName.toLowerCase).get,
              SparkDataTypeConverterImpl.convertCarbonToSparkDataType(col.getDataType),
              timeStampFormat,
              dateFormat)
        }
      }
    }
    convertedStaticPartition
  }

  def getReArrangedSchemaLogicalRelation(reArrangedIndex: Seq[Int],
      logicalRelation: LogicalRelation): LogicalRelation = {
    // rearrange the schema in catalog table and output attributes of logical relation
    if (reArrangedIndex.size != logicalRelation.schema.size) {
      throw new AnalysisException(
        s"Cannot insert into table $tableName because the number of columns are different: " +
        s"need ${ reArrangedIndex.size } columns, " +
        s"but query has ${ logicalRelation.schema.size } columns.")
    }
    val reArrangedFields = new Array[StructField](logicalRelation.schema.size)
    val reArrangedAttributes = new Array[AttributeReference](logicalRelation.schema.size)
    val fields = logicalRelation.schema.fields
    val output = logicalRelation.output
    var i = 0
    for (index <- reArrangedIndex) {
      // rearranged schema
      reArrangedFields(i) = fields(index)
      // rearranged output attributes of logical relation
      reArrangedAttributes(i) = output(index)
      i = i + 1
    }
    // update the new schema and output attributes
    val catalogTable = logicalRelation.catalogTable
      .get
      .copy(schema = new StructType(reArrangedFields))
    logicalRelation.copy(logicalRelation.relation,
      reArrangedAttributes,
      Some(catalogTable))
  }

  def insertData(loadParams: CarbonLoadParams): (Seq[Row], LoadMetadataDetails) = {
    var rows = Seq.empty[Row]
    val loadDataFrame = Some(dataFrame)
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadResult : LoadMetadataDetails = null
    if (table.isHivePartitionTable) {
      rows = CommonLoadUtils.loadDataWithPartition(loadParams)
    } else {
      loadResult = CarbonDataRDDFactory.loadCarbonData(loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel,
        loadParams.partitionStatus,
        isOverwriteTable,
        loadParams.hadoopConf,
        loadDataFrame,
        loadParams.scanResultRDD,
        updateModel,
        operationContext)
    }
    (rows, loadResult)
  }

  override protected def opName: String = {
    if (isOverwriteTable) {
      "INSERT OVERWRITE"
    } else {
      "INSERT INTO"
    }
  }

  def getReArrangedIndexAndSelectedSchema(
      tableInfo: TableInfo,
      partitionColumnSchema: mutable.Buffer[ColumnSchema],
      carbonLoadModel: CarbonLoadModel): (Seq[Int], Seq[Int], Seq[ColumnSchema]) = {
    var reArrangedIndex: Seq[Int] = Seq()
    var reArrangedMVIndex: Seq[Int] = Seq()
    var selectedColumnSchema: Seq[ColumnSchema] = Seq()
    var partitionIndex: Seq[Int] = Seq()
    var partitionMVIndex: Seq[Int] = Seq()
    val properties = tableInfo.getFactTable.getTableProperties.asScala
    val spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX)
    // internal order ColumnSchema (non-flat structure)
    val columnSchema = (table.getVisibleDimensions.asScala ++
                        table.getVisibleMeasures.asScala).map(_.getColumnSchema)
    val partitionColumnNames = if (partitionColumnSchema != null) {
      partitionColumnSchema.map(x => x.getColumnName).toSet
    } else {
      null
    }
    var createOrderColumns = table.getCreateOrderColumn.asScala
    var createOrderMVColumns = table.getCreateOrderColumn.asScala
    val createOrderMap = mutable.Map[String, Int]()
    val createOrderMVTableMap = mutable.Map[String, Int]()
    val isMV = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isMV
    if (partitionColumnNames != null && isAlteredSchema(tableInfo.getFactTable)) {
      // For alter table drop/add column scenarios, partition column may not be in the end.
      // Need to keep it in the end.
      createOrderColumns = createOrderColumns.filterNot(col =>
        partitionColumnNames.contains(col.getColumnSchema.getColumnName)) ++
                           createOrderColumns.filter(col =>
                             partitionColumnNames.contains(col.getColumnSchema.getColumnName))
    } else if (partitionColumnNames != null && isMV) {
      // For mv as partition table scenarios, partition column may not be in the end.
      // Need to keep it in the end.
      createOrderMVColumns = createOrderMVColumns.filterNot(col =>
        partitionColumnNames.contains(col.getColumnSchema.getColumnName)) ++
                             createOrderMVColumns.filter(col =>
                               partitionColumnNames.contains(col.getColumnSchema.getColumnName))
    }
    createOrderColumns.zipWithIndex.map {
      case (col, index) => createOrderMap.put(col.getColName, index)
    }
    createOrderMVColumns.zipWithIndex.map {
      case (col, index) => createOrderMVTableMap.put(col.getColName, index)
    }
    columnSchema.foreach {
      col =>
        if (spatialProperty.isDefined &&
            col.getColumnName.equalsIgnoreCase(spatialProperty.get.trim)) {
          carbonLoadModel.setNonSchemaColumnsPresent(true)
        }
        var skipPartitionColumn = false
        if (partitionColumnNames != null &&
            partitionColumnNames.contains(col.getColumnName)) {
          partitionIndex = partitionIndex :+ createOrderMap(col.getColumnName)
          partitionMVIndex = partitionMVIndex :+ createOrderMVTableMap(col.getColumnName)
          skipPartitionColumn = true
        } else {
          reArrangedIndex = reArrangedIndex :+ createOrderMap(col.getColumnName)
          reArrangedMVIndex = reArrangedMVIndex :+ createOrderMVTableMap(col.getColumnName)
        }
        if (!skipPartitionColumn) {
          selectedColumnSchema = selectedColumnSchema :+ col
        }
    }
    if (partitionColumnSchema != null) {
      // keep partition columns in the end
      selectedColumnSchema = selectedColumnSchema ++ partitionColumnSchema
    }
    if (partitionIndex.nonEmpty) {
      // keep partition columns in the end and in the original create order
      reArrangedIndex = reArrangedIndex ++ partitionIndex.sortBy(x => x)
      reArrangedMVIndex = reArrangedMVIndex ++ partitionMVIndex.sortBy(x => x)
    }
    (reArrangedIndex, reArrangedMVIndex, selectedColumnSchema)
  }

}
