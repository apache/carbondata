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

package org.apache.spark.sql.execution.command.preaaggregate

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.datamap.CarbonDropDataMapCommand
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentManager, SegmentStatus, SegmentStatusManager}

/**
 * Below helper class will be used to create pre-aggregate table
 * and updating the parent table about the child table information
 * It will be either success or nothing happen in case of failure:
 * 1. failed to create pre aggregate table.
 * 2. failed to update main table
 *
 */
case class PreAggregateTableHelper(
    var parentTable: CarbonTable,
    dataMapName: String,
    dataMapClassName: String,
    dataMapProperties: java.util.Map[String, String],
    queryString: String,
    timeSeriesFunction: Option[String] = None,
    ifNotExistsSet: Boolean = false) {

  var loadCommand: CarbonLoadDataCommand = _

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def initMeta(sparkSession: SparkSession): Seq[Row] = {
    val dmProperties = dataMapProperties.asScala
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val df = sparkSession.sql(updatedQuery)
    val fieldRelationMap = PreAggregateUtil.validateActualSelectPlanAndGetAttributes(
      df.logicalPlan, queryString)

    val partitionInfo = parentTable.getPartitionInfo
    val fields = fieldRelationMap.keySet.toSeq
    val tableProperties = mutable.Map[String, String]()
    val usePartitioning = dataMapProperties.getOrDefault("partitioning", "true").toBoolean
    val parentPartitionColumns = if (!usePartitioning) {
      Seq.empty
    } else if (parentTable.isHivePartitionTable) {
      partitionInfo.getColumnSchemaList.asScala.map(_.getColumnName)
    } else {
      Seq()
    }
    // Generate child table partition columns in the same order as the parent table.
    val partitionerFields =
      PartitionUtils.getPartitionerFields(parentPartitionColumns, fieldRelationMap)

    dmProperties.foreach(t => tableProperties.put(t._1, t._2))

    val selectTable = PreAggregateUtil.getParentCarbonTable(df.logicalPlan)
    if (!parentTable.getTableName.equalsIgnoreCase(selectTable.getTableName)) {
      throw new MalformedDataMapCommandException(
        "Parent table name is different in select and create")
    }
    var neworder = Seq[String]()
    val parentOrder = parentTable.getSortColumns(parentTable.getTableName).asScala
    parentOrder.foreach(parentcol =>
      fields.filter(col => fieldRelationMap(col).aggregateFunction.isEmpty &&
                           parentcol.equals(fieldRelationMap(col).
                             columnTableRelationList.get(0).parentColumnName))
        .map(cols => neworder :+= cols.column))
    tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, neworder.mkString(","))
    tableProperties.put("sort_scope", parentTable.getTableInfo.getFactTable.
      getTableProperties.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT))
    tableProperties
      .put(CarbonCommonConstants.TABLE_BLOCKSIZE, parentTable.getBlockSizeInMB.toString)
    tableProperties.put(CarbonCommonConstants.FLAT_FOLDER,
      parentTable.getTableInfo.getFactTable.getTableProperties.asScala.getOrElse(
        CarbonCommonConstants.FLAT_FOLDER, CarbonCommonConstants.DEFAULT_FLAT_FOLDER))

    // Datamap table name and columns are automatically added prefix with parent table name
    // in carbon. For convenient, users can type column names same as the ones in select statement
    // when config dmproperties, and here we update column names with prefix.
    val longStringColumn = tableProperties.get(CarbonCommonConstants.LONG_STRING_COLUMNS)
    if (longStringColumn != None) {
      val fieldNames = fields.map(_.column)
      val newLongStringColumn = longStringColumn.get.split(",").map(_.trim).map{ colName =>
        val newColName = parentTable.getTableName.toLowerCase() + "_" + colName
        if (!fieldNames.contains(newColName)) {
          throw new MalformedDataMapCommandException(
            CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase() + ":" + colName
              + " does not in datamap")
        }
        newColName
      }
      tableProperties.put(CarbonCommonConstants.LONG_STRING_COLUMNS,
        newLongStringColumn.mkString(","))
    }

    // inherit the local dictionary properties of main parent table
    tableProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
        parentTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE, "false"))
    tableProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
        parentTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT))
    val parentDictInclude = parentTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, "").split(",")

    val parentDictExclude = parentTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, "").split(",")

    val newDictInclude =
      parentDictInclude.flatMap(parentcol =>
        fields.collect {
          case col if fieldRelationMap(col).aggregateFunction.isEmpty &&
                      parentcol.equals(fieldRelationMap(col).
                        columnTableRelationList.get.head.parentColumnName) =>
            col.column
        })

    val newDictExclude = parentDictExclude.flatMap(parentcol =>
      fields.collect {
        case col if fieldRelationMap(col).aggregateFunction.isEmpty &&
                    parentcol.equals(fieldRelationMap(col).
                      columnTableRelationList.get.head.parentColumnName) =>
          col.column
      })
    if (newDictInclude.nonEmpty) {
      tableProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, newDictInclude.mkString(","))
    }
    if (newDictExclude.nonEmpty) {
      tableProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, newDictExclude.mkString(","))
    }
    val tableIdentifier =
      TableIdentifier(parentTable.getTableName + "_" + dataMapName,
        Some(parentTable.getDatabaseName))
    // prepare table model of the collected tokens
    val tableModel: TableModel = new CarbonSpark2SqlParser().prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      partitionerFields,
      tableProperties,
      None,
      isAlterFlow = false,
      true,
      None)

    // updating the relation identifier, this will be stored in child table
    // which can be used during dropping of pre-aggreate table as parent table will
    // also get updated
    if(timeSeriesFunction != null) {
      TimeSeriesUtil.validateTimeSeriesEventTime(dmProperties.toMap, parentTable)
      TimeSeriesUtil.validateEventTimeColumnExitsInSelect(
        fieldRelationMap,
        dmProperties(TimeSeriesUtil.TIMESERIES_EVENTTIME))
      TimeSeriesUtil.updateTimeColumnSelect(
        fieldRelationMap,
        dmProperties(TimeSeriesUtil.TIMESERIES_EVENTTIME),
        timeSeriesFunction.get)
    }
    tableModel.parentTable = Some(parentTable)
    tableModel.dataMapRelation = Some(fieldRelationMap)
    val tablePath = if (dmProperties.contains("path")) {
      dmProperties("path")
    } else {
      CarbonEnv.getTablePath(tableModel.databaseNameOp, tableModel.tableName)(sparkSession)
    }
    CarbonCreateTableCommand(TableNewProcessor(tableModel),
      tableModel.ifNotExistsSet, Some(tablePath), isVisible = false).run(sparkSession)

    val table = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    val tableInfo = table.getTableInfo

    // child schema object will be saved on parent table schema
    val childSchema = tableInfo.getFactTable.buildChildSchema(
      dataMapName,
      dataMapClassName,
      tableInfo.getDatabaseName,
      queryString,
      "AGGREGATION")
    dmProperties.foreach(f => childSchema.getProperties.put(f._1, f._2))

    try {
      // updating the parent table about child table
      PreAggregateUtil.updateMainTable(parentTable, childSchema, sparkSession)
    } catch {
      case e: MetadataProcessException =>
        throw e
      case ex: Exception =>
        // If updation failed then forcefully remove datamap from metastore.
        val dropTableCommand = CarbonDropDataMapCommand(childSchema.getDataMapName,
          ifExistsSet = true,
          Some(TableIdentifier
            .apply(parentTable.getTableName, Some(parentTable.getDatabaseName))),
          forceDrop = true)
        dropTableCommand.processMetadata(sparkSession)
        throw ex
    }
    // After updating the parent carbon table with data map entry extract the latest table object
    // to be used in further create process.
    parentTable = CarbonEnv.getCarbonTable(Some(parentTable.getDatabaseName),
      parentTable.getTableName)(sparkSession)

    val updatedLoadQuery = if (timeSeriesFunction != null) {
      PreAggregateUtil.createTimeSeriesSelectQueryFromMain(
        childSchema.getChildSchema,
        parentTable.getTableName,
        parentTable.getDatabaseName)
    } else {
      queryString
    }
    val dataFrame = sparkSession.sql(new CarbonSpark2SqlParser().addPreAggLoadFunction(
      updatedLoadQuery)).drop("preAggLoad")
    loadCommand = PreAggregateUtil.createLoadCommandForChild(
      childSchema.getChildSchema.getListOfColumns,
      tableIdentifier,
      dataFrame,
      isOverwrite = false,
      sparkSession = sparkSession)
    loadCommand.processMetadata(sparkSession)
    Seq.empty
  }

  def initData(sparkSession: SparkSession): Seq[Row] = {
    // load child table if parent table has existing segments
    // This will be used to check if the parent table has any segments or not. If not then no
    // need to fire load for pre-aggregate table. Therefore reading the load details for PARENT
    // table.
    new SegmentManager().deleteLoadsAndUpdateMetadata(
      parentTable,
      false,
      CarbonFilters.getCurrentPartitions(
        sparkSession,
        TableIdentifier(parentTable.getTableName,
          Some(parentTable.getDatabaseName))
      ).map(_.asJava).orNull)

    if (SegmentStatusManager.isLoadInProgressInTable(parentTable)) {
      throw new UnsupportedOperationException(
        "Cannot create pre-aggregate table when insert is in progress on parent table")
    }
    // check if any segment if available for load in the parent table
    val loadAvailable =
      new SegmentManager().getValidSegments(
        parentTable.getAbsoluteTableIdentifier).getValidSegments.asScala.map(_.toString)
    if (loadAvailable.nonEmpty) {
      // Passing segmentToLoad as * because we want to load all the segments into the
      // pre-aggregate table even if the user has set some segments on the parent table.
      loadCommand.dataFrame = Some(PreAggregateUtil
        .getDataFrame(sparkSession, loadCommand.logicalPlan.get))
      PreAggregateUtil.startDataLoadForDataMap(
        TableIdentifier(parentTable.getTableName, Some(parentTable.getDatabaseName)),
        segmentToLoad = loadAvailable.mkString(","),
        validateSegments = false,
        loadCommand,
        isOverwrite = false,
        sparkSession)
    } else {
      LOGGER.info(s"No segment available for load in table ${parentTable.getTableUniqueName}")
    }
    Seq.empty
  }
}


