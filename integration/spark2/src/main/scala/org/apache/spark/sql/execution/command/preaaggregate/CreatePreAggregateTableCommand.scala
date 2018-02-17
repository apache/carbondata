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
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.AggregationDataMapSchema
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}

/**
 * Below command class will be used to create pre-aggregate table
 * and updating the parent table about the child table information
 * It will be either success or nothing happen in case of failure:
 * 1. failed to create pre aggregate table.
 * 2. failed to update main table
 *
 */
case class CreatePreAggregateTableCommand(
    dataMapName: String,
    parentTableIdentifier: TableIdentifier,
    dmClassName: String,
    dmProperties: Map[String, String],
    queryString: String,
    timeSeriesFunction: Option[String] = None)
  extends AtomicRunnableCommand {

  var parentTable: CarbonTable = _
  var loadCommand: CarbonLoadDataCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val df = sparkSession.sql(updatedQuery)
    val fieldRelationMap = PreAggregateUtil.validateActualSelectPlanAndGetAttributes(
      df.logicalPlan, queryString)
    val fields = fieldRelationMap.keySet.toSeq
    val tableProperties = mutable.Map[String, String]()
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))

    parentTable = PreAggregateUtil.getParentCarbonTable(df.logicalPlan)
    assert(parentTable.getTableName.equalsIgnoreCase(parentTableIdentifier.table),
      "Parent table name is different in select and create")
    var neworder = Seq[String]()
    val parentOrder = parentTable.getSortColumns(parentTable.getTableName).asScala
    parentOrder.foreach(parentcol =>
      fields.filter(col => (fieldRelationMap.get(col).get.aggregateFunction.isEmpty) &&
                           (parentcol.equals(fieldRelationMap.get(col).get.
                             columnTableRelationList.get(0).parentColumnName)))
        .map(cols => neworder :+= cols.column)
    )
    tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, neworder.mkString(","))
    tableProperties.put("sort_scope", parentTable.getTableInfo.getFactTable.
      getTableProperties.getOrDefault("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT))
    tableProperties
      .put(CarbonCommonConstants.TABLE_BLOCKSIZE, parentTable.getBlockSizeInMB.toString)
    val tableIdentifier =
      TableIdentifier(parentTableIdentifier.table + "_" + dataMapName,
        parentTableIdentifier.database)
    // prepare table model of the collected tokens
    val tableModel: TableModel = new CarbonSpark2SqlParser().prepareTableModel(
      ifNotExistPresent = false,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      Seq(),
      tableProperties,
      None,
      isAlterFlow = false,
      None)


    // updating the relation identifier, this will be stored in child table
    // which can be used during dropping of pre-aggreate table as parent table will
    // also get updated
    if(timeSeriesFunction.isDefined) {
      TimeSeriesUtil.validateTimeSeriesEventTime(dmProperties, parentTable)
      TimeSeriesUtil.validateEventTimeColumnExitsInSelect(
        fieldRelationMap,
        dmProperties.get(TimeSeriesUtil.TIMESERIES_EVENTTIME).get)
      TimeSeriesUtil.updateTimeColumnSelect(fieldRelationMap,
        dmProperties.get(TimeSeriesUtil.TIMESERIES_EVENTTIME).get,
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
      tableModel.ifNotExistsSet, Some(tablePath)).run(sparkSession)

    val table = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    val tableInfo = table.getTableInfo
    // child schema object which will be updated on parent table about the
    val childSchema = tableInfo.getFactTable.buildChildSchema(
      dataMapName,
      CarbonCommonConstants.AGGREGATIONDATAMAPSCHEMA,
      tableInfo.getDatabaseName,
      queryString,
      "AGGREGATION")
    dmProperties.foreach(f => childSchema.getProperties.put(f._1, f._2))

    // updating the parent table about child table
    PreAggregateUtil.updateMainTable(
      parentTable,
      childSchema,
      sparkSession)
    // After updating the parent carbon table with data map entry extract the latest table object
    // to be used in further create process.
    parentTable = CarbonEnv.getCarbonTable(parentTableIdentifier.database,
      parentTableIdentifier.table)(sparkSession)
    val updatedLoadQuery = if (timeSeriesFunction.isDefined) {
      val dataMap = parentTable.getTableInfo.getDataMapSchemaList.asScala
        .filter(p => p.getDataMapName
          .equalsIgnoreCase(dataMapName)).head
        .asInstanceOf[AggregationDataMapSchema]
      PreAggregateUtil.createTimeSeriesSelectQueryFromMain(dataMap.getChildSchema,
        parentTable.getTableName,
        parentTable.getDatabaseName)
    } else {
      queryString
    }
    val dataFrame = sparkSession.sql(new CarbonSpark2SqlParser().addPreAggLoadFunction(
      updatedLoadQuery)).drop("preAggLoad")
    val dataMap = parentTable.getTableInfo.getDataMapSchemaList.asScala
      .filter(dataMap => dataMap.getDataMapName.equalsIgnoreCase(dataMapName)).head
      .asInstanceOf[AggregationDataMapSchema]
    loadCommand = PreAggregateUtil.createLoadCommandForChild(
      dataMap.getChildSchema.getListOfColumns,
      tableIdentifier,
      dataFrame,
      false,
      sparkSession = sparkSession)
    loadCommand.processMetadata(sparkSession)
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    // drop child table and undo the change in table info of main table
    CarbonDropDataMapCommand(
      dataMapName,
      ifExistsSet = true,
      parentTableIdentifier.database,
      parentTableIdentifier.table).run(sparkSession)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // load child table if parent table has existing segments
    // This will be used to check if the parent table has any segments or not. If not then no
    // need to fire load for pre-aggregate table. Therefore reading the load details for PARENT
    // table.
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(parentTable, false)
    val loadAvailable = SegmentStatusManager.readLoadMetadata(parentTable.getMetadataPath)
    if (loadAvailable.exists(load => load.getSegmentStatus == SegmentStatus.INSERT_IN_PROGRESS ||
      load.getSegmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS)) {
      throw new UnsupportedOperationException(
        "Cannot create pre-aggregate table when insert is in progress on main table")
    } else if (loadAvailable.nonEmpty) {
      val updatedQuery = if (timeSeriesFunction.isDefined) {
        val dataMap = parentTable.getTableInfo.getDataMapSchemaList.asScala
          .filter(p => p.getDataMapName
            .equalsIgnoreCase(dataMapName)).head
          .asInstanceOf[AggregationDataMapSchema]
        PreAggregateUtil.createTimeSeriesSelectQueryFromMain(dataMap.getChildSchema,
          parentTable.getTableName,
          parentTable.getDatabaseName)
      } else {
        queryString
      }
      // Passing segmentToLoad as * because we want to load all the segments into the
      // pre-aggregate table even if the user has set some segments on the parent table.
      loadCommand.dataFrame = Some(PreAggregateUtil
        .getDataFrame(sparkSession, loadCommand.logicalPlan.get))
      PreAggregateUtil.startDataLoadForDataMap(
        parentTable,
        segmentToLoad = "*",
        validateSegments = true,
        sparkSession,
        loadCommand)
    }
    Seq.empty
  }
}


