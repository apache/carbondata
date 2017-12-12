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

import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.datamap.CarbonDropDataMapCommand
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

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

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val df = sparkSession.sql(updatedQuery)
    val fieldRelationMap = PreAggregateUtil.validateActualSelectPlanAndGetAttributes(
      df.logicalPlan, queryString)
    val fields = fieldRelationMap.keySet.toSeq
    val tableProperties = mutable.Map[String, String]()
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))

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

    val parentTable = PreAggregateUtil.getParentCarbonTable(df.logicalPlan)
    assert(parentTable.getTableName.equalsIgnoreCase(parentTableIdentifier.table),
      "Parent table name is different in select and create")
    // updating the relation identifier, this will be stored in child table
    // which can be used during dropping of pre-aggreate table as parent table will
    // also get updated
    if(timeSeriesFunction.isDefined) {
      TimeSeriesUtil.validateTimeSeriesEventTime(dmProperties, parentTable)
      TimeSeriesUtil.validateEventTimeColumnExitsInSelect(
        fieldRelationMap,
        dmProperties.get(CarbonCommonConstants.TIMESERIES_EVENTTIME).get)
      TimeSeriesUtil.updateTimeColumnSelect(fieldRelationMap,
        dmProperties.get(CarbonCommonConstants.TIMESERIES_EVENTTIME).get,
      timeSeriesFunction.get)
    }
    tableModel.parentTable = Some(parentTable)
    tableModel.dataMapRelation = Some(fieldRelationMap)
    val tablePath =
      CarbonEnv.getTablePath(tableModel.databaseNameOp, tableModel.tableName)(sparkSession)
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
      CarbonEnv.getDatabaseName(parentTableIdentifier.database)(sparkSession),
      parentTableIdentifier.table,
      childSchema,
      sparkSession)

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
    val dbName = CarbonEnv.getDatabaseName(parentTableIdentifier.database)(sparkSession)
    val parentCarbonTable = CarbonEnv.getCarbonTable(Some(dbName),
      parentTableIdentifier.table)(sparkSession)
    // This will be used to check if the parent table has any segments or not. If not then no
    // need to fire load for pre-aggregate table. Therefore reading the load details for PARENT
    // table.
    val loadAvailable = SegmentStatusManager.readLoadMetadata(parentCarbonTable.getMetaDataFilepath)
      .nonEmpty
    if (loadAvailable) {
      // Passing segmentToLoad as * because we want to load all the segments into the
      // pre-aggregate table even if the user has set some segments on the parent table.
      PreAggregateUtil.startDataLoadForDataMap(
          parentCarbonTable,
          tableIdentifier,
          queryString,
          segmentToLoad = "*",
          validateSegments = true,
          isOverwrite = false,
          sparkSession = sparkSession)
    }
    Seq.empty
  }

  // Create the aggregation table name with parent table name prefix
  private lazy val tableIdentifier =
    TableIdentifier(parentTableIdentifier.table + "_" + dataMapName, parentTableIdentifier.database)

}


