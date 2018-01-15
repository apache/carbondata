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

import org.apache.spark.sql.{CarbonEnv, CarbonSession, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{AggregationDataMapSchema, CarbonTable}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * below class will be used to create load command for compaction
 * for all the pre agregate child data map
 */
object CompactionProcessMetaListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val sparkSession = SparkSession.getActiveSession.get
    val tableEvent = event.asInstanceOf[LoadMetadataEvent]
    val table = tableEvent.getCarbonTable
    if (!table.isChildDataMap && CarbonUtil.hasAggregationDataMap(table)) {
      val aggregationDataMapList = table.getTableInfo.getDataMapSchemaList.asScala
        .filter(_.isInstanceOf[AggregationDataMapSchema])
        .asInstanceOf[mutable.ArrayBuffer[AggregationDataMapSchema]]
      for (dataMapSchema: AggregationDataMapSchema <- aggregationDataMapList) {
        val childTableName = dataMapSchema.getRelationIdentifier.getTableName
        val childDatabaseName = dataMapSchema.getRelationIdentifier.getDatabaseName
        // Creating a new query string to insert data into pre-aggregate table from that same table.
        // For example: To compact preaggtable1 we can fire a query like insert into preaggtable1
        // select * from preaggtable1
        // The following code will generate the select query with a load UDF that will be used to
        // apply DataLoadingRules
        val childDataFrame = sparkSession.sql(new CarbonSpark2SqlParser()
          // adding the aggregation load UDF
          .addPreAggLoadFunction(
          // creating the select query on the bases on table schema
          PreAggregateUtil.createChildSelectQuery(
            dataMapSchema.getChildSchema, table.getDatabaseName))).drop("preAggLoad")
        val loadCommand = PreAggregateUtil.createLoadCommandForChild(
          dataMapSchema.getChildSchema.getListOfColumns,
          TableIdentifier(childTableName, Some(childDatabaseName)),
          childDataFrame,
          false,
          sparkSession)
        loadCommand.processMetadata(sparkSession)
        operationContext
          .setProperty(dataMapSchema.getChildSchema.getTableName + "_Compaction", loadCommand)
      }
    } else if (table.isChildDataMap) {
      val childTableName = table.getTableName
      val childDatabaseName = table.getDatabaseName
      // Creating a new query string to insert data into pre-aggregate table from that same table.
      // For example: To compact preaggtable1 we can fire a query like insert into preaggtable1
      // select * from preaggtable1
      // The following code will generate the select query with a load UDF that will be used to
      // apply DataLoadingRules
      val childDataFrame = sparkSession.sql(new CarbonSpark2SqlParser()
        // adding the aggregation load UDF
        .addPreAggLoadFunction(
        // creating the select query on the bases on table schema
        PreAggregateUtil.createChildSelectQuery(
          table.getTableInfo.getFactTable, table.getDatabaseName))).drop("preAggLoad")
      val loadCommand = PreAggregateUtil.createLoadCommandForChild(
        table.getTableInfo.getFactTable.getListOfColumns,
        TableIdentifier(childTableName, Some(childDatabaseName)),
        childDataFrame,
        false,
        sparkSession)
      loadCommand.processMetadata(sparkSession)
      operationContext.setProperty(table.getTableName + "_Compaction", loadCommand)
    }
  }
}

/**
 * Below class to is to create LoadCommand for loading the
 * the data of pre aggregate data map
 */
object LoadProcessMetaListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val sparkSession = SparkSession.getActiveSession.get
    val tableEvent = event.asInstanceOf[LoadMetadataEvent]
    if (!tableEvent.isCompaction) {
      val table = tableEvent.getCarbonTable
      if (CarbonUtil.hasAggregationDataMap(table)) {
        // getting all the aggergate datamap schema
        val aggregationDataMapList = table.getTableInfo.getDataMapSchemaList.asScala
          .filter(_.isInstanceOf[AggregationDataMapSchema])
          .asInstanceOf[mutable.ArrayBuffer[AggregationDataMapSchema]]
        // sorting the datamap for timeseries rollup
        val sortedList = aggregationDataMapList.sortBy(_.getOrdinal)
        val parentTableName = table.getTableName
        val databaseName = table.getDatabaseName
        val list = scala.collection.mutable.ListBuffer.empty[AggregationDataMapSchema]
        for (dataMapSchema: AggregationDataMapSchema <- sortedList) {
          val childTableName = dataMapSchema.getRelationIdentifier.getTableName
          val childDatabaseName = dataMapSchema.getRelationIdentifier.getDatabaseName
          val childSelectQuery = if (!dataMapSchema.isTimeseriesDataMap) {
            PreAggregateUtil.getChildQuery(dataMapSchema)
          } else {
            // for timeseries rollup policy
            val tableSelectedForRollup = PreAggregateUtil.getRollupDataMapNameForTimeSeries(list,
              dataMapSchema)
            list += dataMapSchema
            // if non of the rollup data map is selected hit the maintable and prepare query
            if (tableSelectedForRollup.isEmpty) {
              PreAggregateUtil.createTimeSeriesSelectQueryFromMain(dataMapSchema.getChildSchema,
                parentTableName,
                databaseName)
            } else {
              // otherwise hit the select rollup datamap schema
              PreAggregateUtil.createTimeseriesSelectQueryForRollup(dataMapSchema.getChildSchema,
                tableSelectedForRollup.get,
                databaseName)
            }
          }
          val childDataFrame = sparkSession.sql(new CarbonSpark2SqlParser().addPreAggLoadFunction(
            childSelectQuery)).drop("preAggLoad")
          val isOverwrite =
            operationContext.getProperty("isOverwrite").asInstanceOf[Boolean]
          val loadCommand = PreAggregateUtil.createLoadCommandForChild(
            dataMapSchema.getChildSchema.getListOfColumns,
            TableIdentifier(childTableName, Some(childDatabaseName)),
            childDataFrame,
            isOverwrite,
            sparkSession)
          loadCommand.processMetadata(sparkSession)
          operationContext.setProperty(dataMapSchema.getChildSchema.getTableName, loadCommand)
        }
      }
    }
  }
}
object LoadPostAggregateListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val loadEvent = event.asInstanceOf[LoadTablePreStatusUpdateEvent]
    val sparkSession = SparkSession.getActiveSession.get
    val carbonLoadModel = loadEvent.getCarbonLoadModel
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    if (CarbonUtil.hasAggregationDataMap(table)) {
      // getting all the aggergate datamap schema
      val aggregationDataMapList = table.getTableInfo.getDataMapSchemaList.asScala
        .filter(_.isInstanceOf[AggregationDataMapSchema])
        .asInstanceOf[mutable.ArrayBuffer[AggregationDataMapSchema]]
      // sorting the datamap for timeseries rollup
      val sortedList = aggregationDataMapList.sortBy(_.getOrdinal)
      for (dataMapSchema: AggregationDataMapSchema <- sortedList) {
        val childLoadCommand = operationContext
          .getProperty(dataMapSchema.getChildSchema.getTableName)
          .asInstanceOf[CarbonLoadDataCommand]
        childLoadCommand.dataFrame = Some(PreAggregateUtil
          .getDataFrame(sparkSession, childLoadCommand.logicalPlan.get))
        val isOverwrite =
          operationContext.getProperty("isOverwrite").asInstanceOf[Boolean]
        childLoadCommand.operationContext = operationContext
        PreAggregateUtil.startDataLoadForDataMap(
            table,
            carbonLoadModel.getSegmentId,
            validateSegments = false,
            childLoadCommand,
            isOverwrite,
            sparkSession)
        }
      }
    }
}

/**
 * This listener is used to commit all the child data aggregate tables in one transaction. If one
 * failes all will be reverted to original state.
 */
object CommitPreAggregateListener extends OperationEventListener {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override protected def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val commitEvent = event.asInstanceOf[LoadTablePostStatusUpdateEvent]
    val carbonLoadModel = commitEvent.getCarbonLoadModel
    val uuid = operationContext.getProperty("uuid").toString
    val dataMapSchemas =
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getTableInfo.getDataMapSchemaList
    // extract all child LoadCommands
    val childLoadCommands = dataMapSchemas.asScala.map { dataMapSchema =>
      operationContext.getProperty(dataMapSchema.getChildSchema.getTableName)
        .asInstanceOf[CarbonLoadDataCommand]
    }
    if (carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isChildDataMap) {
      val childCarbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val carbonTablePath =
        new CarbonTablePath(childCarbonTable.getCarbonTableIdentifier,
          childCarbonTable.getTablePath)
      // Generate table status file name with UUID, forExample: tablestatus_1
      val oldTableSchemaPath = carbonTablePath.getTableStatusFilePathWithUUID(uuid)
      // Generate table status file name without UUID, forExample: tablestatus
      val newTableSchemaPath = carbonTablePath.getTableStatusFilePath
      // try to rename tablestatus_1 to tablestatus after taking backup of tablestatus by
      // renaming to tablestatus_backup_1
      val commitFailed =
        !renameDataMapTableStatusFiles(oldTableSchemaPath, newTableSchemaPath, uuid)
      // if renaming is not successful then rename tablestatus_backup_1 to tablestatus
      if (commitFailed) {
        val backupTableSchemaPath = carbonTablePath.getTableStatusFilePath + "_backup_" + uuid
        val tableSchemaPath = carbonTablePath.getTableStatusFilePath
        renameDataMapTableStatusFiles(backupTableSchemaPath, tableSchemaPath, "")
      }
      // after success/failure of commit delete all tablestatus files with UUID in their names.
      // if commit failed then remove the segment directory
      cleanUpStaleTableStatusFiles(Seq(childCarbonTable), operationContext, uuid, commitFailed)
      if (commitFailed) {
        sys.error("Failed to update table status for pre-aggregate table")
      }
    } else if (dataMapSchemas.size() > 0) {
      // keep committing until one fails
      val renamedDataMaps = childLoadCommands.takeWhile { childLoadCommand =>
        val childCarbonTable = childLoadCommand.table
        val carbonTablePath =
          new CarbonTablePath(childCarbonTable.getCarbonTableIdentifier,
            childCarbonTable.getTablePath)
        // Generate table status file name with UUID, forExample: tablestatus_1
        val oldTableSchemaPath = carbonTablePath.getTableStatusFilePathWithUUID(uuid)
        // Generate table status file name without UUID, forExample: tablestatus
        val newTableSchemaPath = carbonTablePath.getTableStatusFilePath
        renameDataMapTableStatusFiles(oldTableSchemaPath, newTableSchemaPath, uuid)
      }
      // if true then the commit for one of the child tables has failed
      val commitFailed = renamedDataMaps.lengthCompare(dataMapSchemas.size()) != 0
      if (commitFailed) {
        LOGGER.warn("Reverting table status file to original state")
        renamedDataMaps.foreach {
          loadCommand =>
            val carbonTable = loadCommand.table
            val carbonTablePath = new CarbonTablePath(carbonTable.getCarbonTableIdentifier,
              carbonTable.getTablePath)
            val backupTableSchemaPath = carbonTablePath.getTableStatusFilePath + "_backup_" + uuid
            val tableSchemaPath = carbonTablePath.getTableStatusFilePath
            renameDataMapTableStatusFiles(backupTableSchemaPath, tableSchemaPath, "")
        }
      }
      // after success/failure of commit delete all tablestatus files with UUID in their names.
      // if commit failed then remove the segment directory
      cleanUpStaleTableStatusFiles(childLoadCommands.map(_.table),
        operationContext,
        uuid,
        commitFailed)
      if (commitFailed) {
        sys.error("Failed to update table status for pre-aggregate table")
      }
    }
  }

  /**
   *  Used to rename table status files for commit operation.
   */
  private def renameDataMapTableStatusFiles(sourceFileName: String,
      destinationFileName: String, uuid: String) = {
    val oldCarbonFile = FileFactory.getCarbonFile(sourceFileName)
    val newCarbonFile = FileFactory.getCarbonFile(destinationFileName)
    if (oldCarbonFile.exists() && newCarbonFile.delete()) {
      val backUpPostFix = if (uuid.nonEmpty) {
        "_backup_" + uuid
      } else {
        ""
      }
      LOGGER.info(s"Renaming $newCarbonFile to ${destinationFileName + backUpPostFix}")
      if (newCarbonFile.renameForce(destinationFileName + backUpPostFix)) {
        LOGGER.info(s"Renaming $oldCarbonFile to $destinationFileName")
        oldCarbonFile.renameForce(destinationFileName)
      } else {
        LOGGER.info(s"Renaming $newCarbonFile to ${destinationFileName + backUpPostFix} failed")
        false
      }
    } else {
      false
    }
  }

  /**
   * Used to remove table status files with UUID and segment folders.
   */
  private def cleanUpStaleTableStatusFiles(
      childTables: Seq[CarbonTable],
      operationContext: OperationContext,
      uuid: String,
      removeSegmentDir: Boolean): Unit = {
    childTables.foreach { childTable =>
      val carbonTablePath = new CarbonTablePath(childTable.getCarbonTableIdentifier,
        childTable.getTablePath)
      val segmentDirPath = carbonTablePath.getSegmentDir("0",
        operationContext.getProperty(childTable.getTableUniqueName + "_Segment").toString)
      val metaDataDir = FileFactory.getCarbonFile(carbonTablePath.getMetadataDirectoryPath)
      val segmentDir = FileFactory.getCarbonFile(segmentDirPath)
      if (segmentDir.exists() && removeSegmentDir) {
        segmentDir.delete()
      }
      val tableStatusFiles = metaDataDir.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          file.getName.contains(uuid) || file.getName.contains("backup")
        }
      })
      tableStatusFiles.foreach(_.delete())
    }
  }
}

/**
 * Listener to handle the operations that have to be done after compaction for a table has finished.
 */
object AlterPreAggregateTableCompactionPostListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val compactionEvent = event.asInstanceOf[AlterTableCompactionPreStatusUpdateEvent]
    val carbonTable = compactionEvent.carbonTable
    val compactionType = compactionEvent.carbonMergerMapping.campactionType
    val sparkSession = compactionEvent.sparkSession
    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      carbonTable.getTableInfo.getDataMapSchemaList.asScala.foreach { dataMapSchema =>
        val childRelationIdentifier = dataMapSchema.getRelationIdentifier
        val alterTableModel = AlterTableModel(Some(childRelationIdentifier.getDatabaseName),
          childRelationIdentifier.getTableName,
          None,
          compactionType.toString,
          Some(System.currentTimeMillis()),
          "")
        CarbonAlterTableCompactionCommand(alterTableModel, operationContext = operationContext)
          .run(sparkSession)
      }
    }
  }
}

object LoadPreAggregateTablePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val loadEvent = event.asInstanceOf[LoadTablePreExecutionEvent]
    val carbonLoadModel = loadEvent.getCarbonLoadModel
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val isInternalLoadCall = carbonLoadModel.isAggLoadRequest
    if (table.isChildDataMap && !isInternalLoadCall) {
      throw new UnsupportedOperationException(
        "Cannot insert/load data directly into pre-aggregate table")
    }
  }
}

object PreAggregateDataTypeChangePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDataTypeChangePreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    val alterTableDataTypeChangeModel = dataTypeChangePreListener.alterTableDataTypeChangeModel
    val columnToBeAltered: String = alterTableDataTypeChangeModel.columnName
    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
      dataMapSchemas.asScala.foreach { dataMapSchema =>
        val childColumns = dataMapSchema.getChildSchema.getListOfColumns
        val parentColumnNames = childColumns.asScala
          .flatMap(_.getParentColumnTableRelations.asScala.map(_.getColumnName))
        if (parentColumnNames.contains(columnToBeAltered)) {
          throw new UnsupportedOperationException(
            s"Column $columnToBeAltered exists in a pre-aggregate table. Drop pre-aggregate table" +
            "to continue")
        }
      }
    }
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        s"Cannot change data type for columns in pre-aggregate table ${ carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
    }
  }
}

object PreAggregateAddColumnsPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in pre-aggreagate table ${ carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
    }
  }
}

object PreAggregateDeleteSegmentByDatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val deleteSegmentByDatePreEvent = event.asInstanceOf[DeleteSegmentByDatePreEvent]
    val carbonTable = deleteSegmentByDatePreEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table. " +
          "Drop pre-aggregation table to continue")
      }
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on pre-aggregate table")
      }
    }
  }
}

object PreAggregateDeleteSegmentByIdPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table")
      }
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on pre-aggregate table")
      }
    }
  }

}

object PreAggregateDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    val alterTableDropColumnModel = dataTypeChangePreListener.alterTableDropColumnModel
    val columnsToBeDropped = alterTableDropColumnModel.columns
    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
      dataMapSchemas.asScala.foreach { dataMapSchema =>
        val parentColumnNames = dataMapSchema.getChildSchema.getListOfColumns.asScala
          .flatMap(_.getParentColumnTableRelations.asScala.map(_.getColumnName))
        val columnExistsInChild = parentColumnNames.collectFirst {
          case parentColumnName if columnsToBeDropped.contains(parentColumnName) =>
            parentColumnName
        }
        if (columnExistsInChild.isDefined) {
          throw new UnsupportedOperationException(
            s"Column ${ columnExistsInChild.head } cannot be dropped because it exists in a " +
            s"pre-aggregate table ${ dataMapSchema.getRelationIdentifier.toString }")
        }
      }
    }
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(s"Cannot drop columns in pre-aggreagate table ${
        carbonTable.getDatabaseName}.${ carbonTable.getTableName }")
    }
  }
}

object PreAggregateRenameTablePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val renameTablePostListener = event.asInstanceOf[AlterTableRenamePreEvent]
    val carbonTable = renameTablePostListener.carbonTable
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        "Rename operation for pre-aggregate table is not supported.")
    }
    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      throw new UnsupportedOperationException(
        "Rename operation is not supported for table with pre-aggregate tables")
    }
  }
}

object UpdatePreAggregatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[UpdateTablePreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Update operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException(
          "Update operation is not supported for pre-aggregate table")
      }
    }
  }
}

object DeletePreAggregatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteFromTablePreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for pre-aggregate table")
      }
    }
  }
}
