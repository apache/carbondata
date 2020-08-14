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

package org.apache.spark.sql.listeners

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand
import org.apache.spark.sql.execution.command.partition.CarbonAlterTableDropHivePartitionCommand

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.view.{MVSchema, MVStatus}
import org.apache.carbondata.events.{AlterTableDropColumnPreEvent, _}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.merger.CompactionType
import org.apache.carbondata.view.{MVManagerInSpark, MVRefresher}

object MVListeners {
  def getRelatedColumns(mvSchema: MVSchema,
                        table: CarbonTable): mutable.Buffer[String] = {
    val columnList: mutable.Buffer[String] = new mutable.ArrayBuffer[String]()
    columnList.asJava.addAll(mvSchema.getRelatedTableColumns.get(table.getTableName))
    columnList
  }
}

/**
 * Listener to trigger compaction on mv after related table compaction
 */
object MVCompactionPostEventListener extends OperationEventListener {
  /**
   * Called on AlterTableCompactionPreStatusUpdateEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val compactionEvent = event.asInstanceOf[AlterTableCompactionPreStatusUpdateEvent]
    val compactionType = compactionEvent.carbonMergerMapping.compactionType
    if (compactionType == CompactionType.CUSTOM) {
      return
    }
    val table = compactionEvent.carbonTable
    val viewSchemas =
      MVManagerInSpark.get(compactionEvent.sparkSession).getSchemasOnTable(table)
    if (viewSchemas.isEmpty) {
      return
    }
    viewSchemas.asScala.foreach {
      viewSchema =>
        if (viewSchema.isRefreshIncremental) {
          val viewIdentifier = viewSchema.getIdentifier
          val alterTableModel = AlterTableModel(
            Some(viewIdentifier.getDatabaseName),
            viewIdentifier.getTableName,
            None,
            compactionType.toString,
            Some(System.currentTimeMillis()))
          operationContext.setProperty(
            viewIdentifier.getDatabaseName + "_" +
            viewIdentifier.getTableName + "_Segment",
            compactionEvent.carbonLoadModel.getSegmentId)
          val session = compactionEvent.sparkSession
          CarbonAlterTableCompactionCommand(alterTableModel, None, operationContext).run(session)
        }
    }
  }
}

object MVLoadPreEventListener extends OperationEventListener {
  /**
   * Called on LoadTablePreExecutionEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val loadEvent = event.asInstanceOf[LoadTablePreExecutionEvent]
    val loadModel = loadEvent.getCarbonLoadModel
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val isInternalCall = loadModel.isAggLoadRequest
    if (table.isMV && !isInternalCall) {
      throw new UnsupportedOperationException("Cannot insert data directly into mv.")
    }
  }
}

/**
 * Listener to trigger data load on mv after related table data load
 */
object MVLoadPostEventListener extends OperationEventListener {
  /**
   * Called on LoadTablePostExecutionEvent/UpdateTablePostEvent/DeleteFromTablePostEvent  occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val table: CarbonTable =
      event match {
        case event: LoadTablePostExecutionEvent =>
          val loadModel = Some(event.getCarbonLoadModel)
          if (loadModel.isDefined) {
            loadModel.get.getCarbonDataLoadSchema.getCarbonTable
          } else {
            null
          }
        case event: UpdateTablePostEvent =>
          val table = Some(event.carbonTable)
          if (table.isDefined) {
            table.get
          } else {
            null
          }
        case event: DeleteFromTablePostEvent =>
          val table = Some(event.carbonTable)
          if (table.isDefined) {
            table.get
          } else {
            null
          }
        case _ => null
      }
    val session: SparkSession =
      event match {
        case _: LoadTablePostExecutionEvent =>
          SparkSession.getActiveSession.get // TODO
        case event: UpdateTablePostEvent =>
          event.sparkSession
        case event: DeleteFromTablePostEvent =>
          event.sparkSession
        case _ => null
      }
    if (table == null) {
      return
    }
    val viewManager = MVManagerInSpark.get(session)
    val viewSchemas = viewManager.getSchemasOnTable(table)
    if (viewSchemas.isEmpty) {
      return
    }
    viewSchemas.asScala.foreach {
      viewSchema =>
        val viewIdentifier = viewSchema.getIdentifier
        if (!viewSchema.isRefreshOnManual) {
          try {
            MVRefresher.refresh(viewSchema, SparkSession.getActiveSession.get)
            viewManager.setStatus(viewIdentifier, MVStatus.ENABLED)
          } catch {
            case _: Exception =>
              viewManager.setStatus(viewIdentifier, MVStatus.DISABLED)
          }
        } else {
          viewManager.setStatus(viewIdentifier, MVStatus.DISABLED)
        }
    }
  }
}

/**
 * Listeners to block operations like delete segment on id or by date on tables
 * having an mv or on mv tables
 */
object MVDeleteSegmentPreEventListener extends OperationEventListener {
  /**
   * Called on DeleteSegmentByIdPreEvent/DeleteSegmentByDatePreEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val table = event match {
      case event: DeleteSegmentByIdPreEvent =>
        event.asInstanceOf[DeleteSegmentByIdPreEvent].carbonTable
      case event: DeleteSegmentByDatePreEvent =>
        event.asInstanceOf[DeleteSegmentByDatePreEvent].carbonTable
    }
    val session = event match {
      case event: DeleteSegmentByIdPreEvent =>
        event.asInstanceOf[DeleteSegmentByIdPreEvent].sparkSession
      case event: DeleteSegmentByDatePreEvent =>
        event.asInstanceOf[DeleteSegmentByDatePreEvent].sparkSession
    }
    if (table != null) {
      if (table.isMV) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on mv")
      }
      val viewSchemas = MVManagerInSpark.get(session).getSchemasOnTable(table)
      if (!viewSchemas.isEmpty) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on table related by mv " +
            viewSchemas.asScala.map(_.getIdentifier).mkString(", "))
      }
    }
  }
}

object MVAddColumnsPreEventListener extends OperationEventListener {
  /**
   * Called on AlterTableAddColumnPreEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val table = event.asInstanceOf[AlterTableAddColumnPreEvent].carbonTable
    if (table.isMV) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in a mv ${table.getDatabaseName}.${table.getTableName}")
    }
  }
}

object MVDropColumnPreEventListener extends OperationEventListener {
  /**
   * Called on AlterTableDropColumnPreEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropColumnEvent = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val dropColumnModel = dropColumnEvent.alterTableDropColumnModel
    val table = dropColumnEvent.carbonTable
    if (table.isMV) {
      throw new UnsupportedOperationException(
        s"Cannot drop columns in a mv ${table.getDatabaseName}.${table.getTableName}")
    }
    val viewSchemas =
      MVManagerInSpark.get(dropColumnEvent.sparkSession).getSchemasOnTable(table)
    if (viewSchemas.isEmpty) {
      return
    }
    viewSchemas.asScala.foreach {
      mvSchema =>
        val relatedColumns = MVListeners.getRelatedColumns(mvSchema, table)
        val relatedColumnsInDropModel = relatedColumns.collectFirst {
          case columnName if dropColumnModel.columns.contains(columnName.toLowerCase) => columnName
        }
        if (relatedColumnsInDropModel.isDefined) {
          val mvIdentifier = mvSchema.getIdentifier
          throw new UnsupportedOperationException(
            s"Column ${relatedColumnsInDropModel.head} cannot be dropped because it exists " +
            s"in mv ${mvIdentifier.getDatabaseName}.${mvIdentifier.getTableName}")
        }
    }
  }
}

object MVAlterColumnPreEventListener extends OperationEventListener {
  /**
   * Called on AlterTableColRenameAndDataTypeChangePreEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val alterColumnEvent = event.asInstanceOf[AlterTableColRenameAndDataTypeChangePreEvent]
    val alterTableModel = alterColumnEvent.alterTableDataTypeChangeModel
    val table = alterColumnEvent.carbonTable
    if (table.isMV) {
      throw new UnsupportedOperationException(
        s"Cannot change data type or rename column for columns in mv " +
        s"${table.getDatabaseName}.${table.getTableName}")
    }
    val viewSchemas =
      MVManagerInSpark.get(alterColumnEvent.sparkSession).getSchemasOnTable(table)
    if (viewSchemas.isEmpty) {
      return
    }
    viewSchemas.asScala.foreach {
      viewSchema =>
        val relatedColumns = MVListeners.getRelatedColumns(viewSchema, table)
        if (relatedColumns.contains(alterTableModel.columnName.toLowerCase)) {
          val viewIdentifier = viewSchema.getIdentifier
          throw new UnsupportedOperationException(
            s"Column ${alterTableModel.columnName} exists " +
            s"in mv ${viewIdentifier.getDatabaseName}.${viewIdentifier.getTableName}.")
        }
    }
  }
}

object MVDropPartitionMetaEventListener extends OperationEventListener {
  /**
   * Called on AlterTableDropPartitionMetaEvent occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPartitionEvent = event.asInstanceOf[AlterTableDropPartitionMetaEvent]
    val session = dropPartitionEvent.sparkSession
    val table = dropPartitionEvent.parentCarbonTable
    if (table.isMV) {
      if (operationContext.getProperty("isInternalCall") == null) {
        throw new UnsupportedOperationException("Cannot drop partition directly on mv")
      }
      return
    }
    val viewSchemas = MVManagerInSpark.get(session).getSchemasOnTable(table)
    if (viewSchemas.isEmpty) {
      return
    }
    // Filter out all the tables which don't have the partition being dropped.
    val viewSchemasWithoutPartitionColumns = viewSchemas.asScala.filter {
      viewSchema =>
        val relatedColumns = viewSchema.getRelatedTableColumns.get(table.getTableName).asScala
        val relatedColumnInDropPartitions = dropPartitionEvent.specs.flatMap(_.keys).forall {
            partitionColumn =>
              relatedColumns.exists {
                relatedColumn => relatedColumn.equalsIgnoreCase(partitionColumn)
              }
          }
        !relatedColumnInDropPartitions
      }
    if (viewSchemasWithoutPartitionColumns.nonEmpty) {
      val viewIdentifiers = viewSchemasWithoutPartitionColumns.toList.map{
        viewSchema =>
          viewSchema.getIdentifier.getDatabaseName + viewSchema.getIdentifier.getTableName
      }
      throw new MetadataProcessException(
        s"Cannot drop partition as one of the partition is not participating " +
        s"in the following mvs ${viewIdentifiers.mkString(",")}. " +
        s"Please drop the specified mvs to continue")
    }
    // blocked drop partition for child tables having more than one parent table
    val viewSchemasWithoutPartition = viewSchemas.asScala.filter(_.getRelatedTables.size() >= 2)
    if (viewSchemasWithoutPartition.nonEmpty) {
      val viewIdentifiers = viewSchemasWithoutPartition.toList.map{
        viewSchema =>
          viewSchema.getIdentifier.getDatabaseName + viewSchema.getIdentifier.getTableName
      }
      throw new MetadataProcessException(
        s"Cannot drop partition if mv associate more than one table. " +
        s"Please drop the specified mvs $viewIdentifiers to continue")
    }
    val dropPartitionCommands =
      viewSchemas.asScala.map {
        viewSchema =>
        val viewIdentifier = TableIdentifier(viewSchema.getIdentifier.getTableName,
          Some(viewSchema.getIdentifier.getDatabaseName))
        if (!CarbonEnv.getCarbonTable(viewIdentifier)(session).isHivePartitionTable) {
          throw new MetadataProcessException(
            s"Cannot drop partition as one of the partition is not participating " +
            s"in the following mvs ${viewIdentifier.database}.${viewIdentifier.table}. " +
            s"Please drop the specified mv to continue")
        }
        // as the mv columns start with parent table name therefore the
        // partition column also has to be updated with parent table name to generate
        // partitionSpecs for the child table.
        CarbonAlterTableDropHivePartitionCommand(
          viewIdentifier,
          dropPartitionEvent.specs.map {
            spec =>
              spec.map {
                case (key, value) => (s"${table.getTableName}_$key", value)
              }
          },
          dropPartitionEvent.ifExists,
          dropPartitionEvent.purge,
          dropPartitionEvent.retainData,
          operationContext)
      }
    // used as a flag to block direct drop partition on mvs fired by the user
    operationContext.setProperty("isInternalCall", "true")
    operationContext.setProperty("dropPartitionCommands", dropPartitionCommands)
    dropPartitionCommands.foreach(_.processMetadata(session))
  }
}

object MVDropPartitionPreEventListener extends OperationEventListener {
  /**
   * Called on AlterTableDropPartitionPreStatusEvent occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPartitionEvent = event.asInstanceOf[AlterTableDropPartitionPreStatusEvent]
    val dropPartitionCommands = operationContext.getProperty("dropPartitionCommands")
    val table = dropPartitionEvent.carbonTable
    if (dropPartitionCommands != null &&
      MVManagerInSpark.get(dropPartitionEvent.sparkSession).hasSchemaOnTable(table)) {
      dropPartitionCommands.asInstanceOf[Seq[CarbonAlterTableDropHivePartitionCommand]]
        .foreach(_.processData(SparkSession.getActiveSession.get))
    }
  }
}

object MVDropTablePreEventListener extends OperationEventListener {
  /**
   * Called on DropTablePreEvent occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropTableEvent = event.asInstanceOf[DropTablePreEvent]
    val table = dropTableEvent.carbonTable
    if (table.isMV && !dropTableEvent.isInternalCall) {
      throw new UnsupportedOperationException("Cannot drop mv with drop table command")
    }
  }
}
