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
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.merger.CompactionType

object DataMapListeners {
  def getDataMapTableColumns(dataMapSchema: DataMapSchema,
      carbonTable: CarbonTable): mutable.Buffer[String] = {
    val listOfColumns: mutable.Buffer[String] = new mutable.ArrayBuffer[String]()
    listOfColumns.asJava
      .addAll(dataMapSchema.getMainTableColumnList.get(carbonTable.getTableName))
    listOfColumns
  }
}

/**
 * Listener to trigger compaction on mv datamap after main table compaction
 */
object AlterDataMaptableCompactionPostListener extends OperationEventListener {
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
    if (compactionType == CompactionType.CUSTOM) {
      return
    }
    val carbonLoadModel = compactionEvent.carbonLoadModel
    val sparkSession = compactionEvent.sparkSession
    val allDataMapSchemas = DataMapStoreManager.getInstance
      .getDataMapSchemasOfTable(carbonTable).asScala
      .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                               !dataMapSchema.isIndexDataMap)
    if (!allDataMapSchemas.asJava.isEmpty) {
      allDataMapSchemas.foreach { dataMapSchema =>
        val childRelationIdentifier = dataMapSchema.getRelationIdentifier
        val alterTableModel = AlterTableModel(Some(childRelationIdentifier.getDatabaseName),
          childRelationIdentifier.getTableName,
          None,
          compactionType.toString,
          Some(System.currentTimeMillis()),
          "")
        operationContext.setProperty(
          dataMapSchema.getRelationIdentifier.getDatabaseName + "_" +
          dataMapSchema.getRelationIdentifier.getTableName + "_Segment",
          carbonLoadModel.getSegmentId)
        CarbonAlterTableCompactionCommand(alterTableModel, operationContext = operationContext)
          .run(sparkSession)
      }
    }
  }
}

object LoadMVTablePreListener extends OperationEventListener {
  /**
   * Called on LoadTablePreExecutionEvent event occurrence
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val loadEvent = event.asInstanceOf[LoadTablePreExecutionEvent]
    val carbonLoadModel = loadEvent.getCarbonLoadModel
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val isInternalLoadCall = carbonLoadModel.isAggLoadRequest
    if (table.isChildTableForMV && !isInternalLoadCall) {
      throw new UnsupportedOperationException("Cannot insert data directly into MV table")
    }
  }
}

/**
 * Listener to trigger data load on mv datamap after main table data load
 */
object LoadPostDataMapListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val sparkSession = SparkSession.getActiveSession.get
    val carbonTable: CarbonTable =
      event match {
        case event: LoadTablePostExecutionEvent =>
          val carbonLoadModelOption = Some(event.getCarbonLoadModel)
          if (carbonLoadModelOption.isDefined) {
            val carbonLoadModel = carbonLoadModelOption.get
            carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
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
    if (null != carbonTable) {
      val allDataMapSchemas = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
        .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                 !dataMapSchema.isIndexDataMap)
      if (!allDataMapSchemas.asJava.isEmpty) {
        allDataMapSchemas.foreach { dataMapSchema =>
          if (!dataMapSchema.isLazy) {
            val provider = DataMapManager.get()
              .getDataMapProvider(carbonTable, dataMapSchema, sparkSession)
            try {
              provider.rebuild()
              DataMapStatusManager.enableDataMap(dataMapSchema.getDataMapName)
            } catch {
              case ex: Exception =>
                DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
            }
          }
        }
      }
    }
  }
}

/**
 * Listeners to block operations like delete segment on id or by date on tables
 * having an mv datamap or on mv datamap tables
 */
object DataMapDeleteSegmentPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val carbonTable = event match {
      case e: DeleteSegmentByIdPreEvent =>
        e.asInstanceOf[DeleteSegmentByIdPreEvent].carbonTable
      case e: DeleteSegmentByDatePreEvent =>
        e.asInstanceOf[DeleteSegmentByDatePreEvent].carbonTable
    }
    if (null != carbonTable) {
      if (carbonTable.hasMVCreated) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables having child datamap")
      }
      if (carbonTable.isChildTableForMV) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on datamap table")
      }
    }
  }
}

object DataMapAddColumnsPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    if (carbonTable.isChildTableForMV) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in a DataMap table " +
        s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    }
  }
}


object DataMapDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropColumnChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dropColumnChangePreListener.carbonTable
    val alterTableDropColumnModel = dropColumnChangePreListener.alterTableDropColumnModel
    val columnsToBeDropped = alterTableDropColumnModel.columns
    if (carbonTable.hasMVCreated) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (null != dataMapSchema && !dataMapSchema.isIndexDataMap) {
          val listOfColumns = DataMapListeners.getDataMapTableColumns(dataMapSchema, carbonTable)
          val columnExistsInChild = listOfColumns.collectFirst {
            case parentColumnName if columnsToBeDropped.contains(parentColumnName.toLowerCase) =>
              parentColumnName
          }
          if (columnExistsInChild.isDefined) {
            throw new UnsupportedOperationException(
              s"Column ${ columnExistsInChild.head } cannot be dropped because it exists " +
              s"in " + dataMapSchema.getProviderName + " datamap:" +
              s"${ dataMapSchema.getDataMapName }")
          }
        }
      }
    }
    if (carbonTable.isChildTableForMV) {
      throw new UnsupportedOperationException(
        s"Cannot drop columns present in a datamap table ${ carbonTable.getDatabaseName }." +
        s"${ carbonTable.getTableName }")
    }
  }
}

object DataMapChangeDataTypeorRenameColumnPreListener
  extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val colRenameDataTypeChangePreListener = event
      .asInstanceOf[AlterTableColRenameAndDataTypeChangePreEvent]
    val carbonTable = colRenameDataTypeChangePreListener.carbonTable
    val alterTableDataTypeChangeModel = colRenameDataTypeChangePreListener
      .alterTableDataTypeChangeModel
    val columnToBeAltered: String = alterTableDataTypeChangeModel.columnName
    if (carbonTable.hasMVCreated) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (null != dataMapSchema && !dataMapSchema.isIndexDataMap) {
          val listOfColumns = DataMapListeners.getDataMapTableColumns(dataMapSchema, carbonTable)
          if (listOfColumns.contains(columnToBeAltered.toLowerCase)) {
            throw new UnsupportedOperationException(
              s"Column $columnToBeAltered exists in a " + dataMapSchema.getProviderName +
              " datamap. Drop " + dataMapSchema.getProviderName + "  datamap to continue")
          }
        }
      }
    }
    if (carbonTable.isChildTableForMV) {
      throw new UnsupportedOperationException(
        s"Cannot change data type or rename column for columns present in mv datamap table " +
        s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    }
  }
}

object DataMapAlterTableDropPartitionMetaListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPartitionEvent = event.asInstanceOf[AlterTableDropPartitionMetaEvent]
    val parentCarbonTable = dropPartitionEvent.parentCarbonTable
    val partitionsToBeDropped = dropPartitionEvent.specs.flatMap(_.keys)
    if (parentCarbonTable.hasMVCreated) {
      // used as a flag to block direct drop partition on datamap tables fired by the user
      operationContext.setProperty("isInternalDropCall", "true")
      // Filter out all the tables which don't have the partition being dropped.
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(parentCarbonTable).asScala
      val childTablesWithoutPartitionColumns =
        dataMapSchemaList.filter { dataMapSchema =>
          val childColumns = dataMapSchema.getMainTableColumnList
            .get(parentCarbonTable.getTableName).asScala
          val partitionColExists =
            partitionsToBeDropped.forall {
              partition =>
                childColumns.exists { childColumn =>
                  childColumn.equalsIgnoreCase(partition)
                }
            }
          !partitionColExists
        }
      if (childTablesWithoutPartitionColumns.nonEmpty) {
        throw new MetadataProcessException(s"Cannot drop partition as one of the partition is not" +
                                           s" participating in the following datamaps ${
                                             childTablesWithoutPartitionColumns.toList
                                               .map(_.getRelationIdentifier.getTableName)
                                           }. Please drop the specified child tables to " +
                                           s"continue")
      } else {
        // blocked drop partition for child tables having more than one parent table
        val nonPartitionChildTables = dataMapSchemaList.filter(_.getParentTables.size() >= 2)
        if (nonPartitionChildTables.nonEmpty) {
          throw new MetadataProcessException(
            s"Cannot drop partition if child Table is mapped to more than one parent table. Drop " +
            s"datamaps ${ nonPartitionChildTables.toList.map(_.getDataMapName) }  to continue")
        }
        val childDropPartitionCommands =
          dataMapSchemaList.map { dataMapSchema =>
            val tableIdentifier = TableIdentifier(dataMapSchema.getRelationIdentifier.getTableName,
              Some(dataMapSchema.getRelationIdentifier.getDatabaseName))
            if (!CarbonEnv.getCarbonTable(tableIdentifier)(SparkSession.getActiveSession.get)
              .isHivePartitionTable) {
              throw new MetadataProcessException(
                "Cannot drop partition as one of the partition is not participating in the " +
                "following datamap " + dataMapSchema.getDataMapName +
                ". Please drop the specified datamap to continue")
            }
            // as the datamap table columns start with parent table name therefore the
            // partition column also has to be updated with parent table name to generate
            // partitionSpecs for the child table.
            val childSpecs = dropPartitionEvent.specs.map {
              spec =>
                spec.map {
                  case (key, value) => (s"${ parentCarbonTable.getTableName }_$key", value)
                }
            }
            CarbonAlterTableDropHivePartitionCommand(
              tableIdentifier,
              childSpecs,
              dropPartitionEvent.ifExists,
              dropPartitionEvent.purge,
              dropPartitionEvent.retainData,
              operationContext)
          }
        operationContext.setProperty("dropPartitionCommands", childDropPartitionCommands)
        childDropPartitionCommands.foreach(_.processMetadata(SparkSession.getActiveSession.get))
      }
    } else if (parentCarbonTable.isChildTableForMV) {
      if (operationContext.getProperty("isInternalDropCall") == null) {
        throw new UnsupportedOperationException("Cannot drop partition directly on child table")
      }
    }
  }
}

object DataMapAlterTableDropPartitionPreStatusListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event,
      operationContext: OperationContext) = {
    val preStatusListener = event.asInstanceOf[AlterTableDropPartitionPreStatusEvent]
    val carbonTable = preStatusListener.carbonTable
    val childDropPartitionCommands = operationContext.getProperty("dropPartitionCommands")
    if (childDropPartitionCommands != null &&
        carbonTable.hasMVCreated) {
      val childCommands =
        childDropPartitionCommands.asInstanceOf[Seq[CarbonAlterTableDropHivePartitionCommand]]
      childCommands.foreach(_.processData(SparkSession.getActiveSession.get))
    }
  }
}

