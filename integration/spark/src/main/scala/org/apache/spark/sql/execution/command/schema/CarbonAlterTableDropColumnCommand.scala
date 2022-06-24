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

package org.apache.spark.sql.execution.command.schema

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDropColumnModel, MetadataCommand}
import org.apache.spark.sql.hive.MockClassForAlterRevertTests
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{AlterTableDropColumnPostEvent, AlterTableDropColumnPreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.SchemaEvolutionEntry

private[sql] case class CarbonAlterTableDropColumnCommand(
    alterTableDropColumnModel: AlterTableDropColumnModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableDropColumnModel.tableName
    val dbName = alterTableDropColumnModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    setAuditTable(dbName, tableName)
    setAuditInfo(Map("column" -> alterTableDropColumnModel.columns.mkString(", ")))
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      if (carbonTable.isIndexTable) {
        throw new MalformedCarbonCommandException(
          "alter table drop column is not supported for index table")
      }
      if (!carbonTable.canAllow(carbonTable, TableOperation.ALTER_DROP,
          alterTableDropColumnModel.columns.asJava)) {
        throw new MalformedCarbonCommandException(
          "alter table drop column is not supported for index indexSchema")
      }
      // Do not allow spatial index source columns to be dropped.
      AlterTableUtil.validateColumnsWithSpatialIndexProperties(carbonTable,
        alterTableDropColumnModel.columns)
      val partitionInfo = carbonTable.getPartitionInfo()
      val tableColumns = carbonTable.getCreateOrderColumn().asScala
      if (partitionInfo != null) {
        val partitionColumnSchemaList = partitionInfo.getColumnSchemaList.asScala
          .map(_.getColumnName)
        // check each column existence in the table
        val partitionColumns = alterTableDropColumnModel.columns.filter {
          tableColumn => partitionColumnSchemaList.contains(tableColumn)
        }
        if (partitionColumns.nonEmpty) {
          throwMetadataException(dbName, tableName, "Partition columns cannot be dropped: " +
                                                  s"$partitionColumns")
        }

        // this check is added because, when table have only two columns, one is partition and one
        // is non partition, then dropping one column means, having table with only partition
        // column, which is wrong
        if (tableColumns.filterNot(col => alterTableDropColumnModel.columns
          .contains(col.getColName)).map(_.getColName).equals(partitionColumnSchemaList)) {
          throw new MalformedCarbonCommandException(
            "alter table drop column is failed, cannot have the table with all columns as " +
            "partition columns")
        }
      }
      val bucketInfo = carbonTable.getBucketingInfo
      if (bucketInfo != null) {
        val bucketColumnSchemaList = bucketInfo.getListOfColumns.asScala
          .map(_.getColumnName)
        // check each column existence in the table
        val bucketColumns = alterTableDropColumnModel.columns.filter {
          tableColumn => bucketColumnSchemaList.contains(tableColumn)
        }
        if (bucketColumns.nonEmpty) {
          throwMetadataException(dbName, tableName, "Bucket columns cannot be dropped: " +
            s"$bucketColumns")
        }
      }

      var dictionaryColumns = Seq[org.apache.carbondata.core.metadata.schema.table.column
      .ColumnSchema]()
      // TODO: if deleted column list includes bucketed column throw an error
      alterTableDropColumnModel.columns.foreach { column =>
        var columnExist = false
        tableColumns.foreach { tableColumn =>
          // column should not be already deleted and should exist in the table
          if (!tableColumn.isInvisible && column.equalsIgnoreCase(tableColumn.getColName)) {
            if (tableColumn.isDimension) {
              if (tableColumn.getDataType == DataTypes.DATE) {
                dictionaryColumns ++= Seq(tableColumn.getColumnSchema)
              }
            }
            columnExist = true
          }
        }
        if (!columnExist) {
          throwMetadataException(dbName, tableName,
            s"Column $column does not exists in the table $dbName.$tableName")
        }
      }

      val operationContext = new OperationContext
      // event will be fired before dropping the columns
      val alterTableDropColumnPreEvent: AlterTableDropColumnPreEvent = AlterTableDropColumnPreEvent(
        carbonTable,
        alterTableDropColumnModel,
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableDropColumnPreEvent, operationContext)

      // read the latest schema file
      val tableInfo: org.apache.carbondata.format.TableInfo =
        metastore.getThriftTableInfo(carbonTable)
      // deletedColumnSchema contains parent column schema and child also in case of complex and
      // deletedTableColumns contains only parent columns in case of complex to add in
      // schemaEvolution entry
      var deletedColumnSchema = ListBuffer[org.apache.carbondata.format.ColumnSchema]()
      var deletedTableColumns = ListBuffer[org.apache.carbondata.format.ColumnSchema]()
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala
      alterTableDropColumnModel.columns.foreach { column =>
        columnSchemaList.foreach { columnSchema =>
          if (!columnSchema.invisible) {
            if (column.equalsIgnoreCase(columnSchema.column_name)) {
              val columnSchemaCopy = columnSchema.deepCopy
              deletedTableColumns += columnSchemaCopy
              deletedColumnSchema += columnSchemaCopy
              columnSchema.invisible = true
            } else if (columnSchema.column_name.toLowerCase
              .startsWith(column + CarbonCommonConstants.POINT)) {
              // if the column to be dropped is of complex type then its children are prefixed
              // with -> parent_name + '.'
              deletedColumnSchema += columnSchema.deepCopy
              columnSchema.invisible = true
            }
          }
        }
      }
      // add deleted columns to schema evolution history and update the schema
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
      schemaEvolutionEntry.setRemoved(deletedTableColumns.toList.asJava)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl
      val delCols = deletedColumnSchema.map { deleteCols =>
        schemaConverter.fromExternalToWrapperColumnSchema(deleteCols)
      }
      val tableIdentifier = AlterTableUtil.updateSchemaInfo(
        carbonTable,
        schemaEvolutionEntry,
        tableInfo)(sparkSession)
      AlterTableUtil.deleteColsAndUpdateSchema(carbonTable,
        delCols, tableIdentifier, sparkSession)
      new MockClassForAlterRevertTests().mockForAlterAddColRevertTest()
      // TODO: 1. add check for deletion of index tables

      // event will be fired before dropping the columns
      val alterTableDropColumnPostEvent: AlterTableDropColumnPostEvent =
        AlterTableDropColumnPostEvent(
          carbonTable,
          alterTableDropColumnModel,
          sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableDropColumnPostEvent, operationContext)

      LOGGER.info(s"Alter table for drop columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        if (carbonTable != null) {
          AlterTableUtil.revertDropColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        throwMetadataException(dbName, tableName,
          s"Alter table drop column operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  override protected def opName: String = "ALTER TABLE DROP COLUMN"
}
