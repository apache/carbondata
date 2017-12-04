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
import scala.collection.mutable.Map

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.TablePropertyInfo
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.StructField
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, MalformedCarbonCommandException, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.{AlterTableAddColumnPostEvent, AlterTableAddColumnPreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.spark.rdd.{AlterTableAddColumnRDD, AlterTableDropColumnRDD}

private[sql] case class CarbonAlterTableAddColumnCommand(
    databaseNameOp: Option[String],
    tableName: String,
    newFields: Seq[StructField],
    newTableProperties: Map[String, String])
  extends MetadataCommand {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    LOGGER.audit(s"Alter table add columns request has been received for $dbName.$tableName")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var newCols = Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]()
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil.validateTableAndAcquireLock(
        dbName, tableName, locksToBeAcquired)(sparkSession)
      // Consider a concurrent scenario where 2 alter operations are executed in parallel. 1st
      // operation is success and updates the schema file. 2nd operation will get the lock after
      // completion of 1st operation but as look up relation is called before it will have the
      // older carbon table and this can lead to inconsistent state in the system. Therefor look
      // up relation should be called after acquiring the lock
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      if (!carbonTable.getTableInfo.getParentRelationIdentifiers.isEmpty) {
        throw new MalformedCarbonCommandException(
          "Alter table add column is not supported for table with datamap created")
      }
      val operationContext = new OperationContext
      val alterTableAddColumnListener = AlterTableAddColumnPreEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance().fireEvent(alterTableAddColumnListener, operationContext)
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(
        carbonTable.getAbsoluteTableIdentifier)
      val thriftTableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        dbName,
        tableName,
        carbonTable.getTablePath)

      val numColumns = carbonTable.getAllDimensions.size() + carbonTable.getAllMeasures.size
      newFields.zipWithIndex.foreach { case (field, index) =>
          field.setSchemaOrdinal(numColumns + index)
      }
      newCols = createNewFields(
        carbonTable,
        newFields,
        newTableProperties.asJava,
        wrapperTableInfo)

      // generate dictionary files for the newly added columns
      // TODO: this RDD is not required, do it in driver dirently
      new AlterTableAddColumnRDD(
        sparkSession.sparkContext,
        newCols,
        carbonTable.getAbsoluteTableIdentifier
      ).collect()

      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(timeStamp)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val entry = schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry)
      val thriftTable =
        schemaConverter.fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)

      AlterTableUtil.updateSchemaInfo(carbonTable, entry, thriftTable)(sparkSession)
      val alterTablePostExecutionEvent: AlterTableAddColumnPostEvent =
        AlterTableAddColumnPostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(alterTablePostExecutionEvent, operationContext)
      LOGGER.info(s"Alter table for add columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for add columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error(e, "Alter table add columns failed")
        if (newCols.nonEmpty) {
          LOGGER.info("Cleaning up the dictionary files as alter table add operation failed")
          new AlterTableDropColumnRDD(
            sparkSession.sparkContext,
            newCols,
            carbonTable.getAbsoluteTableIdentifier
          ).collect()
          AlterTableUtil.revertAddColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table add operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  /**
   * Create the ColumnSchema for `newFields` and add them them to `carbonTable`
   */
  def createNewFields(
      carbonTable: CarbonTable,
      newFields: Seq[StructField],
      alterTableProperty: java.util.Map[String, String],
      tableInfo: TableInfo): Seq[ColumnSchema] = {

    // create ColumnSchema for new fields from alter table add column, and construct a new list
    // of all columns, set it in TableInfo and flush it to metastore

    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val existingTableProperty = tableInfo.getFactTable.getTableProperties
    val sortColumns = carbonTable.getSortColumns
    val tableProperty = new TablePropertyInfo(
      newFields.asJava, sortColumns, existingTableProperty, alterTableProperty)
    var newColumns = Seq[ColumnSchema]()

    newFields.foreach { field =>
      // child table does not support alter table, so parent table must be null
      val columnSchema = field.createColumnSchema(tableProperty, null, null)
      newColumns ++= columnSchema.asScala
    }

    // This part will create dictionary file for all newly added dictionary columns
    // if valid default value is provided,
    // then that value will be included while creating dictionary file
    val defaultValueString = "default.value."
    newColumns.foreach { column =>
      var rawData: String = null
      alterTableProperty.asScala.foreach { elem =>
        if (elem._1.toLowerCase.startsWith(defaultValueString)) {
          if (column.getColumnName.equalsIgnoreCase(elem._1.substring(defaultValueString.length))) {
            rawData = elem._2
            val data = DataTypeUtil.convertDataToBytesBasedOnDataType(elem._2, column)
            if (null != data) {
              column.setDefaultValue(data)
            } else {
              LOGGER.error(
                s"Invalid default value for new column " +
                s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }, " +
                s"${ column.getColumnName } : ${ elem._2 }")
            }
          }
        }
        else if (elem._1.equalsIgnoreCase("no_inverted_index") &&
                 elem._2.split(",").contains(column.getColumnName)) {
          column.getEncodingList.remove(Encoding.INVERTED_INDEX)
        }
      }
    }

    val existingColumns = tableInfo.getFactTable.getListOfColumns.asScala
    val existingSortColumns = existingColumns.filter(_.isSortColumn)
    val dimension = existingColumns.filter(c => c.isDimensionColumn && !c.isSortColumn)
    val measure = existingColumns.filter(c => !c.isDimensionColumn && !c.isSortColumn)

    val updatedColumns = existingSortColumns ++
                         newColumns.filter(_.isSortColumn) ++
                         dimension ++
                         newColumns.filter(c => c.isDimensionColumn && !c.isSortColumn) ++
                         measure ++
                         newColumns.filter(c => !c.isDimensionColumn && !c.isSortColumn)

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    updatedColumns
      .filter(!_.isInvisible)
      .groupBy(_.getColumnName)
      .foreach { case (columnName, columns) =>
        if (columns.size > 1) {
          val name = columnName
          LOGGER.error(s"Duplicate column found with name: $name")
          LOGGER.audit(
            s"Validation failed for Create/Alter Table Operation " +
            s"for ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }. " +
            s"Duplicate column found with name: $name")
          sys.error(s"Duplicate column found with name: $name")
        }
      }

    tableInfo.getFactTable.setListOfColumns(updatedColumns.asJava)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    newColumns
  }

}
