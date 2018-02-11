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

package org.apache.spark.sql.execution.command.datamap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, NoSuchDataMapException}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events._

/**
 * Drops the datamap and any related tables associated with the datamap
 * @param dataMapName
 * @param ifExistsSet
 * @param databaseNameOp
 * @param tableName
 */
case class CarbonDropDataMapCommand(
    dataMapName: String,
    ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    forceDrop: Boolean = false)
  extends AtomicRunnableCommand {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  var commandToRun: CarbonDropTableCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK)
    val carbonEnv = CarbonEnv.getInstance(sparkSession)
    val catalog = carbonEnv.carbonMetastore
    val tablePath = CarbonEnv.getTablePath(databaseNameOp, tableName)(sparkSession)
    val tableIdentifier =
      AbsoluteTableIdentifier.from(tablePath, dbName.toLowerCase, tableName.toLowerCase)
    catalog.checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, Some(dbName)))
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      forceDropTableFromMetaStore(sparkSession)
      locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(tableIdentifier, lock)
      }
      LOGGER.audit(s"Deleting datamap [$dataMapName] under table [$tableName]")
      val carbonTable: Option[CarbonTable] = try {
        Some(CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession))
      } catch {
        case ex: NoSuchTableException =>
          throw ex
      }
      // If datamap to be dropped in parent table then drop the datamap from metastore and remove
      // entry from parent table.
      // If force drop is true then remove the datamap from hivemetastore. No need to remove from
      // parent as the first condition would have taken care of it.
      if (carbonTable.isDefined && carbonTable.get.getTableInfo.getDataMapSchemaList.size() > 0) {
        val dataMapSchema = carbonTable.get.getTableInfo.getDataMapSchemaList.asScala.zipWithIndex.
          find(_._1.getDataMapName.equalsIgnoreCase(dataMapName))
        if (dataMapSchema.isDefined) {
          val operationContext = new OperationContext
          val dropDataMapPreEvent =
            DropDataMapPreEvent(
              Some(dataMapSchema.get._1),
              ifExistsSet,
              sparkSession)
          OperationListenerBus.getInstance.fireEvent(dropDataMapPreEvent, operationContext)
          carbonTable.get.getTableInfo.getDataMapSchemaList.remove(dataMapSchema.get._2)
          val schemaConverter = new ThriftWrapperSchemaConverterImpl
          PreAggregateUtil.updateSchemaInfo(
            carbonTable.get,
            schemaConverter.fromWrapperToExternalTableInfo(
              carbonTable.get.getTableInfo,
              dbName,
              tableName))(sparkSession)
          commandToRun = CarbonDropTableCommand(
            ifExistsSet = true,
            Some(dataMapSchema.get._1.getRelationIdentifier.getDatabaseName),
            dataMapSchema.get._1.getRelationIdentifier.getTableName,
            dropChildTable = true
          )
          commandToRun.processMetadata(sparkSession)
          // fires the event after dropping datamap from main table schema
          val dropDataMapPostEvent =
            DropDataMapPostEvent(
              Some(dataMapSchema.get._1),
              ifExistsSet,
              sparkSession)
          OperationListenerBus.getInstance.fireEvent(dropDataMapPostEvent, operationContext)
        } else if (!ifExistsSet) {
          throw new NoSuchDataMapException(dataMapName, tableName)
        }
      } else if (carbonTable.isDefined &&
        carbonTable.get.getTableInfo.getDataMapSchemaList.size() == 0) {
        if (!ifExistsSet) {
          throw new NoSuchDataMapException(dataMapName, tableName)
        }
      }
    } catch {
      case e: NoSuchDataMapException =>
        throw e
      case ex: Exception =>
        LOGGER.error(ex, s"Dropping datamap $dataMapName failed")
        throwMetadataException(dbName, tableName,
          s"Dropping datamap $dataMapName failed: ${ex.getMessage}")
    }
    finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          LOGGER.info("Table MetaData Unlocked Successfully")
        }
      }
    }
    Seq.empty
  }

  /**
   * Used to drop child datamap from hive metastore if it exists.
   * forceDrop will be true only when an exception occurs in main table status updation for
   * parent table or in processData from CreatePreAggregateTableCommand.
   */
  private def forceDropTableFromMetaStore(sparkSession: SparkSession): Unit = {
    if (forceDrop) {
      val childTableName = tableName + "_" + dataMapName
      LOGGER.info(s"Trying to force drop $childTableName from metastore")
      val childCarbonTable: Option[CarbonTable] = try {
        Some(CarbonEnv.getCarbonTable(databaseNameOp, childTableName)(sparkSession))
      } catch {
        case _: Exception =>
          LOGGER.warn(s"Child table $childTableName not found in metastore")
          None
      }
      if (childCarbonTable.isDefined) {
        commandToRun = CarbonDropTableCommand(
          ifExistsSet = true,
          Some(childCarbonTable.get.getDatabaseName),
          childCarbonTable.get.getTableName,
          dropChildTable = true)
        commandToRun.processMetadata(sparkSession)
      }
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    if (commandToRun != null) {
      DataMapStoreManager.getInstance().clearDataMap(
        commandToRun.carbonTable.getAbsoluteTableIdentifier, dataMapName)
      commandToRun.processData(sparkSession)
    }
    Seq.empty
  }

}
