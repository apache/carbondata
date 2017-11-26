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

import org.apache.spark.sql.{CarbonEnv, GetDB, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.{DataProcessCommand, RunnableCommand, SchemaProcessCommand}
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
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
    tableName: String)
  extends RunnableCommand with SchemaProcessCommand with DataProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
    processData(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = GetDB.getDatabaseName(databaseNameOp, sparkSession)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK)
    val carbonEnv = CarbonEnv.getInstance(sparkSession)
    val catalog = carbonEnv.carbonMetastore
    val databaseLocation = GetDB.getDatabaseLocation(dbName, sparkSession,
      CarbonProperties.getStorePath)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName.toLowerCase
    val tableIdentifier =
      AbsoluteTableIdentifier.from(tablePath, dbName.toLowerCase, tableName.toLowerCase)
    catalog.checkSchemasModifiedTimeAndReloadTables()
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(tableIdentifier, lock)
      }
      LOGGER.audit(s"Deleting datamap [$dataMapName] under table [$tableName]")
      var carbonTable: Option[CarbonTable] =
        catalog.getTableFromMetadataCache(dbName, tableName)
      if (carbonTable.isEmpty) {
        try {
          carbonTable = Some(catalog.lookupRelation(identifier)(sparkSession)
            .asInstanceOf[CarbonRelation].metaData.carbonTable)
        } catch {
          case ex: NoSuchTableException =>
            if (!ifExistsSet) {
              throw ex
            }
        }
      }
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
          // fires the event after dropping datamap from main table schema
          val dropDataMapPostEvent =
            DropDataMapPostEvent(
              Some(dataMapSchema.get._1),
              ifExistsSet,
              sparkSession)
          OperationListenerBus.getInstance.fireEvent(dropDataMapPostEvent, operationContext)
        } else if (!ifExistsSet) {
          throw new IllegalArgumentException(
            s"Datamap with name $dataMapName does not exist under table $tableName")
        }
      }

    } catch {
      case ex: Exception =>
        LOGGER.error(ex, s"Dropping datamap $dataMapName failed")
        sys.error(s"Dropping datamap $dataMapName failed: ${ ex.getMessage }")
    }
    finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          logInfo("Table MetaData Unlocked Successfully")
        }
      }
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    val dbName = GetDB.getDatabaseName(databaseNameOp, sparkSession)
    val databaseLocation = GetDB.getDatabaseLocation(dbName, sparkSession,
      CarbonProperties.getStorePath)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName.toLowerCase
    val tableIdentifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableName)
    DataMapStoreManager.getInstance().clearDataMap(tableIdentifier, dataMapName)
    Seq.empty
  }
}
