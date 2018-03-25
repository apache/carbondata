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

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, NoSuchDataMapException}
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events._

/**
 * Drops the datamap and any related tables associated with the datamap
 * @param dataMapName
 * @param ifExistsSet
 * @param table
 */
case class CarbonDropDataMapCommand(
    dataMapName: String,
    ifExistsSet: Boolean,
    table: Option[TableIdentifier],
    forceDrop: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private var dataMapProvider: DataMapProvider = _
  var mainTable: CarbonTable = _
  var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (table.isDefined) {
      val databaseNameOp = table.get.database
      val tableName = table.get.table
      val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
      val locksToBeAcquired = List(LockUsage.METADATA_LOCK)
      val carbonEnv = CarbonEnv.getInstance(sparkSession)
      val catalog = carbonEnv.carbonMetastore
      val tablePath = CarbonEnv.getTablePath(databaseNameOp, tableName)(sparkSession)
      val tableIdentifier =
        AbsoluteTableIdentifier.from(tablePath, dbName.toLowerCase, tableName.toLowerCase)
      catalog.checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, Some(dbName)))
      if (mainTable == null) {
        mainTable = try {
          CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
        } catch {
          case ex: NoSuchTableException =>
            throwMetadataException(dbName, tableName,
              s"Dropping datamap $dataMapName failed: ${ ex.getMessage }")
            null
        }
      }
      if (forceDrop && mainTable != null && dataMapSchema != null) {
        if (dataMapSchema != null) {
          dataMapProvider = DataMapManager.get.getDataMapProvider(dataMapSchema, sparkSession)
        }
        // TODO do a check for existance before dropping
        dataMapProvider.freeMeta(mainTable, dataMapSchema)
        return Seq.empty
      }
      val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
      try {
        locksToBeAcquired foreach {
          lock => carbonLocks += CarbonLockUtil.getLockObject(tableIdentifier, lock)
        }
        LOGGER.audit(s"Deleting datamap [$dataMapName] under table [$tableName]")
        // If datamap to be dropped in parent table then drop the datamap from metastore and remove
        // entry from parent table.
        // If force drop is true then remove the datamap from hivemetastore. No need to remove from
        // parent as the first condition would have taken care of it.
        if (mainTable != null && mainTable.getTableInfo.getDataMapSchemaList.size() > 0) {
          val dataMapSchemaOp = mainTable.getTableInfo.getDataMapSchemaList.asScala.zipWithIndex.
            find(_._1.getDataMapName.equalsIgnoreCase(dataMapName))
          if (dataMapSchemaOp.isDefined) {
            dataMapSchema = dataMapSchemaOp.get._1
            val operationContext = new OperationContext
            val dropDataMapPreEvent =
              DropDataMapPreEvent(
                Some(dataMapSchema),
                ifExistsSet,
                sparkSession)
            OperationListenerBus.getInstance.fireEvent(dropDataMapPreEvent, operationContext)
            mainTable.getTableInfo.getDataMapSchemaList.remove(dataMapSchemaOp.get._2)
            val schemaConverter = new ThriftWrapperSchemaConverterImpl
            PreAggregateUtil.updateSchemaInfo(
              mainTable,
              schemaConverter.fromWrapperToExternalTableInfo(
                mainTable.getTableInfo,
                dbName,
                tableName))(sparkSession)
            if (dataMapProvider == null) {
              dataMapProvider = DataMapManager.get.getDataMapProvider(dataMapSchema, sparkSession)
            }
            dataMapProvider.freeMeta(mainTable, dataMapSchema)

            // fires the event after dropping datamap from main table schema
            val dropDataMapPostEvent =
              DropDataMapPostEvent(
                Some(dataMapSchema),
                ifExistsSet,
                sparkSession)
            OperationListenerBus.getInstance.fireEvent(dropDataMapPostEvent, operationContext)
          } else if (!ifExistsSet) {
            throw new NoSuchDataMapException(dataMapName, tableName)
          }
        } else if (mainTable != null &&
                   mainTable.getTableInfo.getDataMapSchemaList.size() == 0) {
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
            s"Dropping datamap $dataMapName failed: ${ ex.getMessage }")
      }
      finally {
        if (carbonLocks.nonEmpty) {
          val unlocked = carbonLocks.forall(_.unlock())
          if (unlocked) {
            LOGGER.info("Table MetaData Unlocked Successfully")
          }
        }
      }
    } else {
      if (dataMapSchema == null) {
        val schema = DataMapStoreManager.getInstance().getAllDataMapSchemas.asScala.find{dm =>
          dm.getDataMapName.equalsIgnoreCase(dataMapName)
        }
        dataMapSchema = schema match {
          case Some(dmSchema) => dmSchema
          case _ => null
        }
      }
      if (dataMapSchema != null) {
        // TODO do a check for existance before dropping
        dataMapProvider = DataMapManager.get.getDataMapProvider(dataMapSchema, sparkSession)
        dataMapProvider.freeMeta(mainTable, dataMapSchema)
      } else if (!ifExistsSet) {
        new MalformedCarbonCommandException(s"DataMap $dataMapName does not exist")
      }
    }

      Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    if (dataMapProvider != null) {
      dataMapProvider.freeData(mainTable, dataMapSchema)
    }
    Seq.empty
  }

}
