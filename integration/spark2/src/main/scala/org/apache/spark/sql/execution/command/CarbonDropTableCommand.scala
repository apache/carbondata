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

package org.apache.spark.sql.execution.command

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, GetDB, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.ListenerBus
import org.apache.carbondata.events.DropTablePreEvent

case class CarbonDropTableCommand(
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
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    val carbonEnv = CarbonEnv.getInstance(sparkSession)
    val catalog = carbonEnv.carbonMetastore
    val tableIdentifier =
      AbsoluteTableIdentifier.from(CarbonEnv.getInstance(sparkSession).storePath,
        dbName.toLowerCase, tableName.toLowerCase)
    catalog.checkSchemasModifiedTimeAndReloadTables(tableIdentifier.getStorePath)
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(carbonTableIdentifier, lock)
      }
      LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")

      //fires the event before dropping main table
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val carbonTable = metastore
        .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .tableMeta.carbonTable
      val dropTablePostEvent: DropTablePreEvent =
        DropTablePreEvent(
          carbonTable,
          ifExistsSet,
          sparkSession)
      ListenerBus.getInstance.fireEvent(dropTablePostEvent)

      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .dropTable(tableIdentifier.getTablePath, identifier)(sparkSession)
      LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")
    } catch {
      case ex: Exception =>
        LOGGER.error(ex, s"Dropping table $dbName.$tableName failed")
        sys.error(s"Dropping table $dbName.$tableName failed: ${ex.getMessage}")
    } finally {
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
    val tableIdentifier =
      AbsoluteTableIdentifier.from(CarbonEnv.getInstance(sparkSession).storePath, dbName, tableName)
    val metadataFilePath =
      CarbonStorePath.getCarbonTablePath(tableIdentifier).getMetadataDirectoryPath
    val fileType = FileFactory.getFileType(metadataFilePath)
    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
    }
    Seq.empty
  }
}
