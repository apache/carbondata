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

package org.apache.spark.sql.execution.command.management

import java.lang.Boolean

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, Checker}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.cleanfiles.CleanFilesUtil
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.view.MVManagerInSpark

/**
 * Clean data in table
 * If table name is specified and forceTableClean is false, it will clean garbage
 * segment (MARKED_FOR_DELETE state).
 * If table name is specified and forceTableClean is true, it will delete all data
 * in the table.
 * If table name is not provided, it will clean garbage segment in all tables.
 */
case class CarbonCleanFilesCommand(
    databaseNameOp: Option[String],
    tableName: Option[String],
    options: Option[List[(String, String)]],
    forceTableClean: Boolean = false,
    isInternalCleanCall: Boolean = false,
    truncateTable: Boolean = false)
  extends AtomicRunnableCommand {

  var carbonTable: CarbonTable = _
  var cleanFileCommands: List[CarbonCleanFilesCommand] = List.empty
  val optionsMap = options.getOrElse(List.empty[(String, String)]).toMap
  val isDryRun = optionsMap.getOrElse("isdryrun", "false").toBoolean
  val dryRun = "isDryRun"
  val forceTrashClean = optionsMap.getOrElse("force", "false").toBoolean

  override def output: Seq[Attribute] = {
    if (isDryRun && tableName.isDefined) {
      Seq(
        AttributeReference("TABLE NAME", StringType, nullable = true)(),
        AttributeReference("SEGMENT NUMBER", StringType, nullable = true)(),
        AttributeReference("SEGMENT STATUS", StringType, nullable = true)(),
        AttributeReference("SEGMENT LOCATION", StringType, nullable = false)(),
        AttributeReference("ACTION", StringType, nullable = false)(),
        AttributeReference("EXPIRATION TIME", StringType, nullable = false)())
    } else {
      Seq.empty
    }
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName.get)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map(
      "force" -> forceTableClean.toString,
      "internal" -> isInternalCleanCall.toString))
    val viewSchemas =
      MVManagerInSpark.get(sparkSession).getSchemasOnTable(carbonTable)
    if (!viewSchemas.isEmpty) {
      val commands = viewSchemas.asScala.map {
        schema =>
          val relationIdentifier = schema.getIdentifier
          CarbonCleanFilesCommand(
            Some(relationIdentifier.getDatabaseName),
            Some(relationIdentifier.getTableName),
            options,
            isInternalCleanCall = true)
      }.toList
      commands.foreach(_.processMetadata(sparkSession))
      cleanFileCommands = cleanFileCommands ++ commands
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (!isDryRun) {
      cleanFilesOperation(sparkSession)
    } else if (isDryRun && tableName.isDefined) {
      // dry run, do not clean anything and do not delete trash too
      CleanFilesUtil.cleanFilesDryRun(carbonTable, sparkSession)
    } else {
      Seq.empty
    }
  }

  // actual clean files operation of the carbonTable
  def cleanFilesOperation(sparkSession: SparkSession): Seq[Row] = {
    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "clean file")
    }
    val operationContext = new OperationContext
    val cleanFilesPreEvent: CleanFilesPreEvent = CleanFilesPreEvent(carbonTable, sparkSession)
    OperationListenerBus.getInstance.fireEvent(cleanFilesPreEvent, operationContext)
    if (tableName.isDefined) {
      Checker.validateTableExists(databaseNameOp, tableName.get, sparkSession)
      if (forceTrashClean) {
        CleanFilesUtil.deleteDataFromTrashFolder(carbonTable, sparkSession)
      } else {
        // clear trash based on timestamp
        CleanFilesUtil.deleteStaleDataFromTrash(carbonTable, sparkSession)
      }
      // delete partial load and send them to trash
      TableProcessingOperations
        .deletePartialLoadDataIfExist(carbonTable, false)
      if (forceTableClean) {
        deleteAllData(sparkSession, databaseNameOp, tableName.get)
      } else {
        cleanGarbageData(sparkSession, databaseNameOp, tableName.get)
      }
      // clean stash in metadata folder too
      deleteStaleSegmentFiles(carbonTable)
    } else {
      cleanGarbageDataInAllTables(sparkSession)
    }
    if (cleanFileCommands != null) {
      cleanFileCommands.foreach(_.processData(sparkSession))
    }
    val cleanFilesPostEvent: CleanFilesPostEvent =
      CleanFilesPostEvent(carbonTable, sparkSession)
    OperationListenerBus.getInstance.fireEvent(cleanFilesPostEvent, operationContext)
    Seq.empty
  }

  // This method deletes the stale segment files in the metadata/segment folder.
  def deleteStaleSegmentFiles(carbonTable: CarbonTable): Unit = {
    val tableStatusLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
    val carbonLoadModel = new CarbonLoadModel
    try {
      if (tableStatusLock.lockWithRetries()) {
        val tableStatusFilePath = CarbonTablePath
          .getTableStatusFilePath(carbonTable.getTablePath)
        val tableStatusFileContent = SegmentStatusManager.readTableStatusFile(tableStatusFilePath)
        val loadMetaDataDetails = tableStatusFileContent.filter(details => details
          .getSegmentStatus == SegmentStatus.SUCCESS || details.getSegmentStatus == SegmentStatus
          .LOAD_PARTIAL_SUCCESS).sortWith(_.getLoadName < _.getLoadName)
        carbonLoadModel.setLoadMetadataDetails(loadMetaDataDetails.toList.asJava)
      } else {
        throw new ConcurrentOperationException(carbonTable.getDatabaseName,
          carbonTable.getTableName, "table status read", "clean files command")
      }
    } finally {
      tableStatusLock.unlock()
    }
    val loadMetaDataDetails = carbonLoadModel.getLoadMetadataDetails.asScala
    val segmentFileList = loadMetaDataDetails.map(f => CarbonTablePath.getSegmentFilesLocation(
      carbonTable.getTablePath) + CarbonCommonConstants.FILE_SEPARATOR + f.getSegmentFile)

    val metaDataPath = CarbonTablePath.getMetadataPath(carbonTable.getTablePath) +
      CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath.SEGMENTS_METADATA_FOLDER

    val segmentListFromMetadataFOlder = FileFactory.getFolderList(metaDataPath).asScala
      .map(f => f.getAbsolutePath).toList
    if (segmentListFromMetadataFOlder.length > loadMetaDataDetails.length) {
      // stale metadata file exists, need to delete those, no need to add them to trash
      segmentListFromMetadataFOlder.foreach(file =>
        if (!segmentFileList.contains(file)) {
          // delete the stale file
          FileFactory.deleteFile(file)
        })
    }
  }

  private def deleteAllData(sparkSession: SparkSession,
      databaseNameOp: Option[String], tableName: String): Unit = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    CleanFilesUtil.cleanFiles(
      dbName = dbName,
      tableName = tableName,
      tablePath = tablePath,
      carbonTable = null, // in case of delete all data carbonTable is not required.
      forceTableClean = forceTableClean)
  }

  private def cleanGarbageData(sparkSession: SparkSession,
      databaseNameOp: Option[String], tableName: String): Unit = {
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    val partitions: Option[Seq[PartitionSpec]] = CarbonFilters.getPartitions(
      Seq.empty[Expression],
      sparkSession,
      carbonTable)
    CleanFilesUtil.cleanFiles(
      dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession),
      tableName = tableName,
      tablePath = carbonTable.getTablePath,
      carbonTable = carbonTable,
      forceTableClean = forceTableClean,
      currentTablePartitions = partitions,
      truncateTable = truncateTable)
  }

  // Clean garbage data in all tables in all databases
  private def cleanGarbageDataInAllTables(sparkSession: SparkSession): Unit = {
    try {
      val databases = sparkSession.sessionState.catalog.listDatabases()
      databases.foreach(dbName => {
        val databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
        CleanFilesUtil.cleanInProgressSegments(databaseLocation, dbName)
      })
    } catch {
      case e: Throwable =>
        // catch all exceptions to avoid failure
        LogServiceFactory.getLogService(this.getClass.getCanonicalName)
          .error("Failed to clean in progress segments", e)
    }
  }

  override protected def opName: String = "CLEAN FILES"
}
