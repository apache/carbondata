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

package org.apache.spark.sql.execution.command.partition

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{ExecutorService, Executors, Future}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{AlterTableUtil, PartitionUtils}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, PostAlterTableHivePartitionCommandEvent, PreAlterTableHivePartitionCommandEvent}
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.partition.SplitPartitionCallable

/**
 * Command for Alter Table Add & Split partition
 * Add is a special case of Splitting the default partition (part0)
 */
case class CarbonAlterTableSplitPartitionCommand(
    splitPartitionModel: AlterTableSplitPartitionModel)
  extends AtomicRunnableCommand {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
  private val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val dbName = splitPartitionModel.databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val tableName = splitPartitionModel.tableName
    val relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val tablePath = relation.carbonTable.getTablePath
    if (relation == null) {
      throwMetadataException(dbName, tableName, "table not found")
    }
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, Some(dbName)))
    if (null == (CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession))) {
      LOGGER.error(s"Alter table failed. table not found: $dbName.$tableName")
      throwMetadataException(dbName, tableName, "table not found")
    }
    val carbonTable = relation.carbonTable
    val partitionInfo = carbonTable.getPartitionInfo(tableName)
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)

    if (partitionInfo == null) {
      throwMetadataException(dbName, tableName, "Table is not a partition table.")
    }
    if (partitionInfo.getPartitionType == PartitionType.HASH) {
      throwMetadataException(dbName, tableName, "Hash partition table cannot be added or split!")
    }

    updatePartitionInfo(partitionInfo, partitionIds)

    // read TableInfo
    val tableInfo = carbonMetaStore.getThriftTableInfo(carbonTable)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo,
      dbName, tableName, tablePath)
    val tableSchema = wrapperTableInfo.getFactTable
    tableSchema.setPartitionInfo(partitionInfo)
    wrapperTableInfo.setFactTable(tableSchema)
    wrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    val thriftTable =
      schemaConverter.fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
    carbonMetaStore.updateTableSchemaForAlter(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
      thriftTable,
      null,
      carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime()
    Seq.empty
  }

  private def updatePartitionInfo(partitionInfo: PartitionInfo, partitionIds: List[Int]): Unit = {
    val dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))

    val timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))

    PartitionUtils.updatePartitionInfo(
      partitionInfo,
      partitionIds,
      splitPartitionModel.partitionId.toInt,
      splitPartitionModel.splitInfo,
      timestampFormatter,
      dateFormatter)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val dbName = splitPartitionModel.databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val tableName = splitPartitionModel.tableName
    var locks = List.empty[ICarbonLock]
    var success = false
    try {
      val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
        LockUsage.COMPACTION_LOCK,
        LockUsage.DELETE_SEGMENT_LOCK,
        LockUsage.DROP_TABLE_LOCK,
        LockUsage.CLEAN_FILES_LOCK,
        LockUsage.ALTER_PARTITION_LOCK)
      locks = AlterTableUtil.validateTableAndAcquireLock(dbName, tableName,
        locksToBeAcquired)(sparkSession)
      val carbonLoadModel = new CarbonLoadModel()
      val table = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val tablePath = table.getTablePath
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(table.getTableName)
      carbonLoadModel.setDatabaseName(table.getDatabaseName)
      carbonLoadModel.setCarbonTransactionalTable(table.isTransactionalTable)
      carbonLoadModel.setTablePath(tablePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      val operationContext = new OperationContext
      val preAlterTableHivePartitionCommandEvent = PreAlterTableHivePartitionCommandEvent(
        sparkSession,
        table)
      OperationListenerBus.getInstance()
        .fireEvent(preAlterTableHivePartitionCommandEvent, operationContext)
      alterTableSplitPartition(
        sparkSession.sqlContext,
        splitPartitionModel.partitionId.toInt.toString,
        carbonLoadModel,
        oldPartitionIds.asScala.toList
      )
      val postAlterTableHivePartitionCommandEvent = PostAlterTableHivePartitionCommandEvent(
        sparkSession,
        table)
      OperationListenerBus.getInstance()
        .fireEvent(postAlterTableHivePartitionCommandEvent, operationContext)
      success = true
    } catch {
      case e: Exception =>
        success = false
        sys.error(s"Add/Split Partition failed. Please check logs for more info. ${ e.getMessage }")
    } finally {
      AlterTableUtil.releaseLocks(locks)
      CacheProvider.getInstance().dropAllCache()
      val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
      LOGGER.info("Locks released after alter table add/split partition action.")
      if (success) {
        LOGGER.info(s"Alter table add/split partition is successful for table $dbName.$tableName")
        LOGGER.audit(s"Alter table add/split partition is successful for table $dbName.$tableName")
      }
    }
    Seq.empty
  }

  private def alterTableSplitPartition(
      sqlContext: SQLContext,
      partitionId: String,
      carbonLoadModel: CarbonLoadModel,
      oldPartitionIdList: List[Int]
  ): Unit = {
    LOGGER.audit(s"Add partition request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    try {
      startSplitThreads(sqlContext,
        carbonLoadModel,
        partitionId,
        oldPartitionIdList)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in start splitting partition thread. ${ e.getMessage }")
        throw e
    }
  }

  private def startSplitThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      partitionId: String,
      oldPartitionIdList: List[Int]): Unit = {
    val numberOfCores = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.NUM_CORES_ALT_PARTITION,
        CarbonCommonConstants.DEFAULT_NUMBER_CORES)
    val executor : ExecutorService = Executors.newFixedThreadPool(numberOfCores.toInt)
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val segmentStatusManager = new SegmentManager()
      val validSegments =
        segmentStatusManager.getValidSegments(absoluteTableIdentifier).getValidSegments.asScala
      val threadArray: Array[SplitThread] = new Array[SplitThread](validSegments.size)
      var i = 0
      validSegments.foreach { segmentId =>
        threadArray(i) = SplitThread(sqlContext, carbonLoadModel, executor,
          segmentId.getSegmentNo, partitionId, oldPartitionIdList)
        threadArray(i).start()
        i += 1
      }
      threadArray.foreach {
        thread => thread.join()
      }
      val refresher = DataMapStoreManager.getInstance().getTableSegmentRefresher(carbonTable)
      refresher.refreshSegments(validSegments.map(_.getSegmentNo).asJava)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception when split partition: ${ e.getMessage }")
        throw e
    } finally {
      executor.shutdown()
      try {
        TableProcessingOperations
          .deletePartialLoadDataIfExist(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
            false)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in add/split partition thread while deleting partial load file" +
                       s" ${ e.getMessage }")
      }
    }
  }
}

case class SplitThread(sqlContext: SQLContext,
    carbonLoadModel: CarbonLoadModel,
    executor: ExecutorService,
    segmentId: String,
    partitionId: String,
    oldPartitionIdList: List[Int]) extends Thread {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

  override def run(): Unit = {
    var triggeredSplitPartitionStatus = false
    var exception: Exception = null
    try {
      executePartitionSplit(sqlContext,
        carbonLoadModel, executor, segmentId, partitionId, oldPartitionIdList)
      triggeredSplitPartitionStatus = true
    } catch {
      case e: Exception =>
        val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
        LOGGER.error(s"Exception in partition split thread: ${ e.getMessage } }")
        exception = e
    }
    if (!triggeredSplitPartitionStatus) {
      throw new Exception("Exception in split partition " + exception.getMessage)
    }
  }

  private def executePartitionSplit( sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segment: String,
      partitionId: String,
      oldPartitionIdList: List[Int]): Unit = {
    val futureList: util.List[Future[Void]] = new util.ArrayList[Future[Void]](
      CarbonCommonConstants.DEFAULT_COLLECTION_SIZE
    )
    scanSegmentsForSplitPartition(futureList, executor, segment, partitionId,
      sqlContext, carbonLoadModel, oldPartitionIdList)
    try {
      futureList.asScala.foreach { future =>
        future.get
      }
    } catch {
      case e: Exception =>
        LOGGER.error(e, s"Exception in partition split thread ${ e.getMessage }")
        throw e
    }
  }

  private def scanSegmentsForSplitPartition(futureList: util.List[Future[Void]],
      executor: ExecutorService,
      segmentId: String,
      partitionId: String,
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      oldPartitionIdList: List[Int]): Unit = {

    val splitModel = SplitPartitionCallableModel(carbonLoadModel,
      segmentId,
      partitionId,
      oldPartitionIdList,
      sqlContext)

    val future: Future[Void] = executor.submit(new SplitPartitionCallable(splitModel))
    futureList.add(future)
  }
}
