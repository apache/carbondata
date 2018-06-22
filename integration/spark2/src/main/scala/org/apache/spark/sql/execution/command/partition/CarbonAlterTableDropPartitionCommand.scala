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

import java.util
import java.util.concurrent.{ExecutorService, Executors, Future}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{SegmentManager, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.partition.DropPartitionCallable

case class CarbonAlterTableDropPartitionCommand(
    model: AlterTableDropPartitionModel)
  extends AtomicRunnableCommand {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
  private val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (model.partitionId.equals("0")) {
      sys.error(s"Cannot drop default partition! Please use delete statement!")
    }
    val dbName = model.databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val tableName = model.tableName
    val carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val tablePath = relation.carbonTable.getTablePath
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, Some(dbName)))
    if (relation == null || CarbonMetadata.getInstance.getCarbonTable(dbName, tableName) == null) {
      throwMetadataException(dbName, tableName, "table not found")
    }
    val carbonTable = relation.carbonTable
    val partitionInfo = carbonTable.getPartitionInfo(tableName)
    if (partitionInfo == null) {
      throwMetadataException(dbName, tableName, "table is not a partition table")
    }
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)
    val partitionIndex = partitionIds.indexOf(Integer.valueOf(model.partitionId))
    partitionInfo.getPartitionType match {
      case PartitionType.HASH => sys.error(s"Hash partition cannot be dropped!")
      case PartitionType.RANGE =>
        val rangeInfo = new util.ArrayList(partitionInfo.getRangeInfo)
        val rangeToRemove = partitionInfo.getRangeInfo.get(partitionIndex - 1)
        rangeInfo.remove(rangeToRemove)
        partitionInfo.setRangeInfo(rangeInfo)
      case PartitionType.LIST =>
        val listInfo = new util.ArrayList(partitionInfo.getListInfo)
        val listToRemove = partitionInfo.getListInfo.get(partitionIndex - 1)
        listInfo.remove(listToRemove)
        partitionInfo.setListInfo(listInfo)
      case PartitionType.RANGE_INTERVAL =>
        throwMetadataException(dbName, tableName,
          "Dropping range interval partition is unsupported")
    }
    partitionInfo.dropPartition(partitionIndex)

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
    thriftTable.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis)
    carbonMetaStore.updateTableSchemaForAlter(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
      thriftTable,
      null,
      carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime()
    // sparkSession.catalog.refreshTable(tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    val dbName = model.databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val tableName = model.tableName
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
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(table.getTableName)
      carbonLoadModel.setCarbonTransactionalTable(table.isTransactionalTable)
      carbonLoadModel.setDatabaseName(table.getDatabaseName)
      carbonLoadModel.setTablePath(table.getTablePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      alterTableDropPartition(
        sparkSession.sqlContext,
        model.partitionId,
        carbonLoadModel,
        model.dropWithData,
        oldPartitionIds.asScala.toList
      )
      success = true
    } catch {
      case e: Exception =>
        sys.error(s"Drop Partition failed. Please check logs for more info. ${ e.getMessage } ")
        success = false
    } finally {
      CacheProvider.getInstance().dropAllCache()
      AlterTableUtil.releaseLocks(locks)
      LOGGER.info("Locks released after alter table drop partition action.")
    }
    LOGGER.info(s"Alter table drop partition is successful for table $dbName.$tableName")
    LOGGER.audit(s"Alter table drop partition is successful for table $dbName.$tableName")
    Seq.empty
  }

  private def alterTableDropPartition(sqlContext: SQLContext,
      partitionId: String,
      carbonLoadModel: CarbonLoadModel,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    LOGGER.audit(s"Drop partition request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    try {
      startDropThreads(
        sqlContext,
        carbonLoadModel,
        partitionId,
        dropWithData,
        oldPartitionIds)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in start dropping partition thread. ${ e.getMessage }")
        throw e
    }
  }

  private def startDropThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      partitionId: String,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    val numberOfCores = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.NUM_CORES_ALT_PARTITION,
        CarbonCommonConstants.DEFAULT_NUMBER_CORES)
    val executor : ExecutorService = Executors.newFixedThreadPool(numberOfCores.toInt)
    try {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val segmentStatusManager = new SegmentManager()
      val validSegments =
        segmentStatusManager.getValidSegments(absoluteTableIdentifier).getValidSegments.asScala
      val threadArray: Array[Thread] = new Array[Thread](validSegments.size)
      var i = 0
      for (segmentId: Segment <- validSegments) {
        threadArray(i) = dropPartitionThread(sqlContext, carbonLoadModel, executor,
          segmentId, partitionId, dropWithData, oldPartitionIds)
        threadArray(i).start()
        i += 1
      }
      for (thread <- threadArray) {
        thread.join()
      }
      val refresher = DataMapStoreManager.getInstance().getTableSegmentRefresher(carbonTable)
      refresher.refreshSegments(validSegments.map(_.getSegmentNo).asJava)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception when dropping partition: ${ e.getMessage }")
    } finally {
      executor.shutdown()
      try {
        TableProcessingOperations
          .deletePartialLoadDataIfExist(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
            false)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in dropping partition thread while deleting partial load file" +
                       s" ${ e.getMessage }")
      }
    }
  }
}

case class dropPartitionThread(sqlContext: SQLContext,
    carbonLoadModel: CarbonLoadModel,
    executor: ExecutorService,
    segmentId: Segment,
    partitionId: String,
    dropWithData: Boolean,
    oldPartitionIds: List[Int]) extends Thread {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

  override def run(): Unit = {
    try {
      executeDroppingPartition(sqlContext, carbonLoadModel, executor,
        segmentId, partitionId, dropWithData, oldPartitionIds)
    } catch {
      case e: Exception =>
        val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
        LOGGER.error(s"Exception in dropping partition thread: ${ e.getMessage } }")
    }
  }

  private def executeDroppingPartition(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segmentId: Segment,
      partitionId: String,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val model = new DropPartitionCallableModel(carbonLoadModel,
      segmentId, partitionId, oldPartitionIds, dropWithData, carbonTable, sqlContext)
    val future: Future[Void] = executor.submit(new DropPartitionCallable(model))
    try {
      future.get
    } catch {
      case e: Exception =>
        LOGGER.error(e, s"Exception in partition drop thread ${ e.getMessage }")
        throw e
    }
  }
}
