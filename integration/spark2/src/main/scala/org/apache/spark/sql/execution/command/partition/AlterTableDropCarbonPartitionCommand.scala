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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionModel, DataProcessCommand, RunnableCommand, SchemaProcessCommand}
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonRelation}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory

case class AlterTableDropCarbonPartitionCommand(
    alterTableDropPartitionModel: AlterTableDropPartitionModel)
  extends RunnableCommand with DataProcessCommand with SchemaProcessCommand {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
  val tableName: String = alterTableDropPartitionModel.tableName
  var dbName: String = _
  val partitionId: String = alterTableDropPartitionModel.partitionId
  val dropWithData: Boolean = alterTableDropPartitionModel.dropWithData
  if (partitionId.equals("0")) {
    sys.error(s"Cannot drop default partition! Please use delete statement!")
  }
  var partitionInfo: PartitionInfo = _
  var carbonMetaStore: CarbonMetaStore = _
  var relation: CarbonRelation = _
  var storePath: String = _
  var table: CarbonTable = _
  var carbonTableIdentifier: CarbonTableIdentifier = _
  val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()
  val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
    LockUsage.COMPACTION_LOCK,
    LockUsage.DELETE_SEGMENT_LOCK,
    LockUsage.DROP_TABLE_LOCK,
    LockUsage.CLEAN_FILES_LOCK,
    LockUsage.ALTER_PARTITION_LOCK)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
    processData(sparkSession)
    Seq.empty
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    dbName = alterTableDropPartitionModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    carbonTableIdentifier = relation.tableMeta.carbonTableIdentifier
    storePath = relation.tableMeta.storePath
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTables(storePath)
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      LOGGER.error(s"Alter table failed. table not found: $dbName.$tableName")
      sys.error(s"Alter table failed. table not found: $dbName.$tableName")
    }
    table = relation.tableMeta.carbonTable
    partitionInfo = table.getPartitionInfo(tableName)
    if (partitionInfo == null) {
      sys.error(s"Table $tableName is not a partition table.")
    }
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)
    val partitionIndex = partitionIds.indexOf(Integer.valueOf(partitionId))
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
        sys.error(s"Dropping range interval partition isn't support yet!")
    }
    partitionInfo.dropPartition(partitionIndex)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    // read TableInfo
    val tableInfo = carbonMetaStore.getThriftTableInfo(carbonTablePath)(sparkSession)

    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo,
      dbName, tableName, storePath)
    val tableSchema = wrapperTableInfo.getFactTable
    tableSchema.setPartitionInfo(partitionInfo)
    wrapperTableInfo.setFactTable(tableSchema)
    wrapperTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    val thriftTable =
      schemaConverter.fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
    thriftTable.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis)
    carbonMetaStore.updateMetadataByThriftTable(schemaFilePath, thriftTable,
      dbName, tableName, storePath)
    CarbonUtil.writeThriftTableToSchemaFile(schemaFilePath, thriftTable)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime(storePath)
    // sparkSession.catalog.refreshTable(tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    var locks = List.empty[ICarbonLock]
    var success = false
    try {
      locks = AlterTableUtil.validateTableAndAcquireLock(dbName, tableName,
        locksToBeAcquired)(sparkSession)
      val carbonLoadModel = new CarbonLoadModel()
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(storePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      CarbonDataRDDFactory.alterTableDropPartition(sparkSession.sqlContext,
        partitionId,
        carbonLoadModel,
        dropWithData,
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
      LOGGER.audit("Locks released after alter table drop partition action.")
    }
    LOGGER.info(s"Alter table drop partition is successful for table $dbName.$tableName")
    LOGGER.audit(s"Alter table drop partition is successful for table $dbName.$tableName")
    Seq.empty
  }
}
