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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{DataProcessCommand, RunnableCommand, SchemaProcessCommand}
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonRelation}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.store.AlterTableSplitPartitionModel
import org.apache.carbondata.store.util.PartitionUtils

/**
 * Command for Alter Table Add & Split partition
 * Add is a special case of Splitting the default partition (part0)
 */
case class AlterTableSplitCarbonPartitionCommand(
    splitPartitionModel: AlterTableSplitPartitionModel)
  extends RunnableCommand with DataProcessCommand with SchemaProcessCommand {

  val oldPartitionIds: util.ArrayList[Int] = new util.ArrayList[Int]()

  // TODO will add rollback function in case of process data failure
  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
    processData(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    val dbName = splitPartitionModel.databaseName.getOrElse(sparkSession.catalog.currentDatabase)
    val carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val tableName = splitPartitionModel.tableName
    val relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTableIdentifier = relation.tableMeta.carbonTableIdentifier
    val storePath = relation.tableMeta.storePath
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    carbonMetaStore.checkSchemasModifiedTimeAndReloadTables(storePath)
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      LOGGER.error(s"Alter table failed. table not found: $dbName.$tableName")
      sys.error(s"Alter table failed. table not found: $dbName.$tableName")
    }
    val table = relation.tableMeta.carbonTable
    val partitionInfo = table.getPartitionInfo(tableName)
    val partitionIds = partitionInfo.getPartitionIds.asScala.map(_.asInstanceOf[Int]).toList
    // keep a copy of partitionIdList before update partitionInfo.
    // will be used in partition data scan
    oldPartitionIds.addAll(partitionIds.asJava)

    if (partitionInfo == null) {
      sys.error(s"Table $tableName is not a partition table.")
    }
    if (partitionInfo.getPartitionType == PartitionType.HASH) {
      sys.error(s"Hash partition table cannot be added or split!")
    }

    updatePartitionInfo(partitionInfo, partitionIds)

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
    carbonMetaStore.updateMetadataByThriftTable(schemaFilePath, thriftTable,
      dbName, tableName, storePath)
    CarbonUtil.writeThriftTableToSchemaFile(schemaFilePath, thriftTable)
    // update the schema modified time
    carbonMetaStore.updateAndTouchSchemasUpdatedTime(storePath)
    sparkSession.catalog.refreshTable(tableName)
    Seq.empty
  }

  private def updatePartitionInfo(partitionInfo: PartitionInfo,
      partitionIds: List[Int]) = {
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
      val carbonMetaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val relation = carbonMetaStore.lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
      val storePath = relation.tableMeta.storePath
      val table = relation.tableMeta.carbonTable
      val carbonTableIdentifier = relation.tableMeta.carbonTableIdentifier
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(storePath)
      val loadStartTime = CarbonUpdateUtil.readCurrentTime
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      CarbonDataRDDFactory.alterTableSplitPartition(
        sparkSession.sqlContext,
        splitPartitionModel.partitionId.toInt.toString,
        carbonLoadModel,
        oldPartitionIds.asScala.toList
      )
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
      LOGGER.audit("Locks released after alter table add/split partition action.")
      if (success) {
        LOGGER.info(s"Alter table add/split partition is successful for table $dbName.$tableName")
        LOGGER.audit(s"Alter table add/split partition is successful for table $dbName.$tableName")
      }
    }
    Seq.empty
  }
}
