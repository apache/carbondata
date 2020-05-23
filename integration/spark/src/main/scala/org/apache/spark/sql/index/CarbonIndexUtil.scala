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

package org.apache.spark.sql.index

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.sql.secondaryindex.command.{IndexModel, SecondaryIndexModel}
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.secondaryindex.rdd.SecondaryIndexCreator
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * Carbon Index util
 */
object CarbonIndexUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def addIndexTableInfo(
      indexProvider: String,
      carbonTable: CarbonTable,
      tableName: String,
      indexProperties: java.util.Map[String, String]): Unit = {
    val indexMetadata = carbonTable.getIndexMetadata
    if (null != indexMetadata) {
        indexMetadata.addIndexTableInfo(indexProvider, tableName, indexProperties)
    }
  }

  def removeIndexTableInfo(carbonTable: CarbonTable, tableName: String): Unit = {
    val indexMetadata = carbonTable.getIndexMetadata
    if (null != indexMetadata) {
      indexMetadata.removeIndexTableInfo(tableName)
    }
  }

  /**
   * Check if the index meta data for the table exists or not
   *
   * @param carbonTable
   * @return
   */
  def isIndexTableExists(carbonTable: CarbonTable): String = {
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .get("indextableexists")
  }

  def isIndexExists(carbonTable: CarbonTable): String = {
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .get("indexexists")
  }

  def getSecondaryIndexes(carbonTable: CarbonTable): java.util.List[String] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val secondaryIndexesTables = if (null != indexMetadata) {
     indexMetadata.getIndexTables(IndexType.SI.getIndexProviderName)
    } else {
      new java.util.ArrayList[String]
    }
    secondaryIndexesTables
  }

  def getCGAndFGIndexes(carbonTable: CarbonTable): java.util.Map[String,
    util.Map[String, util.Map[String, String]]] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val cgAndFgIndexes = if (null != indexMetadata && null != indexMetadata.getIndexesMap) {
      val indexesMap = indexMetadata.getIndexesMap
      indexesMap.asScala.filter(provider =>
        !provider._1.equalsIgnoreCase(IndexType.SI.getIndexProviderName)).asJava
    } else {
      new util.HashMap[String, util.Map[String, util.Map[String, String]]]()
    }
    cgAndFgIndexes
  }

  def getParentTableName(carbonTable: CarbonTable): String = {
    val indexMetadata = carbonTable.getIndexMetadata
    val indexesTables = if (null != indexMetadata) {
      indexMetadata.getParentTableName
    } else {
      null
    }
    indexesTables
  }

  def getSecondaryIndexes(relation: CarbonDatasourceHadoopRelation): scala.collection.mutable
  .Map[String, Array[String]] = {
    val indexes = scala.collection.mutable.Map[String, Array[String]]()
    val carbonTable = relation.carbonRelation.carbonTable
    val indexInfo = carbonTable.getIndexInfo(IndexType.SI.getIndexProviderName)
    if (null != indexInfo) {
      IndexTableInfo.fromGson(indexInfo).foreach { indexTableInfo =>
        indexes.put(indexTableInfo.getTableName, indexTableInfo.getIndexCols.asScala.toArray)
      }
    }
    indexes
  }

  /**
   * For a given index table this method will prepare the table status details
   *
   */
  def getTableStatusDetailsForIndexTable(factLoadMetadataDetails: util.List[LoadMetadataDetails],
      indexTable: CarbonTable,
      newSegmentDetailsObject: util.List[LoadMetadataDetails]): util.List[LoadMetadataDetails] = {
    val indexTableDetailsList: util.List[LoadMetadataDetails] = new util
    .ArrayList[LoadMetadataDetails](
      factLoadMetadataDetails.size)
    val indexTableStatusDetailsArray: Array[LoadMetadataDetails] = SegmentStatusManager
      .readLoadMetadata(indexTable.getMetadataPath)
    if (null !=
        indexTableStatusDetailsArray) {
      for (loadMetadataDetails <- indexTableStatusDetailsArray) {
        indexTableDetailsList.add(loadMetadataDetails)
      }
    }
    indexTableDetailsList.addAll(newSegmentDetailsObject)
    val iterator: util.Iterator[LoadMetadataDetails] = indexTableDetailsList.iterator
    // synchronize the index table status file with its parent table
    while ( { iterator.hasNext }) {
      val indexTableDetails: LoadMetadataDetails = iterator.next
      var found: Boolean = false
      for (factTableDetails <- factLoadMetadataDetails.asScala) {
        // null check is added because in case of auto load, load end time will be null
        // for the last entry
        if (0L != factTableDetails.getLoadEndTime &&
            indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(factTableDetails.getLoadStatus)
          indexTableDetails.setMajorCompacted(factTableDetails.isMajorCompacted)
          indexTableDetails.setMergedLoadName(factTableDetails.getMergedLoadName)
          indexTableDetails
            .setModificationOrdeletionTimesStamp(factTableDetails
              .getModificationOrdeletionTimesStamp)
          indexTableDetails.setLoadEndTime(factTableDetails.getLoadEndTime)
          indexTableDetails.setVisibility(factTableDetails.getVisibility)
          found = true
          // TODO: make it breakable
        } else if (indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(CarbonCommonConstants
          // .STORE_LOADSTATUS_SUCCESS)
          indexTableDetails.setLoadEndTime(CarbonUpdateUtil.readCurrentTime)
          found = true
          // TODO: make it breakable
        }
      }
      // in case there is some inconsistency between fact table index file and index table
      // status file, it can resolved here by removing unwanted segments
      if (!found) {
        iterator.remove()
      }
    }
    indexTableDetailsList
  }

  def checkIsIndexTable(plan: LogicalPlan): Boolean = {
    plan match {
      case Aggregate(_, _, plan) if (isIndexTablesJoin(plan)) => true
      case _ => false
    }
  }

  /**
   * Collect all logical relation and check for if plan contains index table join
   *
   * @param plan
   * @return false if there are no index tables found in the plan or if logical relation is empty.
   */
  def isIndexTablesJoin(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.nonEmpty && allRelations.forall(x =>
      x.relation.isInstanceOf[CarbonDatasourceHadoopRelation]
      && x.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isIndexTable)
  }

  /**
   * Get the column compressor for the index table. Check first in the index table tableproperties
   * and then fall back to main table at last to the default compressor
   */
  def getCompressorForIndexTable(
      indexTable: CarbonTable,
      parentTable: CarbonTable) : String = {
    // get the compressor from the index table (table properties)
    var columnCompressor = indexTable.getTableInfo.getFactTable.getTableProperties.get(
      CarbonCommonConstants.COMPRESSOR)
    if (null == columnCompressor) {
      // if nothing is set to index table then fall to the main table compressor
      columnCompressor = parentTable.getTableInfo.getFactTable.getTableProperties.get(
        CarbonCommonConstants.COMPRESSOR)
      if (null == columnCompressor) {
        // if main table compressor is also not set then choose the default compressor
        columnCompressor = CompressorFactory.getInstance.getCompressor.getName
      }
    }
    columnCompressor
  }


  def getIndexCarbonTables(carbonTable: CarbonTable,
      sparkSession: SparkSession): Seq[CarbonTable] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val siIndexesMap = if (null != indexMetadata && null != indexMetadata.getIndexesMap) {
      indexMetadata.getIndexesMap.get(IndexType.SI.getIndexProviderName)
    } else {
      new util.HashMap[String, util.Map[String, util.Map[String, String]]]()
    }
    if (null != siIndexesMap) {
      siIndexesMap.keySet().asScala.map {
        indexTable =>
          CarbonEnv.getCarbonTable(Some(carbonTable.getDatabaseName), indexTable)(sparkSession);
      }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * This method loads data to SI table, if isLoadToFailedSISegments is true, then load to only
   * failed segments, if false, just load the data to current segment of main table load
   */
  def LoadToSITable(sparkSession: SparkSession,
    carbonLoadModel: CarbonLoadModel,
    indexTableName: String,
    isLoadToFailedSISegments: Boolean,
    secondaryIndex: IndexModel,
    carbonTable: CarbonTable,
    indexTable: CarbonTable,
    failedLoadMetaDataDetils: java.util.List[LoadMetadataDetails] = null): Unit = {

    var segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
      scala.collection.mutable.Map()

    val segmentsToReload: scala.collection.mutable.ListBuffer[String] = scala
      .collection
      .mutable.ListBuffer[String]()

    if (isLoadToFailedSISegments && null != failedLoadMetaDataDetils) {
      val metadata = CarbonInternalLoaderUtil
        .getListOfValidSlices(SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath))
      failedLoadMetaDataDetils.asScala.foreach(loadMetaDetail => {
        // check whether this segment is valid or invalid, if it is present in the valid list
        // then don't consider it for reloading
        if (!metadata.contains(loadMetaDetail.getLoadName)) {
          segmentsToReload.append(loadMetaDetail.getLoadName)
        }
      })
      LOGGER.info(
        s"SI segments to be reloaded for index table: ${
          indexTable.getTableUniqueName} are: ${segmentsToReload}")
      segmentIdToLoadStartTimeMapping = CarbonInternalLoaderUtil
        .getSegmentToLoadStartTimeMapping(carbonLoadModel.getLoadMetadataDetails.asScala.toArray)
        .asScala
    } else {
      segmentIdToLoadStartTimeMapping = scala.collection.mutable
        .Map((carbonLoadModel.getSegmentId, carbonLoadModel.getFactTimeStamp))
    }
    val secondaryIndexModel = if (isLoadToFailedSISegments) {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        segmentsToReload.toList,
        segmentIdToLoadStartTimeMapping)
    } else {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        List(carbonLoadModel.getSegmentId),
        segmentIdToLoadStartTimeMapping)
    }

    val segmentToSegmentTimestampMap: java.util.Map[String, String] = new java.util
    .HashMap[String, String]()
    SecondaryIndexCreator
      .createSecondaryIndex(secondaryIndexModel,
        segmentToSegmentTimestampMap,
        indexTable,
        forceAccessSegment = true,
        isCompactionCall = false,
        isLoadToFailedSISegments)
  }

  /**
   * This method add/modify the table properties.
   *
   */
  def addOrModifyTableProperty(carbonTable: CarbonTable,
    properties: Map[String, String],
    needLock: Boolean = true)
    (sparkSession: SparkSession): Unit = {
    val tableName = carbonTable.getTableName
    val dbName = carbonTable.getDatabaseName
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    val locks: java.util.List[ICarbonLock] = new java.util.ArrayList[ICarbonLock]
    try {
      try {
        if (needLock) {
          locksToBeAcquired.foreach { lock =>
            locks.add(CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier, lock))
          }
        }
      } catch {
        case e: Exception =>
          AlterTableUtil.releaseLocks(locks.asScala.toList)
          throw e
      }
      val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      val lowerCasePropertiesMap: mutable.Map[String, String] = mutable.Map.empty
      // convert all the keys to lower case
      properties.foreach { entry =>
        lowerCasePropertiesMap.put(entry._1.toLowerCase, entry._2)
      }

      val thriftTableInfo: TableInfo = metaStore.getThriftTableInfo(carbonTable)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        dbName,
        tableName,
        carbonTable.getTablePath)
      val thriftTable = schemaConverter.fromWrapperToExternalTableInfo(
        wrapperTableInfo, dbName, tableName)
      val tblPropertiesMap: mutable.Map[String, String] =
        thriftTable.fact_table.getTableProperties.asScala

      // This overrides/add the newProperties of thriftTable
      lowerCasePropertiesMap.foreach { property =>
        if (tblPropertiesMap.get(property._1) != null) {
          tblPropertiesMap.put(property._1, property._2)
        }
      }
      val (tableIdentifier, schemaParts) = AlterTableUtil.updateSchemaInfo(
        carbonTable = carbonTable,
        thriftTable = thriftTable)(sparkSession)
      CarbonSessionCatalogUtil.alterTable(tableIdentifier, schemaParts, None, sparkSession)
      // remove from the cache so that the table will be loaded again with the new tableproperties
      CarbonInternalMetastore
        .removeTableFromMetadataCache(carbonTable.getDatabaseName, tableName)(sparkSession)
      // refresh the parent table relation
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)

      LOGGER.info(s"Adding/Modifying tableProperties is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        sys.error(s"Adding/Modifying tableProperties operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks.asScala.toList)
    }
  }
}
