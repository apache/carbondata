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

package org.apache.spark.sql.execution.command.cache

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheType
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.bloom.{BloomCacheKeyValue, BloomCoarseGrainDataMapFactory}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil


object CacheUtil {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Given a carbonTable, returns the list of all carbonindex files
   *
   * @param carbonTable
   * @return List of all index files
   */
  def getAllIndexFiles(carbonTable: CarbonTable)(sparkSession: SparkSession): List[String] = {
    if (carbonTable.isTransactionalTable) {
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val validAndInvalidSegmentsInfo = new SegmentStatusManager(absoluteTableIdentifier)
        .getValidAndInvalidSegments(carbonTable.isChildTableForMV)
      // Fire a job to clear the invalid segments cached in the executors.
      if (CarbonProperties.getInstance().isDistributedPruningEnabled(carbonTable.getDatabaseName,
        carbonTable.getTableName)) {
        val invalidSegmentIds = validAndInvalidSegmentsInfo.getInvalidSegments.asScala
          .map(_.getSegmentNo).toArray
        try {
          IndexServer.getClient
            .invalidateSegmentCache(carbonTable,
              invalidSegmentIds,
              SparkSQLUtil.getTaskGroupId(sparkSession))
        } catch {
          case e: Exception =>
            LOGGER.warn("Failed to clear cache from executors. ", e)
        }
      }
      validAndInvalidSegmentsInfo.getValidSegments.asScala.flatMap {
        segment =>
          segment.getCommittedIndexFile.keySet().asScala
      }.map { indexFile =>
        indexFile.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
          CarbonCommonConstants.FILE_SEPARATOR)
      }.toList
    } else {
      val tablePath = carbonTable.getTablePath
      val readCommittedScope = new LatestFilesReadCommittedScope(tablePath,
        FileFactory.getConfiguration)
      readCommittedScope.getSegmentList.flatMap {
        load =>
          val seg = new Segment(load.getLoadName, null, readCommittedScope)
          seg.getCommittedIndexFile.keySet().asScala
      }.map { indexFile =>
        indexFile.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
          CarbonCommonConstants.FILE_SEPARATOR)
      }.toList
    }
  }

  def getBloomCacheKeys(carbonTable: CarbonTable, datamap: DataMapSchema): List[String] = {
    val segments = CarbonDataMergerUtil.getValidSegmentList(carbonTable).asScala

    // Generate shard Path for the datamap
    val shardPaths = segments.flatMap {
      segment =>
        BloomCoarseGrainDataMapFactory.getAllShardPaths(carbonTable.getTablePath,
          segment.getSegmentNo, datamap.getDataMapName).asScala
    }

    // get index columns
    val indexColumns = carbonTable.getIndexedColumns(datamap).asScala.map {
      entry =>
        entry.getColName
    }

    // generate cache key using shard path and index columns on which bloom was created.
    val datamapKeys = shardPaths.flatMap {
      shardPath =>
        indexColumns.map {
          indexCol =>
            new BloomCacheKeyValue.CacheKey(shardPath, indexCol).toString
      }
    }
    datamapKeys.toList
  }

}
