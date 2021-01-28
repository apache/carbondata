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

package org.apache.spark.util

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.httpclient.util.URIUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.{Cache, Cacheable, CarbonLRUCache}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath

object PartitionCacheManager extends Cache[PartitionCacheKey,
  java.util.List[CatalogTablePartition]] {

  private val CACHE = new CarbonLRUCache(
    CarbonCommonConstants.CARBON_PARTITION_MAX_DRIVER_LRU_CACHE_SIZE,
    CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT)

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def get(identifier: PartitionCacheKey): java.util.List[CatalogTablePartition] = {
    LOGGER.info("Reading partition values from store")
    // read the tableStatus file to get valid and invalid segments
    val validInvalidSegments = new SegmentStatusManager(AbsoluteTableIdentifier.from(
      identifier.tablePath, null, null, identifier.tableId))
      .getValidAndInvalidSegments
    val existingCache = CACHE.get(identifier.tableId)
    val cacheablePartitionSpecs = validInvalidSegments.getValidSegments.asScala.map { segment =>
      val segmentFileName = segment.getSegmentFileName
      val segmentFilePath = FileFactory.getCarbonFile(
        CarbonTablePath.getSegmentFilePath(identifier.tablePath, segmentFileName))
      // read the last modified time
      val segmentFileModifiedTime = segmentFilePath.getLastModifiedTime
      if (existingCache != null) {
        val segmentCache = existingCache.asInstanceOf[CacheablePartitionSpec]
          .partitionSpecs.get(segment.getSegmentNo)
        segmentCache match {
          case Some(c) =>
            // check if cache needs to be updated
            if (segmentFileModifiedTime > c._2) {
              (segment.getSegmentNo, (readPartition(identifier,
                segmentFilePath.getAbsolutePath), segmentFileModifiedTime))
            } else {
              (segment.getSegmentNo, c)
            }
          case None =>
            (segment.getSegmentNo, (readPartition(identifier,
              segmentFilePath.getAbsolutePath), segmentFileModifiedTime))
        }
      } else {
        // read the partitions if not available in cache.
        (segment.getSegmentNo, (readPartition(identifier,
          segmentFilePath.getAbsolutePath), segmentFileModifiedTime))
      }
    }.toMap
    val invalidSegmentMap = validInvalidSegments.getInvalidSegments.asScala
      .map(seg => (seg.getSegmentNo, seg)).toMap
    // remove all invalid segment entries from cache
    val finalCache = cacheablePartitionSpecs -- invalidSegmentMap.keySet
    val cacheObject = CacheablePartitionSpec(finalCache)
    if (finalCache.nonEmpty) {
      // remove the existing cache as new cache values may be added.
      // CarbonLRUCache does not allow cache updation until time is expired.
      // TODO: Need to fix!!
      CACHE.remove(identifier.tableId)
      CACHE.put(identifier.tableId,
        cacheObject,
        cacheObject.getMemorySize,
        identifier.expirationTime)
    } else if (invalidSegmentMap != null && invalidSegmentMap.nonEmpty) {
      CACHE.remove(identifier.tableId)
    }
    finalCache.values.flatMap(_._1).toSet.toList.asJava
  }

  override def getAll(keys: util.List[PartitionCacheKey]):
  util.List[util.List[CatalogTablePartition]] = {
    keys.asScala.toList.map(get).asJava
  }

  override def getIfPresent(key: PartitionCacheKey): java.util.List[CatalogTablePartition] = {
    CACHE.get(key.tableId).asInstanceOf[CacheablePartitionSpec].partitionSpecs.values.flatMap(_._1)
      .toList.asJava
  }

  override def invalidate(partitionCacheKey: PartitionCacheKey): Unit = {
    CACHE.remove(partitionCacheKey.tableId)
  }

  private def readPartition(identifier: PartitionCacheKey, segmentFilePath: String) = {
    val segmentFile = SegmentFileStore.readSegmentFile(segmentFilePath)
    var partitionSpec: Map[String, String] = Map()
    segmentFile.getLocationMap.keySet().asScala
      .map { uniquePartition: String =>
        val partitionSplit = uniquePartition.substring(1)
          .split(CarbonCommonConstants.FILE_SEPARATOR)
        val storageFormat = CatalogStorageFormat(
          Some(new URI(URIUtil.encodeQuery(identifier.tablePath + uniquePartition))),
          None, None, None, compressed = false, Map())
        partitionSplit.foreach(partition => {
          val partitionArray = partition.split("=")
          partitionSpec = partitionSpec. + (partitionArray(0) -> partitionArray(1))
        })
        CatalogTablePartition(partitionSpec, storageFormat)
      }.toSeq
  }

  override def put(key: PartitionCacheKey,
      value: java.util.List[CatalogTablePartition]): Unit = {

  }

  override def clearAccessCount(keys: util.List[PartitionCacheKey]): Unit = {

  }
}

case class PartitionCacheKey(tableId: String, tablePath: String, expirationTime: Long)

/**
 * Cacheable instance of the CatalogTablePartitions.
 *
 * Maintains a mapping of segmentNo -> (Seq[CatalogTablePartition], lastModifiedTime)
 */
case class CacheablePartitionSpec(partitionSpecs: Map[String, (Seq[CatalogTablePartition], Long)])
  extends Cacheable {

  /**
   * This method will return the access count for a column based on which decision will be taken
   * whether to keep the object in memory
   *
   * @return
   */
  override def getAccessCount: Int = {
    0
  }

  /**
   * This method will return the memory size of the cached partition
   *
   * @return
   */
  override def getMemorySize: Long = {
    partitionSpecs.values.flatMap {
      partitionSpec =>
        partitionSpec._1.map { specs =>
          if (specs.stats.isDefined) {
            specs.stats.get.sizeInBytes.toLong
          } else {
            SizeEstimator.estimate(specs)
          }
        }
    }.sum
  }

  /**
   * Method to be used for invalidating the cacheable object. API to be invoked at the time of
   * removing the cacheable object from memory. Example at removing the cacheable object
   * from LRU cache
   */
  override def invalidate(): Unit = {

  }
}
