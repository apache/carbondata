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
package org.apache.carbondata.indexserver

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.Partition

import org.apache.carbondata.core.datamap.{DataMapDistributable, Segment}
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper

object DistributedRDDUtils {
  // Segment number to executorNode mapping
  val segmentToExecutorMapping: java.util.Map[String, String] =
    new ConcurrentHashMap[String, String]()

  // executorNode to segmentSize mapping
  val executorToCacheSizeMapping: java.util.Map[String, Long] =
    new ConcurrentHashMap[String, Long]()

  def getExecutors(segment: Array[InputSplit], executorsList : Set[String],
      tableUniqueName: String, rddId: Int): Seq[Partition] = {
    // sort the partitions in increasing order of index size.
    val (segments, legacySegments) = segment.span(split => split
      .asInstanceOf[DataMapDistributableWrapper].getDistributable.getSegment.getIndexSize > 0)
    val sortedPartitions = segments.sortWith(_.asInstanceOf[DataMapDistributableWrapper]
                                              .getDistributable.getSegment.getIndexSize >
                                            _.asInstanceOf[DataMapDistributableWrapper]
                                              .getDistributable.getSegment.getIndexSize)
    val executorCache = DistributedRDDUtils.executorToCacheSizeMapping
    // check if any executor is dead.
    val invalidExecutors = executorCache.keySet().asScala.diff(executorsList)
    if (invalidExecutors.nonEmpty) {
      // extract the dead executor host name
      DistributedRDDUtils.invalidateExecutors(invalidExecutors.toSeq)
    }
    (convertToPartition(legacySegments, tableUniqueName, executorsList) ++
     convertToPartition(sortedPartitions, tableUniqueName, executorsList)).zipWithIndex.map {
      case (dataMapDistributable, index) =>
        new DataMapRDDPartition(rddId, index, dataMapDistributable)
    }
  }

  private def convertToPartition(segments: Seq[InputSplit], tableUniqueName: String,
      executorList: Set[String]): Seq[InputSplit] = {
    segments.map { partition =>
      val wrapper: DataMapDistributable = partition.asInstanceOf[DataMapDistributableWrapper]
        .getDistributable
      if (wrapper.getSegment.getIndexSize == 0L) {
        wrapper.getSegment.setIndexSize(1L)
      }
      wrapper.setLocations(Array(DistributedRDDUtils
        .assignExecutor(tableUniqueName, wrapper.getSegment, executorList)))
      partition
    }
  }

  /**
   * Update the cache size returned by the executors to the driver mapping.
   */
  def updateExecutorCacheSize(cacheSizes: Set[String]): Unit = {
    synchronized {
      cacheSizes.foreach {
        executorCacheSize =>
          // executorCacheSize would be in the form of 127.0.0.1_10024 where the left of '_'
          // would be the executor IP and the right would be the cache that executor is holding.
          val executorIP = executorCacheSize.substring(0, executorCacheSize.lastIndexOf('_'))
          val size = executorCacheSize.substring(executorCacheSize.lastIndexOf('_') + 1,
            executorCacheSize.length)
          executorToCacheSizeMapping.put(executorIP, size.toLong)
      }
    }
  }

  def invalidateCache(tableUniqueName: String): Unit = {
    segmentToExecutorMapping.keySet().asScala.foreach {
      key =>
        if (key.split("_")(0).equalsIgnoreCase(tableUniqueName)) {
          segmentToExecutorMapping.remove(key)
        }
    }
  }

  /**
   * Invalidate the dead executors from the mapping and assign the segments to some other
   * executor, so that the query can load the segments to the new assigned executor.
   */
  def invalidateExecutors(invalidExecutors: Seq[String]): Unit = synchronized {
    // remove all invalidExecutor mapping from cache.
    for ((key: String, value: String) <- segmentToExecutorMapping.asScala) {
      // find the invalid executor in cache.
      if (invalidExecutors.contains(value)) {
        // remove mapping for the invalid executor.
        val invalidExecutorSize = executorToCacheSizeMapping.remove(key)
        // find a new executor for the segment
        val reassignedExecutor = getLeastLoadedExecutor
        segmentToExecutorMapping.put(key, reassignedExecutor)
        // add the size size of the invalid executor to the reassigned executor.
        executorToCacheSizeMapping.put(
          reassignedExecutor,
          executorToCacheSizeMapping.get(reassignedExecutor) + invalidExecutorSize
        )
      }
    }
  }

  /**
   * Sorts the executor cache based on the size each one is handling and returns the least of them.
   *
   * @return
   */
  private def getLeastLoadedExecutor: String = {
    executorToCacheSizeMapping.asScala.toSeq.sortWith(_._2 < _._2).head._1
  }

  /**
   * Assign a executor for the current segment. If a executor was previously assigned to the
   * segment then the same would be returned.
   *
   * @return
   */
  def assignExecutor(tableName: String, segment: Segment, validExecutors: Set[String]): String = {
    val cacheKey = s"${ tableName }_${ segment.getSegmentNo }"
    val executor = segmentToExecutorMapping.get(cacheKey)
    if (executor != null) {
      executor
    } else {
      // check if any executor is not assigned. If yes then give priority to that executor
      // otherwise get the executor which has handled the least size.
      val unassignedExecutors = validExecutors
        .diff(executorToCacheSizeMapping.asScala.keys.toSet)
      val newExecutor = if (unassignedExecutors.nonEmpty) {
        unassignedExecutors.head.split(":")(0)
      } else {
        val identifiedExecutor = getLeastLoadedExecutor
        identifiedExecutor
      }
      val existingExecutorSize = executorToCacheSizeMapping.get(newExecutor)
      executorToCacheSizeMapping.put(newExecutor, existingExecutorSize + segment.getIndexSize
        .toInt)
      segmentToExecutorMapping.put(cacheKey, newExecutor)
      newExecutor
    }
  }

}
