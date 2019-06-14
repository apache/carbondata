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

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.Partition

import org.apache.carbondata.core.datamap.{DataMapDistributable, Segment}
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper

object DistributedRDDUtils {
  // Segment number to executorNode mapping
  val tableToExecutorMapping: ConcurrentHashMap[String, ConcurrentHashMap[String, String]] =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, String]]()

  // executorNode to segmentSize mapping
  val executorToCacheSizeMapping: ConcurrentHashMap[String, ConcurrentHashMap[String, Long]] =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, Long]]()

  def getExecutors(segment: Array[InputSplit], executorsList : Map[String, Seq[String]],
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
    val invalidHosts = executorCache.keySet().asScala.diff(executorsList.keySet)
    if (invalidHosts.nonEmpty) {
      // extract the dead executor host name
      DistributedRDDUtils.invalidateHosts(invalidHosts.toSeq)
    }
    val invalidExecutorIds = executorsList.collect {
      case (host, executors) if executorCache.get(host) != null =>
        val toBeRemovedExecutors = executorCache.get(host).keySet().asScala.diff(executors.toSet)
        if (executors.size == toBeRemovedExecutors.size) {
          DistributedRDDUtils.invalidateHosts(Seq(host))
          Seq()
        } else {
          toBeRemovedExecutors.map(executor => host + "_" + executor)
        }
    }.flatten
    if (invalidExecutorIds.nonEmpty) {
      DistributedRDDUtils.invalidateExecutors(invalidExecutorIds.toSeq)
    }
    val groupedPartitions = (convertToPartition(legacySegments, tableUniqueName, executorsList) ++
                             convertToPartition(sortedPartitions, tableUniqueName, executorsList))
      .groupBy {
        partition =>
          partition.getLocations.head
      }
    groupedPartitions.zipWithIndex.map {
      case ((location, splitList), index) =>
        new DataMapRDDPartition(rddId,
          index, splitList,
          Array(location))
    }.toArray.sortBy(_.index)
  }

  private def convertToPartition(segments: Seq[InputSplit], tableUniqueName: String,
      executorList: Map[String, Seq[String]]): Seq[InputSplit] = {
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
          val hostAndExecutor = executorCacheSize.substring(0,
            executorCacheSize.lastIndexOf('_'))
          val (host, executor) = (hostAndExecutor
            .substring(0, hostAndExecutor.lastIndexOf('_')), hostAndExecutor
            .substring(hostAndExecutor.lastIndexOf('_') + 1, hostAndExecutor.length))
          val size = executorCacheSize.substring(executorCacheSize.lastIndexOf('_') + 1,
            executorCacheSize.length)
          val executorMapping = executorToCacheSizeMapping.get(host)
          if (executorMapping != null) {
            executorMapping.put(executor, size.toLong)
            executorToCacheSizeMapping.put(host, executorMapping)
          }
      }
    }
  }

  def invalidateCache(tableUniqueName: String): Unit = {
    tableToExecutorMapping.keySet().asScala.foreach {
      key =>
        if (key.equalsIgnoreCase(tableUniqueName)) {
          tableToExecutorMapping.remove(key)
        }
    }
  }

  /**
   * Invalidate the dead executors from the mapping and assign the segments to some other
   * executor, so that the query can load the segments to the new assigned executor.
   */
  def invalidateHosts(invalidHosts: Seq[String]): Unit = {
    synchronized {
      val validInvalidExecutors: Map[String, String] = invalidHosts.flatMap {
        host =>
          val invalidExecutorToSizeMapping = executorToCacheSizeMapping.remove(host)
          invalidExecutorToSizeMapping.asScala.map {
            case (invalidExecutor, size) =>
              getLeastLoadedExecutor match {
                case Some((reassignedHost, reassignedExecutorId)) =>
                  val existingExecutorMapping = executorToCacheSizeMapping.get(reassignedHost)
                  if (existingExecutorMapping != null) {
                    val existingSize = existingExecutorMapping.get(reassignedExecutorId)
                    existingExecutorMapping.put(reassignedExecutorId, existingSize + size)
                  } else {
                    existingExecutorMapping.put(reassignedExecutorId, size)
                  }
                  executorToCacheSizeMapping.put(reassignedHost, existingExecutorMapping)
                  s"${host}_$invalidExecutor" -> s"${ reassignedHost }_$reassignedExecutorId"
                case None => "" -> ""
              }
          }
      }.toMap
      updateTableMappingForInvalidExecutors(validInvalidExecutors)
    }
  }

  private def updateTableMappingForInvalidExecutors(validInvalidExecutors: Map[String, String]) {
    // remove all invalidExecutor mapping from cache.
    for ((tableName: String, segmentToExecutorMapping) <- tableToExecutorMapping.asScala) {
      // find the invalid executor in cache.
      val newSegmentToExecutorMapping = new ConcurrentHashMap[String, String]()
      val existingMapping = tableToExecutorMapping.get(tableName)
      segmentToExecutorMapping.asScala.collect {
        case (segmentNumber, executorUniqueName) if validInvalidExecutors
          .contains(executorUniqueName) =>
          val newExecutorId = validInvalidExecutors(executorUniqueName)
          // remove mapping for the invalid executor.
          val executorIdSplits = newExecutorId.split("_")
          val (host, executorId) = (executorIdSplits(0), executorIdSplits(1))
          // find a new executor for the segment
          newSegmentToExecutorMapping
            .put(segmentNumber, s"${ host }_$executorId")
      }
      existingMapping.putAll(newSegmentToExecutorMapping)
      tableToExecutorMapping.put(tableName, existingMapping)
    }
  }

  def invalidateExecutors(invalidExecutors: Seq[String]): Unit = {
    synchronized {
      val validInvalidExecutors: Map[String, String] = invalidExecutors.map {
        invalidExecutor =>
          val executorIdSplits = invalidExecutor.split("_")
          val (host, executor) = (executorIdSplits(0), executorIdSplits(1))
          val invalidExecutorSize = executorToCacheSizeMapping.get(host).remove(executor)
          getLeastLoadedExecutor match {
            case Some((reassignedHost, reassignedExecutorId)) =>
              val existingExecutorMapping = executorToCacheSizeMapping.get(reassignedHost)
              if (existingExecutorMapping != null) {
                val existingSize = existingExecutorMapping.get(reassignedExecutorId)
                existingExecutorMapping
                  .put(reassignedExecutorId, existingSize + invalidExecutorSize)
              } else {
                existingExecutorMapping.put(reassignedExecutorId, invalidExecutorSize)
              }
              executorToCacheSizeMapping.put(reassignedHost, existingExecutorMapping)
              invalidExecutor -> s"${ reassignedHost }_$reassignedExecutorId"
            case None => "" -> ""
          }
      }.toMap
      updateTableMappingForInvalidExecutors(validInvalidExecutors)
    }
  }

  /**
   * Sorts the executor cache based on the size each one is handling and returns the least of them.
   *
   * @return
   */
  private def getLeastLoadedExecutor: Option[(String, String)] = {
    val leastHostExecutor = executorToCacheSizeMapping.asScala.flatMap {
      case (host, executorToCacheMap) =>
        executorToCacheMap.asScala.map {
          case (executor, size) =>
            (host, executor, size)
        }
    }.toSeq.sortWith(_._3 < _._3).toList
    leastHostExecutor match {
      case head :: _ =>
        Some(head._1, head._2)
      case _ => None
    }
  }

  private def checkForUnassignedExecutors(validExecutorIds: Seq[String]): Option[String] = {
    val usedExecutorIds = executorToCacheSizeMapping.asScala.flatMap {
      case (host, executorMap) =>
        executorMap.keySet().asScala.map {
          executor => s"${ host }_$executor"
        }
    }
    val unassignedExecutor = validExecutorIds.diff(usedExecutorIds.toSeq)
    unassignedExecutor.headOption
  }

  /**
   * Assign a executor for the current segment. If a executor was previously assigned to the
   * segment then the same would be returned.
   *
   * @return
   */
  def assignExecutor(tableUniqueName: String,
      segment: Segment,
      validExecutors: Map[String, Seq[String]]): String = {
    val segmentMapping = tableToExecutorMapping.get(tableUniqueName)
    lazy val executor = segmentMapping.get(segment.getSegmentNo)
    if (segmentMapping != null && executor != null) {
      s"executor_$executor"
    } else {
      // check if any executor is not assigned. If yes then give priority to that executor
      // otherwise get the executor which has handled the least size.
      val validExecutorIds = validExecutors.flatMap {
        case (host, executors) => executors.map {
          executor => s"${host}_$executor"
        }
      }.toSeq
      val unassignedExecutor = checkForUnassignedExecutors(validExecutorIds)
      val (newHost, newExecutor) = if (unassignedExecutor.nonEmpty) {
        val freeExecutor = unassignedExecutor.get.split("_")
        (freeExecutor(0), freeExecutor(1))
      } else {
        getLeastLoadedExecutor match {
          case Some((host, executorID)) => (host, executorID)
          case None => throw new RuntimeException("Could not find any alive executors.")
        }
      }
      val existingExecutorMapping = executorToCacheSizeMapping.get(newHost)
      if (existingExecutorMapping != null) {
        val existingSize = existingExecutorMapping.get(newExecutor)
        if (existingSize != null) {
          existingExecutorMapping.put(newExecutor, existingSize + segment.getIndexSize
            .toInt)
        } else {
          existingExecutorMapping.put(newExecutor, segment.getIndexSize
            .toInt)
        }
      } else {
        val newExecutorMapping = new ConcurrentHashMap[String, Long]()
        newExecutorMapping.put(newExecutor, segment.getIndexSize)
        executorToCacheSizeMapping.put(newHost, newExecutorMapping)
      }
      val existingSegmentMapping = tableToExecutorMapping.get(tableUniqueName)
      if (existingSegmentMapping == null) {
        val newSegmentMapping = new ConcurrentHashMap[String, String]()
        newSegmentMapping.put(segment.getSegmentNo, s"${newHost}_$newExecutor")
        tableToExecutorMapping.put(tableUniqueName, newSegmentMapping)
      } else {
        existingSegmentMapping.put(segment.getSegmentNo, s"${newHost}_$newExecutor")
        tableToExecutorMapping.put(tableUniqueName, existingSegmentMapping)
      }
      s"executor_${newHost}_$newExecutor"
    }
  }

}
