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

package org.apache.spark.rdd

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, LinkedHashSet}

import org.apache.spark.Partition
import org.apache.spark.scheduler.TaskLocation

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 * DataLoadPartitionCoalescer
 * Repartition the partitions of rdd to few partitions, one partition per node.
 * exmaple:
 * blk_hst  host1 host2 host3 host4 host5
 * block1   host1 host2 host3
 * block2         host2       host4 host5
 * block3               host3 host4 host5
 * block4   host1 host2       host4
 * block5   host1       host3 host4
 * block6   host1 host2             host5
 * -------------------------------------------------------
 * 1. sort host by number of blocks
 * -------------------------------------------------------
 * host3: block1 block3 block5
 * host5: block2 block3 block6
 * host1: block1 block4 block5 block6
 * host2: block1 block2 block4 block6
 * host4: block2 block3 block4 block5
 * -------------------------------------------------------
 * 2. sort blocks of each host1
 * new partitions are before old partitions
 * -------------------------------------------------------
 * host3:                      block1 block3 block5
 * host5:        block2 block6+block3
 * host1: block4+block1 block5 block6
 * host2: block1 block2 block4 block6
 * host4: block2 block3 block4 block5
 * -------------------------------------------------------
 * 3. assign blocks to host
 * -------------------------------------------------------
 * step1: host3 choose block1, remove from host1, host2
 * step2: host5 choose block2, remove from host2, host4
 * step3: host1 choose block4, .....
 * -------------------------------------------------------
 * result:
 * host3:                      block1       block5
 * host5:        block2
 * host1: block4
 * host2:                      block6
 * host4:        block3
 */
class DataLoadPartitionCoalescer(prev: RDD[_], nodeList: Array[String]) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val prevPartitions = prev.partitions
  val numOfParts = Math.max(1, Math.min(nodeList.length, prevPartitions.length))
  // host => partition id list
  val hostMapPartitionIds = new HashMap[String, LinkedHashSet[Int]]
  // partition id => host list
  val partitionIdMapHosts = new HashMap[Int, ArrayBuffer[String]]
  val noLocalityPartitions = new ArrayBuffer[Int]
  var noLocality = true
  /**
   * assign a task location for a partition
   */
  private def getLocation(index: Int): Option[String] = {
    if (index < nodeList.length) {
      Some(nodeList(index))
    } else {
      None
    }
  }

  /**
   * collect partitions to each node
   */
  private def groupByNode(): Unit = {
    // initialize hostMapPartitionIds
    nodeList.foreach { node =>
      val map = new LinkedHashSet[Int]
      hostMapPartitionIds.put(node, map)
    }
    // collect partitions for each node
    val tmpNoLocalityPartitions = new ArrayBuffer[Int]
    prevPartitions.foreach { p =>
      val locs = DataLoadPartitionCoalescer.getPreferredLocs(prev, p)
      if (locs.isEmpty) {
        // if a partition has no location, add to noLocalityPartitions
        tmpNoLocalityPartitions += p.index
      } else {
        // add partion to hostMapPartitionIds and partitionIdMapHosts
        locs.foreach { loc =>
          val host = loc.host
          hostMapPartitionIds.get(host) match {
            // if the location of the partition is not in node list,
            // will add this partition to noLocalityPartitions
            case None => tmpNoLocalityPartitions += p.index
            case Some(ids) =>
              noLocality = false
              ids += p.index
              partitionIdMapHosts.get(p.index) match {
                case None =>
                  val hosts = new ArrayBuffer[String]
                  hosts += host
                  partitionIdMapHosts.put(p.index, hosts)
                case Some(hosts) =>
                  hosts += host
              }
          }
        }
      }
    }

    // remove locality partition
    tmpNoLocalityPartitions.distinct.foreach {index =>
      partitionIdMapHosts.get(index) match {
        case None => noLocalityPartitions += index
        case Some(_) =>
      }
    }
  }

  /**
   * sort host and partitions
   */
  private def sortHostAndPartitions(hostMapPartitionIdsSeq: Seq[(String, LinkedHashSet[Int])]) = {
    val oldPartitionIdSet = new HashSet[Int]
    // sort host by number of partitions
    hostMapPartitionIdsSeq.sortBy(_._2.size).map { loc =>
      // order: newPartitionIds + oldPartitionIds
      val sortedPartitionIdSet = new LinkedHashSet[Int]
      var newPartitionIds = new ArrayBuffer[Int]
      var oldPartitionIds = new ArrayBuffer[Int]
      loc._2.foreach { p =>
        if (oldPartitionIdSet.contains(p)) {
          oldPartitionIds += p
        } else {
          newPartitionIds += p
          oldPartitionIdSet.add(p)
        }
      }
      // sort and add new partitions
      newPartitionIds.sortBy(x => x).foreach(sortedPartitionIdSet.add(_))
      // sort and add old partitions
      oldPartitionIds.sortBy(x => x).foreach(sortedPartitionIdSet.add(_))
      // update hostMapPartitionIds
      hostMapPartitionIds.put(loc._1, sortedPartitionIdSet)
      (loc._1, sortedPartitionIdSet)
    }.toArray
  }

  /**
   *  assign locality partition to each host
   */
  private def assignPartitionNodeLocality(
      noEmptyHosts: Seq[(String, LinkedHashSet[Int])]): Array[ArrayBuffer[Int]] = {
    val localityResult = new Array[ArrayBuffer[Int]](noEmptyHosts.length)
    var i = 0
    val len = localityResult.length
    while (i < len) {
      localityResult(i) = new ArrayBuffer[Int]
      i += 1
    }
    val noEmptyHostSet = new HashSet[String]
    noEmptyHosts.foreach {loc => noEmptyHostSet.add(loc._1)}

    var hostIndex = 0
    while (noEmptyHostSet.nonEmpty) {
      val hostEntry = noEmptyHosts(hostIndex)
      if (noEmptyHostSet.contains(hostEntry._1)) {
        if (hostEntry._2.nonEmpty) {
          var partitionId = hostEntry._2.iterator.next
          localityResult(hostIndex) += partitionId
          // remove from sortedParts
          partitionIdMapHosts.get(partitionId) match {
            case Some(locs) =>
              locs.foreach { loc =>
                hostMapPartitionIds.get(loc) match {
                  case Some(parts) =>
                    parts.remove(partitionId)
                  case None =>
                }
              }
            case None =>
          }
        } else {
          noEmptyHostSet.remove(hostEntry._1)
        }
      }

      hostIndex = hostIndex + 1
      if (hostIndex == noEmptyHosts.length) {
        hostIndex = 0
      }
    }
    localityResult
  }

  /**
   * assign no locality partitions to each host
   */
  private def assignPartitionNoLocality(emptyHosts: mutable.Buffer[String],
      noEmptyHosts: mutable.Buffer[String],
      localityResult: mutable.Buffer[ArrayBuffer[Int]]): Array[ArrayBuffer[Int]] = {
    val noLocalityResult = new Array[ArrayBuffer[Int]](emptyHosts.length)
    LOGGER.info(s"non empty host: ${noEmptyHosts.length}, empty host: ${emptyHosts.length}")
    val avgNumber = prevPartitions.length / (noEmptyHosts.length + emptyHosts.length)
    for (i <- 0 until noLocalityResult.length) {
      noLocalityResult(i) = new ArrayBuffer[Int]
    }
    var noLocalityPartitionIndex = 0
    if (noLocalityPartitions.nonEmpty) {
      if (emptyHosts.nonEmpty) {
        // at first, assign avg number to empty node
        for (i <- 0 until avgNumber) {
          noLocalityResult.foreach { partitionIds =>
            if (noLocalityPartitionIndex < noLocalityPartitions.length) {
              partitionIds += noLocalityPartitions(noLocalityPartitionIndex)
              noLocalityPartitionIndex = noLocalityPartitionIndex + 1
            }
          }
        }
      }
      // still have no locality partitions
      // assign to all hosts
      if (noLocalityPartitionIndex < noLocalityPartitions.length) {
        var partIndex = 0
        for (i <- noLocalityPartitionIndex until noLocalityPartitions.length) {
          if (partIndex < localityResult.length) {
            localityResult(partIndex) += noLocalityPartitions(i)
          } else {
            noLocalityResult(partIndex - localityResult.length) += noLocalityPartitions(i)
          }
          partIndex = partIndex + 1
          if (partIndex == localityResult.length + noLocalityResult.length) {
            partIndex = 0
          }
        }
      }
    }
    noLocalityResult
  }

  /**
   * no locality repartition
   */
  private def repartitionNoLocality(): Array[Partition] = {
    // no locality repartition
    LOGGER.info("no locality partition")
    val prevPartIndexs = new Array[ArrayBuffer[Int]](numOfParts)
    for (i <- 0 until numOfParts) {
      prevPartIndexs(i) = new ArrayBuffer[Int]
    }
    for (i <- 0 until prevPartitions.length) {
      prevPartIndexs(i % numOfParts) += prevPartitions(i).index
    }
    prevPartIndexs.filter(_.nonEmpty).zipWithIndex.map { x =>
      CoalescedRDDPartition(x._2, prev, x._1.toArray, getLocation(x._2))
    }
  }

  private def repartitionLocality(): Array[Partition] = {
    LOGGER.info("locality partition")
    val hostMapPartitionIdsSeq = hostMapPartitionIds.toSeq
    // empty host seq
    val emptyHosts = hostMapPartitionIdsSeq.filter(_._2.isEmpty).map(_._1).toBuffer
    // non empty host array
    var tempNoEmptyHosts = hostMapPartitionIdsSeq.filter(_._2.nonEmpty)

    // 1. do locality repartition
    // sort host and partitions
    tempNoEmptyHosts = sortHostAndPartitions(tempNoEmptyHosts)
    // assign locality partition to non empty hosts
    val templocalityResult = assignPartitionNodeLocality(tempNoEmptyHosts)
    // collect non empty hosts and empty hosts
    val noEmptyHosts = mutable.Buffer[String]()
    val localityResult = mutable.Buffer[ArrayBuffer[Int]]()
    for(index <- 0 until templocalityResult.size) {
      if (templocalityResult(index).isEmpty) {
        emptyHosts += tempNoEmptyHosts(index)._1
      } else {
        noEmptyHosts += tempNoEmptyHosts(index)._1
        localityResult += templocalityResult(index)
      }
    }
    // 2. do no locality repartition
    // assign no locality partitions to all hosts
    val noLocalityResult = assignPartitionNoLocality(emptyHosts, noEmptyHosts, localityResult)

    // 3. generate CoalescedRDDPartition
    (0 until localityResult.length + noLocalityResult.length).map { index =>
      val ids = if (index < localityResult.length) {
        localityResult(index).toArray
      } else {
        noLocalityResult(index - localityResult.length).toArray
      }
      val loc = if (index < localityResult.length) {
        Some(noEmptyHosts(index))
      } else {
        Some(emptyHosts(index - localityResult.length))
      }
      LOGGER.info(s"CoalescedRDDPartition $index, ${ids.length}, $loc ")
      CoalescedRDDPartition(index, prev, ids, loc)
    }.filter(_.parentsIndices.nonEmpty).toArray

  }

  def run(): Array[Partition] = {
    // 1. group partitions by node
    groupByNode()
    LOGGER.info(s"partition: ${prevPartitions.length}, no locality: ${noLocalityPartitions.length}")
    val partitions = if (noLocality) {
      // 2.A no locality partition
      repartitionNoLocality()
    } else {
      // 2.B locality partition
      repartitionLocality()
    }
    DataLoadPartitionCoalescer.checkPartition(prevPartitions, partitions)
    partitions
  }
}

object DataLoadPartitionCoalescer {
  def getPreferredLocs(prev: RDD[_], p: Partition): Seq[TaskLocation] = {
    prev.context.getPreferredLocs(prev, p.index)
  }

  def getParentsIndices(p: Partition): Array[Int] = {
    p.asInstanceOf[CoalescedRDDPartition].parentsIndices
  }

  def checkPartition(prevParts: Array[Partition], parts: Array[Partition]): Unit = {
    val prevPartIds = new ArrayBuffer[Int]
    parts.foreach{ p =>
      prevPartIds ++= DataLoadPartitionCoalescer.getParentsIndices(p)
    }
    // all partitions must be arranged once.
    assert(prevPartIds.size == prevParts.size)
    val prevPartIdsMap = prevPartIds.map{ id =>
      (id, id)
    }.toMap
    prevParts.foreach{ p =>
      prevPartIdsMap.get(p.index) match {
        case None => assert(false, "partition " + p.index + " not found")
        case Some(_) =>
      }
    }
  }
}
