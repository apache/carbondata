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

import scala.reflect.ClassTag

import org.apache.spark._

import org.apache.carbondata.spark.rdd.CarbonRDD


// This RDD distributes previous RDD data based on number of nodes. i.e., one partition for one node

class UpdateCoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    nodeList: Array[String])
  extends CarbonRDD[T](prev.context, Nil, prev.sparkContext.hadoopConfiguration) {

  override def getPartitions: Array[Partition] = {
    new DataLoadPartitionCoalescer(prev, nodeList).run
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[T] = {
    // This iterator combines data from all the parent partitions
    new Iterator[T] {
      val parentPartitionIter = split.asInstanceOf[CoalescedRDDPartition].parents.iterator
      var currentDataIter: Iterator[T] = _
      val prevRdd = firstParent[T]

      def hasNext: Boolean = {
        while (currentDataIter == null ||
               !currentDataIter.hasNext &&
               parentPartitionIter.hasNext) {
          val currentPartition = parentPartitionIter.next()
          currentDataIter = prevRdd.compute(currentPartition, context)
        }
        if (currentDataIter == null) {
          false
        } else {
          currentDataIter.hasNext
        }
      }

      def next: T = {
        currentDataIter.next()
      }
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] = {
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
      }
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   *
   * @param partition
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}
