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
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.spark.rdd.CarbonRDD


case class DataLoadPartitionWrap[T: ClassTag](rdd: RDD[T], partition: Partition)

class DataLoadCoalescedRDD[T: ClassTag](
    @transient private val sparkSession: SparkSession,
    @transient private var prev: RDD[T],
    nodeList: Array[String])
  extends CarbonRDD[DataLoadPartitionWrap[T]](sparkSession, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    new DataLoadPartitionCoalescer(prev, nodeList).run
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[DataLoadPartitionWrap[T]] = {
    new Iterator[DataLoadPartitionWrap[T]] {
      val iter = split.asInstanceOf[CoalescedRDDPartition].parents.iterator
      def hasNext = iter.hasNext
      def next: DataLoadPartitionWrap[T] = {
        DataLoadPartitionWrap(firstParent[T], iter.next())
      }
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   * @param partition
   * @return the machine most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}
