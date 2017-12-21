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

package org.apache.carbondata.spark.rdd

import scala.collection.JavaConverters._

import org.apache.spark.{Partition, SparkContext, TaskContext}

import org.apache.carbondata.core.metadata.PartitionMapFileStore
import org.apache.carbondata.core.util.path.CarbonTablePath

case class CarbonDropPartition(rddId: Int, val idx: Int, segmentPath: String)
  extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * RDD to drop the partitions from partition mapper files of all segments.
 * @param sc
 * @param tablePath
 * @param segments segments to be merged
 */
class CarbonDropPartitionRDD(
    sc: SparkContext,
    tablePath: String,
    segments: Seq[String],
    partitions: Seq[String],
    uniqueId: String)
  extends CarbonRDD[String](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    segments.zipWithIndex.map {s =>
      CarbonDropPartition(id, s._2, CarbonTablePath.getSegmentPath(tablePath, s._1))
    }.toArray
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val iter = new Iterator[String] {
      val split = theSplit.asInstanceOf[CarbonDropPartition]
      logInfo("Dropping partition information from : " + split.segmentPath)

      new PartitionMapFileStore().dropPartitions(
        split.segmentPath,
        partitions.toList.asJava,
        uniqueId)

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): String = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        ""
      }

    }
    iter
  }

}

/**
 * This RDD is used for committing the partitions which were removed in before step. It just removes
 * old mapper files and related data files.
 * @param sc
 * @param tablePath
 * @param segments segments to be merged
 */
class CarbonDropPartitionCommitRDD(
    sc: SparkContext,
    tablePath: String,
    segments: Seq[String],
    success: Boolean,
    uniqueId: String)
  extends CarbonRDD[String](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    segments.zipWithIndex.map {s =>
      CarbonDropPartition(id, s._2, CarbonTablePath.getSegmentPath(tablePath, s._1))
    }.toArray
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val iter = new Iterator[String] {
      val split = theSplit.asInstanceOf[CarbonDropPartition]
      logInfo("Commit partition information from : " + split.segmentPath)

      new PartitionMapFileStore().commitPartitions(split.segmentPath, uniqueId, success)

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): String = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        ""
      }

    }
    iter
  }

}
