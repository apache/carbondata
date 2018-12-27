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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore

case class CarbonDropPartition(rddId: Int, idx: Int, segment: Segment)
  extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * RDD to drop the partitions from segment files of all segments.
 * @param ss
 * @param tablePath
 * @param segments segments to be cleaned
 */
class CarbonDropPartitionRDD(
    @transient private val ss: SparkSession,
    tablePath: String,
    segments: Seq[Segment],
    partitions: util.List[PartitionSpec],
    uniqueId: String)
  extends CarbonRDD[(String, String)](ss, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    segments.zipWithIndex.map {s =>
      CarbonDropPartition(id, s._2, s._1)
    }.toArray
  }

  override def internalCompute(
      theSplit: Partition,
      context: TaskContext): Iterator[(String, String)] = {
    val iter = new Iterator[(String, String)] {
      val split = theSplit.asInstanceOf[CarbonDropPartition]
      logInfo("Dropping partition information from : " + split.segment)
      val toBeDeletedSegments = new util.ArrayList[String]()
      val toBeUpdateSegments = new util.ArrayList[String]()
      new SegmentFileStore(
        tablePath,
        split.segment.getSegmentFileName).dropPartitions(
        split.segment,
        partitions,
        uniqueId,
        toBeDeletedSegments,
        toBeUpdateSegments)

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (String, String) = {
        finished = true
        (toBeUpdateSegments.asScala.mkString(","), toBeDeletedSegments.asScala.mkString(","))
      }
    }
    iter
  }

}
