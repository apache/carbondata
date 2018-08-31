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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd.CarbonRDD

case class CarbonMergeFilePartition(rddId: Int, idx: Int, segmentId: String)
  extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * RDD to merge all carbonindex files of each segment to carbonindex file into the same segment.
 * @param ss
 * @param carbonTable
 * @param segments segments to be merged
 */
class CarbonMergeFilesRDD(
  @transient private val ss: SparkSession,
  carbonTable: CarbonTable,
  segments: Seq[String],
  segmentFileNameToSegmentIdMap: java.util.Map[String, String],
  isHivePartitionedTable: Boolean,
  readFileFooterFromCarbonDataFile: Boolean)
  extends CarbonRDD[String](ss, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    segments.zipWithIndex.map {s =>
      CarbonMergeFilePartition(id, s._2, s._1)
    }.toArray
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val tablePath = carbonTable.getTablePath
    val iter = new Iterator[String] {
      val split = theSplit.asInstanceOf[CarbonMergeFilePartition]
      logInfo("Merging carbon index files of segment : " +
              CarbonTablePath.getSegmentPath(tablePath, split.segmentId))

      if (isHivePartitionedTable) {
        CarbonLoaderUtil
          .mergeIndexFilesinPartitionedSegment(carbonTable, split.segmentId,
            segmentFileNameToSegmentIdMap.get(split.segmentId))
      } else {
        new CarbonIndexFileMergeWriter(carbonTable)
          .mergeCarbonIndexFilesOfSegment(split.segmentId,
            tablePath,
            readFileFooterFromCarbonDataFile,
            segmentFileNameToSegmentIdMap.get(split.segmentId))
      }

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

