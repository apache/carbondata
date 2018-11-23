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

package org.apache.carbondata.datamap

import scala.collection.JavaConverters._

import org.apache.spark.Partition
import org.apache.spark.rdd.CarbonMergeFilePartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.datamap.bloom.BloomIndexFileStore
import org.apache.carbondata.spark.rdd.CarbonRDD


/**
 * RDD to merge all bloomindex files of specified segment for bloom datamap
 */
class CarbonMergeBloomIndexFilesRDD(
  @transient private val ss: SparkSession,
  carbonTable: CarbonTable,
  segmentIds: Seq[String],
  bloomDatamapNames: Seq[String],
  bloomIndexColumns: Seq[Seq[String]])
  extends CarbonRDD[String](ss, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    segmentIds.zipWithIndex.map {s =>
      CarbonMergeFilePartition(id, s._2, s._1)
    }.toArray
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val tablePath = carbonTable.getTablePath
    val split = theSplit.asInstanceOf[CarbonMergeFilePartition]
    logInfo("Merging bloom index files of " +
      s"segment ${split.segmentId} for ${carbonTable.getTableName}")

    bloomDatamapNames.zipWithIndex.map( dm => {
      val dmSegmentPath = CarbonTablePath.getDataMapStorePath(
        tablePath, split.segmentId, dm._1)
      BloomIndexFileStore.mergeBloomIndexFile(dmSegmentPath, bloomIndexColumns(dm._2).asJava)
    })

    val iter = new Iterator[String] {
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

