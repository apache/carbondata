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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.SegmentStatus

/**
 * This is a partitioner class for dividing the newly added columns into partitions
 *
 * @param rddId
 * @param idx
 * @param schema
 */
class DropColumnPartition(rddId: Int, idx: Int, schema: ColumnSchema) extends Partition {
  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  val columnSchema = schema
}

/**
 * This class is aimed at generating dictionary file for the newly added columns
 */
class AlterTableDropColumnRDD[K, V](sc: SparkContext,
    @transient newColumns: Seq[ColumnSchema],
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonStorePath: String)
  extends CarbonRDD[(Int, SegmentStatus)](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    newColumns.zipWithIndex.map { column =>
      new DropColumnPartition(id, column._2, column._1)
    }.toArray
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(Int, SegmentStatus)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val status = SegmentStatus.SUCCESS
    val iter = new Iterator[(Int, SegmentStatus)] {
      try {
        val columnSchema = split.asInstanceOf[DropColumnPartition].columnSchema
        if (columnSchema.hasEncoding(Encoding.DICTIONARY) &&
            !columnSchema.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          ManageDictionaryAndBTree
            .deleteDictionaryFileAndCache(columnSchema, carbonTableIdentifier, carbonStorePath)
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex, ex.getMessage)
          throw ex
      }

      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        } else {
          !finished
        }
      }

      override def next(): (Int, SegmentStatus) = {
        (split.index, status)
      }
    }
    iter
  }

}
