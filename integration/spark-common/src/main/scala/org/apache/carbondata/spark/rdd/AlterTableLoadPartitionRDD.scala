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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.AlterPartitionModel
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.partition.spliter.RowResultProcessor
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.spark.AlterPartitionResult
import org.apache.carbondata.spark.util.CommonUtil

class AlterTableLoadPartitionRDD[K, V](alterPartitionModel: AlterPartitionModel,
    result: AlterPartitionResult[K, V],
    partitionIds: Seq[String],
    bucketId: Int,
    identifier: AbsoluteTableIdentifier,
    prev: RDD[Array[AnyRef]])
  extends CarbonRDD[(K, V)](alterPartitionModel.sqlContext.sparkSession, prev) {

  val carbonLoadModel = alterPartitionModel.carbonLoadModel
  val segmentId = alterPartitionModel.segmentId
  val oldPartitionIds = alterPartitionModel.oldPartitionIds
  val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
  val databaseName = carbonTable.getDatabaseName
  val factTableName = carbonTable.getTableName
  val partitionInfo = carbonTable.getPartitionInfo(factTableName)

  override protected def internalGetPartitions: Array[Partition] = {
    val sc = alterPartitionModel.sqlContext.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", "DDL")
    sc.setLocalProperty("spark.job.interruptOnCancel", "true")
    firstParent[Array[AnyRef]].partitions
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val rows = firstParent[Array[AnyRef]].iterator(split, context).toList.asJava
    val iter = new Iterator[(K, V)] {
      val partitionId: Int = partitionInfo.getPartitionId(split.index)
      carbonLoadModel.setTaskNo(String.valueOf(partitionId))
      carbonLoadModel.setSegmentId(segmentId)

      CommonUtil.setTempStoreLocation(split.index, carbonLoadModel,
        isCompactionFlow = false, isAltPartitionFlow = true)
      val tempStoreLoc: Array[String] = CarbonDataProcessorUtil.getLocalDataFolderLocation(
        carbonTable, carbonLoadModel.getTaskNo, segmentId, false, true)

      val loadStatus: Boolean = if (rows.isEmpty) {
        LOGGER.info("After repartition this split, NO target rows to write back.")
        true
      } else {
        val segmentProperties = PartitionUtils.getSegmentProperties(identifier,
          segmentId, partitionIds.toList, oldPartitionIds, partitionInfo, carbonTable)
        val processor = new RowResultProcessor(
          carbonTable,
          carbonLoadModel,
          segmentProperties,
          tempStoreLoc,
          bucketId)
        try {
          processor.execute(rows)
        } catch {
          case e: Exception =>
            sys.error(s"Exception when executing Row result processor ${ e.getMessage }")
        } finally {
          TableProcessingOperations
            .deleteLocalDataLoadFolderLocation(carbonLoadModel, false, true)
        }
      }

      val loadResult = segmentId
      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(loadResult, loadStatus)
      }
    }
    iter
  }
}
