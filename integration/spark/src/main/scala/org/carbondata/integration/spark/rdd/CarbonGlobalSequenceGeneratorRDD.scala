/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.carbondata.core.carbon.CarbonDef.CubeDimension
import org.carbondata.integration.spark.KeyVal
import org.carbondata.integration.spark.load.{CarbonLoadModel, CarbonLoaderUtil}
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension


class CarbonGlobalDimensionsPartition(
                                      columns: Array[CarbonDimension], partitionColumns: Array[String], noPartition: Int, idx: Int)
  extends Partition {
  override val index: Int = idx
  val serializableHadoopSplit = columns
  //new SerializableWritable[Seq[CubeDimension]](columns)
  val partitionColumn = partitionColumns;
  val numberOfPartition = noPartition;

  override def hashCode(): Int = 41 * (41 + idx) + idx

}

/**
 * This RDD class is used to  create splits as per the region servers of Hbase  and compute each split in the respective node located in the same server by
 * using co-processor of Hbase.
 *
 * @author R00900208
 */
class CarbonGlobalSequenceGeneratorRDD[K, V](
                                             sc: SparkContext,
                                             keyClass: KeyVal[K, V], carbonLoadModel: CarbonLoadModel,
                                             var storeLocation: String,
                                             hdfsStoreLocation: String,
                                             partitioner: Partitioner,
                                             currentRestructNumber: Integer)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    val splits = CarbonLoaderUtil.getDimensionSplit(carbonLoadModel.getCarbonDataLoadSchema, carbonLoadModel.getCubeName(), partitioner.partitionCount)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      //      result(i) = new CarbonDataPartition(id, i, splits(i))
      result(i) = new CarbonGlobalDimensionsPartition(splits(i), partitioner.partitionColumn, partitioner.partitionCount, i)
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[CarbonGlobalDimensionsPartition]
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
        storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
      }
      //TO-DO need to check if this code is required
      //CarbonLoaderUtil.generateGlobalSurrogates(carbonLoadModel, hdfsStoreLocation, split.numberOfPartition, split.partitionColumn, split.serializableHadoopSplit, currentRestructNumber)

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !false
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = new CarbonKey(null)
        val value = new CarbonValue(null)
        keyClass.getKey(row, value)
      }
    }
    iter
  }

}

