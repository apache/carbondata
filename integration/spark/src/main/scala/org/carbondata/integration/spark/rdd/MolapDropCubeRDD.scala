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
import org.carbondata.core.metadata.MolapMetadata
import org.carbondata.integration.spark.KeyVal
import org.carbondata.integration.spark.util.MolapQueryUtil
import org.carbondata.query.datastorage.InMemoryCubeStore
import org.carbondata.query.scanner.impl.{MolapKey, MolapValue}

import scala.collection.JavaConversions._

class MolapDropCubeRDD[K, V](
                              sc: SparkContext,
                              keyClass: KeyVal[K, V],
                              schemaName: String,
                              cubeName: String,
                              partitioner: Partitioner)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    val splits = MolapQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapLoadPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext) = {

    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[MolapLoadPartition]

      val partitionCount = partitioner.partitionCount
      for (a <- 0 until partitionCount) {
        val cubeUniqueName = schemaName + "_" + a + "_" + cubeName + "_" + a
        val cube = MolapMetadata.getInstance().getCube(cubeUniqueName)
        if (InMemoryCubeStore.getInstance().getCubeNames().contains(cubeUniqueName)) {
          InMemoryCubeStore.getInstance().clearCache(cubeUniqueName)
          val tables = cube.getMetaTableNames()
          tables.foreach { tableName =>
            val tabelUniqueName = cubeUniqueName + '_' + tableName
            InMemoryCubeStore.getInstance().clearTableAndCurrentRSMap(tabelUniqueName)
          }
        }
      }

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
        val row = new MolapKey(null)
        val value = new MolapValue(null)
        keyClass.getKey(row, value)
      }
    }
    iter
  }
}

