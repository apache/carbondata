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

package org.carbondata.spark.rdd

import scala.collection.JavaConverters._

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner

import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}
import org.carbondata.spark.KeyVal
import org.carbondata.spark.util.CarbonQueryUtil

class CarbonDeleteLoadRDD[K, V](
                                 sc: SparkContext,
                                 keyClass: KeyVal[K, V],
                                 loadId: Int,
                                 schemaName: String,
                                 cubeName: String,
                                 partitioner: Partitioner)
  extends RDD[(K, V)](sc, Nil) with Logging {
  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    val splits = CarbonQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new CarbonLoadPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[CarbonLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)
      // TODO call CARBON delete API

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
    logInfo("********Deleting***************")
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations.asScala
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

