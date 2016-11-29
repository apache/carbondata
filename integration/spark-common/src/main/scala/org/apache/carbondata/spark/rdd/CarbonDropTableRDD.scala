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

import org.apache.carbondata.spark.Value
import org.apache.carbondata.spark.util.CarbonQueryUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class CarbonDropTableRDD[V: ClassTag](
    sc: SparkContext,
    valueClass: Value[V],
    databaseName: String,
    tableName: String)
  extends RDD[V](sc, Nil) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    val splits = CarbonQueryUtil.getTableSplits(databaseName, tableName, null)
    splits.zipWithIndex.map { s =>
      new CarbonLoadPartition(id, s._2, s._1)
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[V] = {

    val iter = new Iterator[V] {
      // TODO: Clear Btree from memory

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        valueClass.getValue(null)
      }
    }
    iter
  }
}

