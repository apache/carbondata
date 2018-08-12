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

package org.apache.carbondata.store

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}

import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier
import org.apache.carbondata.spark.rdd.CarbonRDD
import org.apache.carbondata.store.devapi.Scanner


/**
 * Scan RDD supporting Table select with pruning and Filter
 *
 * @param sc
 * @param tableIdentifier
 * @param scanner
 * @tparam T
 * @tparam U
 */
class CarbonStoreScanRDD[T: ClassTag, U](sc: SparkContext,
                                         tableIdentifier: TableIdentifier,
                                         scanner: Scanner[U],
                                         filterExpression: Expression)
  extends CarbonRDD[T](sc, Nil, sc.hadoopConfiguration) {

  override def internalCompute(split: Partition, context: TaskContext): scala.Iterator[T] = {
    val splitUnit = split.asInstanceOf[CarbonStorePartition].scanUnit
    val resultIterator = scanner.scan(splitUnit)
    new CarbonResultBatchIterator[U](context, resultIterator).asInstanceOf[Iterator[T]]
  }

  override protected def getPartitions: Array[Partition] = {
    val scanUnits = scanner.prune(tableIdentifier, filterExpression)
    val result = new Array[Partition](scanUnits.size())
    for (i <- 0 until scanUnits.size()) {
      result(i) = new CarbonStorePartition(scanUnits.get(i), i)
    }
    result
  }
}
