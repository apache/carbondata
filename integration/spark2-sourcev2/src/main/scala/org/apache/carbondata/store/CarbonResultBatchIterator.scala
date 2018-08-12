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

import java.util

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.apache.carbondata.store.devapi.ResultBatch

/**
 * Returns a Result Batch iterator
 *
 * @param context
 * @param resultIterator
 * @tparam T
 */
class CarbonResultBatchIterator[T](var context: TaskContext,
                                   var resultIterator: util.Iterator[_ <: ResultBatch[T]])
  extends Iterator[Any] {

  private var resultBatch: ResultBatch[T] = null

  override def hasNext: Boolean = {
    if (context.isInterrupted) {
      throw new TaskKilledException
    }
    (resultBatch != null && resultBatch.hasNext) || resultIterator.hasNext
  }

  override def next(): Any = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of result")
    }
    if (resultBatch == null || !resultBatch.hasNext) {
      resultBatch = resultIterator.next()
    }
    Row.fromSeq(resultBatch.next().asInstanceOf[GenericInternalRow].values)
  }
}
