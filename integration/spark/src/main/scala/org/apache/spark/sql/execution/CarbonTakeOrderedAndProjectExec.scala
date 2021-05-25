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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{CarbonTakeOrderedAndProjectExecHelper, CarbonToSparkAdapter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}

// To skip the order at map task
case class CarbonTakeOrderedAndProjectExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    skipMapOrder: Boolean = false,
    readFromHead: Boolean = true) extends CarbonTakeOrderedAndProjectExecHelper(sortOrder,
      limit, skipMapOrder, readFromHead) {

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)

  override def executeCollect(): Array[InternalRow] = {
    val ordering = new LazilyGeneratedOrdering(sortOrder, child.output)
    val rdd = child.execute().map(_.copy())
    val data = takeOrdered(rdd, limit, skipMapOrder, readFromHead)(ordering)
    if (projectList != child.output) {
      val projection = UnsafeProjection.create(projectList, child.output)
      data.map(r => projection(r).copy())
    } else {
      data
    }
  }

  def takeOrdered(rdd: RDD[InternalRow],
      num: Int,
      skipMapOrder: Boolean = false,
      readFromHead: Boolean = true)(implicit ord: Ordering[InternalRow]): Array[InternalRow] = {
    if (!skipMapOrder) {
      return rdd.takeOrdered(num)(ord)
    }
    // new ShuffledRowRDD by skipping the order at map task as column data is already sorted
    if (num == 0) {
      Array.empty
    } else {
      val mapRDDs = rdd.mapPartitions { items =>
        if (readFromHead) {
          items.slice(0, num)
        } else {
          items.drop(items.size - num)
        }
      }
      if (mapRDDs.partitions.length == 0) {
        Array.empty
      } else {
        mapRDDs.collect().sorted(ord)
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val localTopK: RDD[InternalRow] = {
      child.execute().map(_.copy()).mapPartitions { iter =>
        if (skipMapOrder) {
          // new ShuffledRowRDD by skipping the order at map task as column data is already sorted
          if (readFromHead) {
            iter.slice(0, limit)
          } else {
            iter.drop(iter.size - limit)
          }
        } else {
          org.apache.spark.util.collection.Utils.takeOrdered(iter, limit)(ord)
        }
      }
    }
    // update with modified RDD (localTopK)
    val shuffled =
      CarbonToSparkAdapter.createShuffledRowRDD(sparkContext, localTopK, child, serializer)
    shuffled.mapPartitions { iter =>
      val topK = org.apache.spark.util.collection.Utils.takeOrdered(iter.map(_.copy()), limit)(ord)
      if (projectList != child.output) {
        val projection = UnsafeProjection.create(projectList, child.output)
        topK.map(r => projection(r))
      } else {
        topK
      }
    }
  }

}
