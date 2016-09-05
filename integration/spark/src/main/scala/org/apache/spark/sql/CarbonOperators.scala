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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode

case class CarbonScan(
    var attributesRaw: Seq[Attribute],
    relationRaw: CarbonRelation,
    dimensionPredicatesRaw: Seq[Expression],
    useUnsafeCoversion: Boolean = true)(@transient val ocRaw: SQLContext) extends LeafNode {

  lazy val builder = new CarbonScanRDDBuilder(
    ocRaw,
    relationRaw,
    attributesRaw,
    dimensionPredicatesRaw,
    useUnsafeCoversion
  )

  def attributesNeedToDecode = builder.attributesNeedToDecode

  def unprocessedExprs = builder.unprocessedExprs

  override def outputsUnsafeRows: Boolean =
    (builder.attributesNeedToDecode.size() == 0) && useUnsafeCoversion

  override def doExecute(): RDD[InternalRow] = {
    val inputRdd = builder.build
    inputRdd.mapPartitions { iter =>
      val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): InternalRow =
          if (outputsUnsafeRows) {
            unsafeProjection(new GenericMutableRow(iter.next()))
          } else {
            new GenericMutableRow(iter.next())
          }
      }
    }
  }

  def output: Seq[Attribute] = {
    attributesRaw
  }

}


