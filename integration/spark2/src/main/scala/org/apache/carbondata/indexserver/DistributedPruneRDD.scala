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
package org.apache.carbondata.indexserver

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.core.indexstore.ExtendedBlocklet
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.spark.rdd.CarbonRDD


class DistributedPruneRDD(@transient private val ss: SparkSession,
    dataMapFormat: DistributableDataMapFormat,
    resolverIntf: FilterResolverIntf)
  extends CarbonRDD[(String, ExtendedBlocklet)](ss, Nil) {

  override protected def internalGetPartitions: Array[Partition] = {
    Array()
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, ExtendedBlocklet)] = {
    Nil.iterator
  }
}
