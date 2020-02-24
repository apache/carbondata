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

import scala.collection.JavaConverters._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.spark.rdd.CarbonRDD

class InvalidateSegmentCacheRDD(@transient private val ss: SparkSession, carbonTable: CarbonTable,
    invalidSegmentIds: List[String]) extends CarbonRDD[String](ss, Nil) {

  val executorsList: Array[String] = DistributionUtil.getExecutors(ss.sparkContext).flatMap {
    case (host, executors) =>
      executors.map {
        executor => s"executor_${host}_$executor"
      }
  }.toArray

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[String] = {
    DataMapStoreManager.getInstance().clearInvalidSegments(carbonTable, invalidSegmentIds.asJava)
    Iterator.empty
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.asInstanceOf[DataMapRDDPartition].getLocations != null) {
      split.asInstanceOf[DataMapRDDPartition].getLocations.toSeq
    } else {
      Seq()
    }
  }

  override protected def internalGetPartitions: Array[Partition] = {
    if (invalidSegmentIds.isEmpty) {
      Array()
    } else {
      executorsList.zipWithIndex.map {
        case (executor, idx) =>
          // create a dummy split for each executor to accumulate the cache size.
          val dummySplit = new CarbonInputSplit()
          dummySplit.setLocation(Array(executor))
          new DataMapRDDPartition(id, idx, List(dummySplit), Array(executor))
      }
    }
  }
}
