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

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.index.{IndexInputFormat, IndexStoreManager}
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper
import org.apache.carbondata.core.indexstore.SegmentWrapper
import org.apache.carbondata.spark.rdd.CarbonRDD

class SegmentPruneRDD(@transient private val ss: SparkSession,
    indexInputFormat: IndexInputFormat)
  extends CarbonRDD[(String, SegmentWrapper)](ss, Nil) {

  override protected def internalGetPartitions: Array[Partition] = {
    new DistributedPruneRDD(ss, indexInputFormat).partitions
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, SegmentWrapper)] = {
    val inputSplits = split.asInstanceOf[IndexRDDPartition].inputSplit
    val segments = inputSplits.map(_
      .asInstanceOf[IndexInputSplitWrapper].getDistributable.getSegment)
    segments.foreach(_.setReadCommittedScope(indexInputFormat.getReadCommittedScope))
    if (indexInputFormat.getInvalidSegments.size > 0) {
      // clear the segmentMap and from cache in executor when there are invalid segments
      IndexStoreManager.getInstance().clearInvalidSegments(indexInputFormat.getCarbonTable,
        indexInputFormat.getInvalidSegments)
    }
    val blockletMap = IndexStoreManager.getInstance
      .getDefaultIndex(indexInputFormat.getCarbonTable)
    val prunedSegments = blockletMap
      .pruneSegments(segments.toList.asJava, indexInputFormat.getFilterResolverIntf)
    val executorIP = s"${ SparkEnv.get.blockManager.blockManagerId.host }_${
      SparkEnv.get.blockManager.blockManagerId.executorId
    }"
    val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
      CacheProvider.getInstance().getCarbonCache.getCurrentSize
    } else {
      0L
    }
    val value = (executorIP + "_" + cacheSize.toString, new SegmentWrapper(prunedSegments))
    Iterator(value)
  }
}
