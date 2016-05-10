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

package org.apache.spark.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark._

import org.carbondata.core.load.BlockDetails

/**
 * this RDD use to combine blocks in node level
 * return (host,Array[BlockDetails])
 * @param prev
 */
class DummyLoadRDD(prev: NewHadoopRDD[LongWritable, Text])
  extends RDD[(String, BlockDetails)](prev) {

  override def getPartitions: Array[Partition] = firstParent[(LongWritable, Text)].partitions

  override def compute(theSplit: Partition,
                       context: TaskContext): Iterator[(String, BlockDetails)] = {
    new Iterator[(String, BlockDetails)] {
      val split = theSplit.asInstanceOf[NewHadoopPartition]
      var finished = false
      // added to make sure spark distributes tasks not to single node
      // giving sufficient time for spark to schedule
      Thread.sleep(5000);
      override def hasNext: Boolean = {
        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (String, BlockDetails) = {
        val host = TaskContext.get.taskMetrics.hostname
        val fileSplit = split.serializableHadoopSplit.value.asInstanceOf[FileSplit]
        val nodeBlocksDetail = new BlockDetails
        nodeBlocksDetail.setBlockOffset(fileSplit.getStart)
        nodeBlocksDetail.setBlockLength(fileSplit.getLength)
        nodeBlocksDetail.setFilePath(fileSplit.getPath.toString)
        (host, nodeBlocksDetail)
      }
    }
  }

}
