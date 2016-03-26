/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.carbondata.integration.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.carbondata.common.logging.impl.StandardLogService
import org.carbondata.integration.spark.PartitionResult
import org.carbondata.integration.spark.partition.api.impl.CSVFilePartitioner
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.MolapQueryUtil

import scala.collection.JavaConversions._


class MolapDataPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * This RDD class is used to  create splits the fact csv store to various partitions as per configuration  and compute each split in the respective node located in the
 * server.
 * .
 *
 * @author R00900208
 */
class MolapDataPartitionRDD[K, V](
                                   sc: SparkContext,
                                   //keyClass: KeyVal[K,V],
                                   results: PartitionResult[K, V],
                                   schemaName: String,
                                   cubeName: String,
                                   sourcePath: String,
                                   targetFolder: String,
                                   requiredColumns: Array[String],
                                   headers: String,
                                   delimiter: String,
                                   quoteChar: String,
                                   escapeChar: String,
                                   multiLine: Boolean,
                                   partitioner: Partitioner)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    val splits = MolapQueryUtil.getPartitionSplits(sourcePath, partitioner.nodeList, partitioner.partitionCount)
    //
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapDataPartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[MolapDataPartition]
      StandardLogService.setThreadName(split.serializableHadoopSplit.value.getPartition().getUniqueID(), null);
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      val csvPart = new CSVFilePartitioner(partitioner.partitionClass, sourcePath)
      csvPart.splitFile(schemaName, cubeName, split.serializableHadoopSplit.value.getPartition().getFilesPath, targetFolder, partitioner.nodeList.toList, partitioner.partitionCount, partitioner.partitionColumn, requiredColumns, delimiter, quoteChar, headers, escapeChar, multiLine)

      var finished = false

      override def hasNext: Boolean = {
        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (K, V) = {
        results.getKey(partitioner.partitionCount, csvPart.isPartialSuccess)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapDataPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

