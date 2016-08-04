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

import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.hadoop.CarbonInputSplit

import scala.collection.JavaConverters._
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.carbondata.spark.PartitionResult
import org.apache.carbondata.spark.partition.api.impl.CSVFilePartitioner
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.CarbonQueryUtil


class CarbonSparkRawDataPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * This RDD class is used to  create splits the fact csv store to various partitions as per
 * configuration  and compute each split in the respective node located in the server.
 * .
 */
class CarbonDataPartitionRDD[K, V](
    sc: SparkContext,
    results: PartitionResult[K, V],
    databaseName: String,
    tableName: String,
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
    val splits = CarbonQueryUtil
      .getPartitionSplits(sourcePath, partitioner.nodeList, partitioner.partitionCount)
    splits.zipWithIndex.map {s =>
      new CarbonSparkRawDataPartition(id, s._2, s._1)
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[CarbonSparkRawDataPartition]
      StandardLogService
        .setThreadName(split.serializableHadoopSplit.value.getPartition.getUniqueID, null)
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      val csvPart = new CSVFilePartitioner(partitioner.partitionClass, sourcePath)
      csvPart.splitFile(databaseName, tableName,
        split.serializableHadoopSplit.value.getPartition.getFilesPath, targetFolder,
        partitioner.nodeList.toList.asJava, partitioner.partitionCount, partitioner.partitionColumn,
        requiredColumns, delimiter, quoteChar, headers, escapeChar, multiLine)

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
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkRawDataPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations.asScala
    logInfo("Host Name : " + s.head + s.length)
    s
  }
}

