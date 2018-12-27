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

package org.apache.carbondata.spark

import org.apache.spark.Partitioner

import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.scan.partition.{HashPartitioner => JavaHashPartitioner, ListPartitioner => JavaListPartitioner, RangePartitioner => JavaRangePartitioner}
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException

object PartitionFactory {

  def getPartitioner(partitionInfo: PartitionInfo): Partitioner = {
    partitionInfo.getPartitionType match {
      case PartitionType.HASH => new HashPartitioner(partitionInfo.getNumPartitions)
      case PartitionType.LIST => new ListPartitioner(partitionInfo)
      case PartitionType.RANGE => new RangePartitioner(partitionInfo)
      case partitionType =>
        throw new CarbonDataLoadingException(s"Unsupported partition type: $partitionType")
    }
  }
}

class HashPartitioner(partitions: Int) extends Partitioner {

  private val partitioner = new JavaHashPartitioner(partitions)

  override def numPartitions: Int = partitioner.numPartitions()

  override def getPartition(key: Any): Int = partitioner.getPartition(key)
}

class ListPartitioner(partitionInfo: PartitionInfo) extends Partitioner {

  private val partitioner = new JavaListPartitioner(partitionInfo)

  override def numPartitions: Int = partitioner.numPartitions()

  override def getPartition(key: Any): Int = partitioner.getPartition(key)
}

class RangePartitioner(partitionInfo: PartitionInfo) extends Partitioner {

  private val partitioner = new JavaRangePartitioner(partitionInfo)

  override def numPartitions: Int = partitioner.numPartitions()

  override def getPartition(key: Any): Int = partitioner.getPartition(key)
}
