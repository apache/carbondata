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
package org.apache.spark.util

import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.executor.InputMetrics
import org.apache.spark.SerializableWritable
import org.apache.spark.TaskContext

import org.apache.carbondata.hadoop.CarbonMultiBlockSplit

/**
 * It gives statistics of number of bytes and record read
 */
trait InputMetricsStats extends Serializable {
  /**
   * intialize hdfs read callback
   */
  def initBytesReadCallback(context: TaskContext, carbonMultiBlockSplit: CarbonMultiBlockSplit)

  /**
   * increment if record is read
   */
  def incrementRecordRead(recordRead: Long)

  /**
   * update hdfs byte read
   */
  def updateBytesRead(serializableWritable: SerializableWritable[CarbonMultiBlockSplit])
}
