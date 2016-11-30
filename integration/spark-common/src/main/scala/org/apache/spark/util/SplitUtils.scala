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

import org.apache.carbondata.core.load.BlockDetails
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD}

/*
 * this object use to handle file splits
 */
object SplitUtils {

  /**
   * get file splits,return Array[BlockDetails], if file path is empty,then return empty Array
   *
   */
  def getSplits(path: String, sc: SparkContext): Array[BlockDetails] = {
    val filePath = FileUtils.getPaths(path)
    if (filePath == null || filePath.isEmpty) {
      // return a empty block details
      Array[BlockDetails]()
    } else {
      // clone the hadoop configuration
      val hadoopConfiguration = new Configuration(sc.hadoopConfiguration)
      // set folder or file
      hadoopConfiguration.set(FileInputFormat.INPUT_DIR, filePath)
      hadoopConfiguration.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
      val newHadoopRDD = new NewHadoopRDD[LongWritable, Text](
        sc,
        classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        hadoopConfiguration)
      val splits: Array[FileSplit] = newHadoopRDD.getPartitions.map { part =>
        part.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value.asInstanceOf[FileSplit]
      }
      splits.map { block =>
        new BlockDetails(block.getPath,
          block.getStart,
          block.getLength,
          block.getLocations
        )
      }
    }
  }
}
