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

import java.io.DataOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.spark.rdd.CarbonRDD

case class CarbonCopyFilePartition(rddId: Int, idx: Int, filePath: String)
  extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * RDD to copy all files of segment to destination folder.
 *
 * @param ss
 * @param segment segment to be copied
 */
class CarbonCopyFilesRDD(
    @transient private val ss: SparkSession,
    @transient segment: Segment,
    destLocation: String)
  extends CarbonRDD[String](ss, Nil) {


  override def internalGetPartitions: Array[Partition] = {
    val carbonFile = FileFactory.getCarbonFile(segment.getSegmentPath, hadoopConf)
    carbonFile.listFiles().zipWithIndex.map { f =>
      CarbonCopyFilePartition(id, f._2, f._1.getAbsolutePath)
    }
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val iter = new Iterator[String] {
      val split = theSplit.asInstanceOf[CarbonCopyFilePartition]
      logInfo("copying file : " +
              split.filePath + "to : " + destLocation)

      private val inputStream =
        FileFactory.getDataInputStream(split.filePath,
          FileFactory.getFileType(split.filePath), getConf)

      val destPath = new Path(destLocation, new Path(split.filePath).getName)
      val outputStream: DataOutputStream =
        FileFactory.getDataOutputStream(destPath.toString, FileFactory.getFileType(destLocation))

      IOUtils.copyBytes(inputStream, outputStream, getConf)

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): String = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        ""
      }
    }
    iter
  }
}
