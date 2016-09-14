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

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException

import org.apache.carbondata.core.load.BlockDetails
import org.apache.carbondata.processing.csvreaderstep.{UnivocityCsvParser, UnivocityCsvParserVo}

case class CsvPartition(val index: Int,
                        val blockList: util.List[BlockDetails]) extends Partition

class CarbonCsvParserRDD(sc: SparkContext,
                         vo: UnivocityCsvParserVo,
                         requiredColumn: Array[Int],
                         @transient conf: Configuration)
  extends RDD[Array[String]](sc, Nil) with SparkHadoopMapReduceUtil {
  override def compute(split: Partition, context: TaskContext): Iterator[Array[String]] = {
    new Iterator[Array[String]] {
      val theSplit = split.asInstanceOf[CsvPartition]
      val numberOfColumn = vo.getNumberOfColumns
      val numberOfRequired = requiredColumn.length
      lazy val parser: UnivocityCsvParser = {
        vo.setBlockDetailsList(theSplit.blockList)
        val tmp = new UnivocityCsvParser(vo)
        tmp.initialize
        tmp
      }

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        } else {
          parser.hasMoreRecords
        }
      }

      override def next(): Array[String] = {
        var row = parser.getNextRecord
        if (row.length < numberOfColumn) {
          val temp = new Array[String](numberOfColumn)
          System.arraycopy(row, 0, temp, 0, row.length)
          row = temp;
        }
        val rtn = new Array[String](numberOfRequired)
        for (i <- 0 until numberOfRequired) {
          rtn(i) = row(requiredColumn(i))
        }
        rtn
      }
    }
  }

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[Partition] = {
    val inputFormat = new TextInputFormat
    val jobContext = newJobContext(conf, jobId)
    val rawSplits = inputFormat.getSplits(jobContext)
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      val rawSplit = rawSplits.get(i).asInstanceOf[FileSplit]
      val blockList = new util.ArrayList[BlockDetails](1)
      blockList.add(new BlockDetails(rawSplit.getPath.toString, rawSplit.getStart,
        rawSplit.getLength, rawSplit.getLocations))
      result(i) = CsvPartition(i, blockList)
    }
    result
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val block = hsplit.asInstanceOf[CsvPartition].blockList.get(0)
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(new FileSplit(
            new Path(block.getFilePath),
            block.getBlockOffset,
            block.getBlockLength,
            block.getLocations
          )).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e: Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(block.getLocations.filter(_ != "localhost"))
  }
}
