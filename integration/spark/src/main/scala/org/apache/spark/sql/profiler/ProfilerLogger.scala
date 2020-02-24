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

package org.apache.spark.sql.profiler

import java.lang.{StringBuilder => JavaStringBuilder}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.stats.TaskStatistics
import org.apache.carbondata.core.util.CarbonUtil

/**
 * the util of profiler logger
 */
private[profiler] object ProfilerLogger {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  lazy val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  // format timestamp value
  def format(time: Long): String = this.synchronized {
    if (time < 0) {
      ""
    } else {
      val timestamp = new Timestamp(time)
      simpleDateFormat.format(timestamp)
    }
  }

  def logStatementSummary(statementId: Long, messages: ArrayBuffer[ProfilerMessage]): Unit = {
    LOGGER.info(new StatementSummary(statementId, messages).toString)
  }

  def logExecutionSummary(executionId: Long, messages: ArrayBuffer[ProfilerMessage]): Unit = {
    LOGGER.info(new ExecutionSummary(executionId, messages).toString)
  }
}

/**
 * summarize the statement information
 */
private[profiler] class StatementSummary(
    statementId: Long,
    messages: ArrayBuffer[ProfilerMessage]
) {

  private var sqlText: String = ""

  private var isCommand = false

  private var startTime: Long = -1

  private var parserEnd: Long = -1

  private var analyzerEnd: Long = -1

  private var optimizerStart: Long = -1

  private var optimizerEnd: Long = -1

  private var optimizerTaken: Long = -1

  private var endTime: Long = -1

  // summarize the messages
  messages.foreach {
    case sqlStart: SQLStart =>
      sqlText = sqlStart.sqlText.trim
      isCommand = sqlStart.isCommand
      startTime = sqlStart.startTime
      parserEnd = sqlStart.parseEnd
      analyzerEnd = sqlStart.analyzerEnd
      endTime = sqlStart.endTime
    case optimizer: Optimizer =>
      optimizerTaken = optimizer.timeTaken
      optimizerStart = optimizer.startTime
      optimizerEnd = optimizerStart + optimizerTaken
  }

  private def totalTaken: Long = endTime - startTime

  private def parserTaken: Long = parserEnd - startTime

  private def analyzerTaken: Long = analyzerEnd - parserEnd

  private def parserToOptimizer: Long = optimizerEnd - startTime

  private def commandTaken: Long = endTime - analyzerEnd

  override def toString: String = {
    if (isCommand) {
      buildForComand()
    } else {
      buildForQuery()
    }
  }

  /**
   * for example
   * [statement id]: 1
   * [sql text]:
   * +-----------------------------------------------------------+
   * |CREATE TABLE carbon_table(                                 |
   * | shortField SHORT,                                         |
   * | intField INT,                                             |
   * | bigintField LONG,                                         |
   * | doubleField DOUBLE,                                       |
   * | stringField STRING,                                       |
   * | timestampField TIMESTAMP,                                 |
   * | decimalField DECIMAL(18,2),                               |
   * | dateField DATE,                                           |
   * | charField CHAR(5),                                        |
   * | floatField FLOAT                                          |
   * | )                                                         |
   * | STORED AS carbondata                                      |
   * +-----------------------------------------------------------+
   * [start time]: 2018-03-22 17:12:18.310
   * [driver side total taken]: 1255 ms
   *   |__ 1.parser taken: 203 ms
   *   |__ 2.analyzer taken: 1 ms
   *   |__ 3.execution taken: 1051 ms
   *   |__ ...
   */
  private def buildForComand(): String = {
    val builder = new JavaStringBuilder(1000)
    builder.append(s"\n[statement id]: ${ statementId }")
    builder.append(s"\n[sql text]:\n")
    CarbonUtil.logTable(builder, sqlText, "")
    builder.append(s"\n[start time]: ${ ProfilerLogger.format(startTime) }\n")
    builder.append(s"[driver side total taken]: $totalTaken ms\n")
    builder.append(s"  |__ 1.parser taken: $parserTaken ms\n")
    builder.append(s"  |__ 2.analyzer taken: $analyzerTaken ms\n")
    builder.append(s"  |__ 3.execution taken: $commandTaken ms")
    builder.toString
  }

  /**
   * for example
   * [statement id]: 4
   * [sql text]:
   * +--------------------------------------------------+
   * |SELECT charField, stringField, intField           |
   * | FROM carbon_table                                |
   * | WHERE stringfield = 'spark' AND decimalField > 40|
   * +--------------------------------------------------+
   * [start time]: 2018-03-22 17:12:22.001
   * [driver side total taken]: 476ms
   * |__ 1.parser taken: 82 ms
   * |__ 2.analyzer taken: 195 ms
   * |__ 3.(149ms)
   * |__ 4.carbon optimizer taken: 50 ms
   *       end time: 2018-03-22 17:12:22.477
   */
  private def buildForQuery(): String = {
    val builder = new JavaStringBuilder(1000)
    builder.append(s"\n[statement id]: ${ statementId }")
    builder.append(s"\n[sql text]:\n")
    CarbonUtil.logTable(builder, sqlText, "")
    builder.append(s"\n[start time]: ${ ProfilerLogger.format(startTime) }\n")
    builder.append(s"[driver side total taken]: ${ parserToOptimizer }ms\n")
    builder.append(s"  |__ 1.parser taken: $parserTaken ms\n")
    builder.append(s"  |__ 2.analyzer taken: $analyzerTaken ms\n")
    builder.append(s"  |__ 3.(${ optimizerStart - analyzerEnd }ms)\n")
    builder.append(s"  |__ 4.carbon optimizer taken: $optimizerTaken ms\n")
    builder.append(s"        end time: ${ ProfilerLogger.format(optimizerEnd) }")
    builder.toString
  }
}

/**
 * summarize the execution information
 */
private[profiler] class ExecutionSummary(
    executionId: Long,
    messages: ArrayBuffer[ProfilerMessage]
) {

  private var sqlPlan: String = ""

  private var startTime: Long = -1

  private var endTime: Long = -1

  private val partitions = new util.ArrayList[GetPartition]()

  private val tasks = new util.ArrayList[TaskStatistics]()

  // summarize the messages
  messages.foreach {
    case start: ExecutionStart =>
      sqlPlan = start.plan
      startTime = start.startTime
    case end: ExecutionEnd =>
      endTime = end.endTime
    case partition: GetPartition =>
      partitions.add(partition)
    case task: QueryTaskEnd =>
      tasks.add(new TaskStatistics(task.queryId, task.values, task.size, task.files))
  }

  private def totalTaken: Long = endTime - startTime

  /**
   * for example
   * (prepare inputFormat 55ms)~(getSplits 90ms)~(distributeSplits 4ms)~(3ms)
   */
  private def detail(p: GetPartition, builder: JavaStringBuilder): Unit = {
    builder.append("(prepare inputFormat ").append(p.getSplitsStart - p.startTime).append("ms)~")
    builder.append("(getSplits ").append(p.getSplitsEnd - p.getSplitsStart).append("ms)~")
    val gap1 = p.distributeStart - p.getSplitsEnd
    if (gap1 > 0) {
      builder.append("(").append(gap1).append("ms)~")
    }
    builder.append("(distributeSplits ").append(p.distributeEnd - p.distributeStart).append("ms)")
    val gap2 = p.endTime - p.distributeEnd
    if (gap2 > 0) {
      builder.append("~(").append(gap2).append("ms)")
    }
  }

  private def printGetPartitionTable(builder: JavaStringBuilder, indent: String): Unit = {
    util.Collections.sort(partitions)
    for (rowIndex <- 0 until partitions.size()) {
      val partition = partitions.get(rowIndex)
      val content = new JavaStringBuilder()
      content.append("query_id: ").append(partition.queryId).append("\n")
        .append("table_name: ").append(partition.tableName).append("\n")
        .append("table_path: ").append(partition.tablePath).append("\n")
        .append("start_time: ").append(ProfilerLogger.format(partition.startTime)).append("\n")
      // total time
      content.append("total_time: ").append(partition.endTime - partition.startTime).append("ms")
      content.append(" [")
      detail(partition, content)
      content.append("]\n")
      content.append("valid_segment_num: ").append(partition.numSegments).append("\n")
        .append("stream_segment_num: ").append(partition.numStreamSegments).append("\n")
        .append("hit_data_block: ").append(partition.numBlocks).append("\n")
        .append("spark_partition_num: ").append(partition.numOfPartitions).append("\n")
        .append("pushed_filter: ").append(partition.filter).append("\n")
        .append("pushed_projection: ").append(partition.projection)
      if (rowIndex > 0) {
        builder.append("\n")
      }
      CarbonUtil.logTable(builder, content.toString, indent)
    }
  }

  private def printStatisticTable(builder: JavaStringBuilder, indent: String): Unit = {
    util.Collections.sort(tasks, comparator)
    TaskStatistics.printStatisticTable(tasks, builder, indent)
  }

  private def printInputFiles(builder: JavaStringBuilder, indent: String): Unit = {
    for (taskIndex <- 0 until tasks.size()) {
      val task = tasks.get(taskIndex)
      val content = new JavaStringBuilder()
      content.append("query_id: ").append(task.getQueryId).append("\n")
        .append("task_id: ").append(task.getValues()(1)).append("\n")
        .append("total_size: ").append(task.getFileSize).append("Byte\n")
        .append("file_list: ")
      task.getFiles.foreach { file =>
        content.append("\n  Segment_").append(file)
      }
      if (taskIndex > 0) {
        builder.append("\n")
      }
      CarbonUtil.logTable(builder, content.toString, indent)
    }
  }

  // use to sort tasks
  private lazy val comparator = new Comparator[TaskStatistics]() {
    override def compare(o1: TaskStatistics,
        o2: TaskStatistics) = {
      val result = o1.getQueryId.compareTo(o2.getQueryId())
      if (result != 0) {
        result
      } else {
        val task = o1.getValues()(1) - o2.getValues()(1)
        if (task > 0) {
          1
        } else if (task < 0) {
          -1
        } else {
          0
        }
      }
    }
  }

  // scalastyle:off
  /**
   * for example
   * [execution id]: 0
   * [start time]: 2018-03-22 17:12:22.608
   * [executor side total taken]: 845 ms
   *   |_1.getPartition
   *     +------------------------------------------------------------------------------------------------------------------+
   *     |query_id: 23737310772188                                                                                          |
   *     |table_name: default.carbon_table                                                                                  |
   *     |table_path: /carbondata/examples/spark/target/store/default/carbon_table                                         |
   *     |start_time: 2018-03-22 17:12:23.141                                                                               |
   *     |total_time: 152ms [(prepare inputFormat 55ms)~(getSplits 90ms)~(distributeSplits 4ms)~(3ms)]                      |
   *     |valid_segment_num: 1                                                                                              |
   *     |stream_segment_num: 1                                                                                             |
   *     |hit_data_block: 1                                                                                                 |
   *     |spark_partition_num: 1                                                                                            |
   *     |pushed_filter: (((stringfield <> null and decimalfield <> null) and stringfield = spark) and decimalfield > 40.00)|
   *     |pushed_projection: charfield,stringfield,intfield                                                                 |
   *     +------------------------------------------------------------------------------------------------------------------+
   *   |_2.task statistics
   *     +--------------+-------+-----------------------+----------+----------------+--------------------+----------------+--------------+---------------+---------------+---------------+-----------+-------------+-----------+-----------+
   *     |query_id      |task_id|start_time             |total_time|load_blocks_time|load_dictionary_time|carbon_scan_time|carbon_IO_time|scan_blocks_num|total_blocklets|valid_blocklets|total_pages|scanned_pages|valid_pages|result_size|
   *     +--------------+-------+-----------------------+----------+----------------+--------------------+----------------+--------------+---------------+---------------+---------------+-----------+-------------+-----------+-----------+
   *     |23737310772188|      0|2018-03-22 17:12:23.334|     106ms|             3ms|                 0ms|            -1ms|          -1ms|              1|              1|              1|          0|            1|          1|          3|
   *     +--------------+-------+-----------------------+----------+----------------+--------------------+----------------+--------------+---------------+---------------+---------------+-----------+-------------+-----------+-----------+
   *   |_3.input files for each task
   *     +--------------------------------------------------------+
   *     |query_id: 23737310772188                                |
   *     |task_id: 0                                              |
   *     |total_size: 2717Byte                                    |
   *     |file_list:                                              |
   *     |  Segment_0/part-0-0_batchno0-0-1521709939969.carbondata|
   *     +--------------------------------------------------------+
   * [sql plan]:
   * +----------+
   * | ...      |
   * +----------+
   */
  // scalastyle:on
  override def toString: String = {
    val builder = new JavaStringBuilder(1000)
    builder.append(s"\n[execution id]: ${ executionId }\n")
    builder.append(s"[start time]: ${ ProfilerLogger.format(startTime) }\n")
    builder.append(s"[executor side total taken]: $totalTaken ms")
    builder.append(s"\n  |_1.getPartition\n")
    printGetPartitionTable(builder, "    ")
    builder.append(s"\n  |_2.task statistics\n")
    printStatisticTable(builder, "    ")
    builder.append(s"\n  |_3.input files for each task\n")
    printInputFiles(builder, "    ")
    builder.append(s"\n[sql plan]:\n")
    CarbonUtil.logTable(builder, sqlPlan, "")
    builder.toString
  }
}
