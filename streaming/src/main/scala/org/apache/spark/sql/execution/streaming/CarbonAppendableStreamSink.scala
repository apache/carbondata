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

package org.apache.spark.sql.execution.streaming

import java.util.Date

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.stats.QueryStatistic
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.streaming.CarbonStreamOutputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.streaming.{CarbonStreamException, StreamHandoffRDD}
import org.apache.carbondata.streaming.parser.CarbonStreamParser
import org.apache.carbondata.streaming.segment.StreamSegment

/**
 * an implement of stream sink, it persist each batch to disk by appending the batch data to
 * data files.
 */
class CarbonAppendableStreamSink(
    sparkSession: SparkSession,
    val carbonTable: CarbonTable,
    var currentSegmentId: String,
    parameters: Map[String, String],
    carbonLoadModel: CarbonLoadModel,
    server: Option[DictionaryServer]) extends Sink {

  private val fileLogPath = CarbonTablePath.getStreamingLogDir(carbonTable.getTablePath)
  private val fileLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, fileLogPath)
  // prepare configuration
  private val hadoopConf = {
    val conf = sparkSession.sessionState.newHadoopConf()
    // put all parameters into hadoopConf
    parameters.foreach { entry =>
      conf.set(entry._1, entry._2)
    }
    conf
  }
  // segment max size(byte)
  private val segmentMaxSize = hadoopConf.getLong(
    CarbonCommonConstants.HANDOFF_SIZE,
    CarbonProperties.getInstance().getHandoffSize
  )

  // auto handoff
  private val enableAutoHandoff = hadoopConf.getBoolean(
    CarbonCommonConstants.ENABLE_AUTO_HANDOFF,
    CarbonProperties.getInstance().isEnableAutoHandoff
  )

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      CarbonAppendableStreamSink.LOGGER.info(s"Skipping already committed batch $batchId")
    } else {

      val statistic = new QueryStatistic()

      // fire pre event on every batch add
      val operationContext = new OperationContext
      val loadTablePreExecutionEvent = new LoadTablePreExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        carbonLoadModel,
        carbonLoadModel.getFactFilePath,
        false,
        parameters.asJava,
        null,
        false
      )
      OperationListenerBus.getInstance().fireEvent(loadTablePreExecutionEvent, operationContext)
      checkOrHandOffSegment()

      // committer will record how this spark job commit its output
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = fileLogPath,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      CarbonAppendableStreamSink.writeDataFileJob(
        sparkSession,
        carbonTable,
        parameters,
        batchId,
        currentSegmentId,
        data.queryExecution,
        committer,
        hadoopConf,
        carbonLoadModel,
        server)
      // fire post event on every batch add
      val loadTablePostExecutionEvent = new LoadTablePostExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        carbonLoadModel
      )
      OperationListenerBus.getInstance().fireEvent(loadTablePostExecutionEvent, operationContext)

      statistic.addStatistics(s"add batch: $batchId", System.currentTimeMillis())
      CarbonAppendableStreamSink.LOGGER.info(
        s"${statistic.getMessage}, taken time(ms): ${statistic.getTimeTaken}")
    }
  }

  /**
   * if the directory size of current segment beyond the threshold, hand off new segment
   */
  private def checkOrHandOffSegment(): Unit = {
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, currentSegmentId)
    val fileType = FileFactory.getFileType(segmentDir)
    if (segmentMaxSize <= StreamSegment.size(segmentDir)) {
      val newSegmentId = StreamSegment.close(carbonTable, currentSegmentId)
      currentSegmentId = newSegmentId
      val newSegmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, currentSegmentId)
      FileFactory.mkdirs(newSegmentDir, fileType)

      // TODO trigger hand off operation
      if (enableAutoHandoff) {
        StreamHandoffRDD.startStreamingHandoffThread(
          carbonLoadModel,
          sparkSession)
      }
    }
  }
}

object CarbonAppendableStreamSink {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * package the hadoop configuration and it will be passed to executor side from driver side
   */
  case class WriteDataFileJobDescription(
      serializableHadoopConf: SerializableConfiguration,
      batchId: Long,
      segmentId: String)

  /**
   * Run a spark job to append the newly arrived data to the existing row format
   * file directly.
   * If there are failure in the task, spark will re-try the task and
   * carbon will do recovery by HDFS file truncate. (see StreamSegment.tryRecoverFromTaskFault)
   * If there are job level failure, every files in the stream segment will do truncate
   * if necessary. (see StreamSegment.tryRecoverFromJobFault)
   */
  def writeDataFileJob(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      parameters: Map[String, String],
      batchId: Long,
      segmentId: String,
      queryExecution: QueryExecution,
      committer: FileCommitProtocol,
      hadoopConf: Configuration,
      carbonLoadModel: CarbonLoadModel,
      server: Option[DictionaryServer]): Unit = {

    // create job
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    val jobId = CarbonInputFormatUtil.getJobId(new Date, batchId.toInt)
    job.setJobID(jobId)

    val description = WriteDataFileJobDescription(
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      batchId,
      segmentId
    )

    // run write data file job
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      var result: Array[TaskCommitMessage] = null
      try {
        committer.setupJob(job)
        // initialize dictionary server
        if (server.isDefined) {
          server.get.initializeDictionaryGenerator(carbonTable)
        }

        // write data file
        result = sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iterator: Iterator[InternalRow]) => {
            writeDataFileTask(
              description,
              carbonLoadModel,
              sparkStageId = taskContext.stageId(),
              sparkPartitionId = taskContext.partitionId(),
              sparkAttemptNumber = taskContext.attemptNumber(),
              committer,
              iterator
            )
          })

        // write dictionary
        if (server.isDefined) {
          try {
            server.get.writeTableDictionary(carbonTable.getCarbonTableIdentifier.getTableId)
          } catch {
            case _: Exception =>
              LOGGER.error(
                s"Error while writing dictionary file for ${carbonTable.getTableUniqueName}")
              throw new Exception(
                "Streaming ingest failed due to error while writing dictionary file")
          }
        }

        // update data file info in index file
        StreamSegment.updateIndexFile(
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentId))

      } catch {
        // catch fault of executor side
        case t: Throwable =>
          val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentId)
          StreamSegment.recoverSegmentIfRequired(segmentDir)
          LOGGER.error(t, s"Aborting job ${ job.getJobID }.")
          committer.abortJob(job)
          throw new CarbonStreamException("Job failed to write data file", t)
      }
      committer.commitJob(job, result)
      LOGGER.info(s"Job ${ job.getJobID } committed.")
    }
  }

  /**
   * execute a task for each partition to write a data file
   */
  def writeDataFileTask(
      description: WriteDataFileJobDescription,
      carbonLoadModel: CarbonLoadModel,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]
  ): TaskCommitMessage = {

    val jobId = CarbonInputFormatUtil.getJobId(new Date, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      CarbonStreamOutputFormat.setSegmentId(hadoopConf, description.segmentId)
      hadoopConf.set("mapred.job.id", jobId.toString)
      hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapred.task.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapred.task.is.map", true)
      hadoopConf.setInt("mapred.task.partition", 0)
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {

        val parserName = taskAttemptContext.getConfiguration.get(
          CarbonStreamParser.CARBON_STREAM_PARSER,
          CarbonStreamParser.CARBON_STREAM_PARSER_DEFAULT)

        val streamParser =
          Class.forName(parserName).newInstance.asInstanceOf[CarbonStreamParser]
        streamParser.initialize(taskAttemptContext.getConfiguration)

        StreamSegment.appendBatchData(new InputIterator(iterator, streamParser),
          taskAttemptContext, carbonLoadModel)
      })(catchBlock = {
        committer.abortTask(taskAttemptContext)
        LOGGER.error(s"Job $jobId aborted.")
      })
      committer.commitTask(taskAttemptContext)
    } catch {
      case t: Throwable =>
        throw new CarbonStreamException("Task failed while writing rows", t)
    }
  }

  /**
   * convert spark iterator to carbon iterator, so that java module can use it.
   */
  class InputIterator(rddIter: Iterator[InternalRow], streamParser: CarbonStreamParser)
    extends CarbonIterator[Array[Object]] {

    override def hasNext: Boolean = rddIter.hasNext

    override def next: Array[Object] = {
      streamParser.parserRow(rddIter.next())
    }

    override def close(): Unit = {
      streamParser.close()
    }
  }

}
