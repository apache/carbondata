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

import java.util
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
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.stats.QueryStatistic
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.rdd.StreamHandoffRDD
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.carbondata.streaming.{CarbonStreamException, CarbonStreamOutputFormat}
import org.apache.carbondata.streaming.index.StreamFileIndex
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
    operationContext: OperationContext) extends Sink {

  private val fileLogPath = CarbonTablePath.getStreamingLogDir(carbonTable.getTablePath)
  private val fileLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, fileLogPath)
  // prepare configuration
  private val hadoopConf = {
    val conf = sparkSession.sessionState.newHadoopConf()
    // put all parameters into hadoopConf
    parameters.foreach { entry =>
      conf.set(entry._1, entry._2)
    }
    // properties below will be used for default CarbonStreamParser
    val complexDelimiters = carbonLoadModel.getComplexDelimiters
    conf.set("carbon_complex_delimiter_level_1", complexDelimiters.get(0))
    conf.set("carbon_complex_delimiter_level_2", complexDelimiters.get(1))
    conf.set("carbon_complex_delimiter_level_3", complexDelimiters.get(2))
    conf.set(
      DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT,
      carbonLoadModel.getSerializationNullFormat().split(",")(1))
    conf.set(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      carbonLoadModel.getTimestampformat())
    conf.set(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      carbonLoadModel.getDateFormat())
    conf
  }
  CommonUtil.configureCSVInputFormat(hadoopConf, carbonLoadModel)
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

  // measure data type array
  private lazy val msrDataTypes = {
    carbonLoadModel
      .getCarbonDataLoadSchema
      .getCarbonTable
      .getVisibleMeasures
      .asScala
      .map(_.getDataType)
      .toArray
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      CarbonAppendableStreamSink.LOGGER.info(s"Skipping already committed batch $batchId")
    } else {

      val statistic = new QueryStatistic()

      // fire pre event on every batch add
      // in case of streaming options and optionsFinal can be same
      val loadTablePreExecutionEvent = new LoadTablePreExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        carbonLoadModel,
        carbonLoadModel.getFactFilePath,
        false,
        parameters.asJava,
        parameters.asJava,
        false,
        sparkSession
      )
      OperationListenerBus.getInstance().fireEvent(loadTablePreExecutionEvent, operationContext)
      checkOrHandOffSegment()

      // committer will record how this spark job commit its output
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = fileLogPath,
        false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      CarbonAppendableStreamSink.writeDataFileJob(
        sparkSession,
        carbonTable,
        batchId,
        currentSegmentId,
        data.queryExecution,
        committer,
        hadoopConf,
        carbonLoadModel,
        msrDataTypes)
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
    // get streaming segment, if not exists, create new streaming segment
    val segmentId = StreamSegment.open(carbonTable)
    if (segmentId.equals(currentSegmentId)) {
      val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, currentSegmentId)
      if (segmentMaxSize <= StreamSegment.size(segmentDir)) {
        val newSegmentId = StreamSegment.close(carbonTable, currentSegmentId)
        currentSegmentId = newSegmentId
        val newSegmentDir =
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath, currentSegmentId)
        FileFactory.mkdirs(newSegmentDir)

        // trigger hand off operation
        if (enableAutoHandoff) {
          StreamHandoffRDD.startStreamingHandoffThread(
            carbonLoadModel,
            operationContext,
            sparkSession,
            false)
        }
      }
    } else {
      currentSegmentId = segmentId
      val newSegmentDir =
        CarbonTablePath.getSegmentPath(carbonTable.getTablePath, currentSegmentId)
      FileFactory.mkdirs(newSegmentDir)
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
      batchId: Long,
      segmentId: String,
      queryExecution: QueryExecution,
      committer: FileCommitProtocol,
      hadoopConf: Configuration,
      carbonLoadModel: CarbonLoadModel,
      msrDataTypes: Array[DataType]): Unit = {

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
      var result: Array[(TaskCommitMessage, StreamFileIndex)] = null
      try {
        committer.setupJob(job)

        val rowSchema = queryExecution.analyzed.schema
        val isVarcharTypeMapping = {
          val col2VarcharType = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
                      .getCreateOrderColumn().asScala
            .map(c => c.getColName -> (c.getDataType == DataTypes.VARCHAR)).toMap
          rowSchema.fieldNames.map(c => {
            val r = col2VarcharType.get(c.toLowerCase)
            r.isDefined && r.get
          })
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
              iterator,
              rowSchema,
              isVarcharTypeMapping
            )
          })

        // update data file info in index file
        StreamSegment.updateIndexFile(
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentId),
          result.map(_._2), msrDataTypes)

      } catch {
        // catch fault of executor side
        case t: Throwable =>
          val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentId)
          StreamSegment.recoverSegmentIfRequired(segmentDir)
          LOGGER.error(s"Aborting job ${ job.getJobID }.", t)
          committer.abortJob(job)
          throw new CarbonStreamException("Job failed to write data file", t)
      }
      committer.commitJob(job, result.map(_._1))
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
      iterator: Iterator[InternalRow],
      rowSchema: StructType,
      isVarcharTypeMapping: Array[Boolean]): (TaskCommitMessage, StreamFileIndex) = {

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
      var blockIndex: StreamFileIndex = null
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {

        val parserName = taskAttemptContext.getConfiguration.get(
          CarbonStreamParser.CARBON_STREAM_PARSER,
          CarbonStreamParser.CARBON_STREAM_PARSER_DEFAULT)

        val streamParser =
          Class.forName(parserName).newInstance.asInstanceOf[CarbonStreamParser]
        streamParser.initialize(taskAttemptContext.getConfiguration,
            rowSchema, isVarcharTypeMapping)

        blockIndex = StreamSegment.appendBatchData(new InputIterator(iterator, streamParser),
          taskAttemptContext, carbonLoadModel)
      })(catchBlock = {
        committer.abortTask(taskAttemptContext)
        LOGGER.error(s"Job $jobId aborted.")
      })
      (committer.commitTask(taskAttemptContext), blockIndex)
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
