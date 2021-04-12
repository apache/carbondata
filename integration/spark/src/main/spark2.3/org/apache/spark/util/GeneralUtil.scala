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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.streaming.CarbonAppendableStreamSink.{WriteDataFileJobDescription, writeDataFileTask}
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.CarbonSparkUtil
import org.apache.carbondata.streaming.CarbonStreamException
import org.apache.carbondata.streaming.index.StreamFileIndex
import org.apache.carbondata.streaming.segment.StreamSegment
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, EqualTo, NamedExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.secondaryindex.jobs.BlockletIndexDetailsWithSchema
import org.apache.spark.sql.secondaryindex.query.SecondaryIndexQueryResultProcessor

object GeneralUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

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
    val job = CarbonSparkUtil.createHadoopJob(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    val jobId = CarbonInputFormatUtil.getJobId(batchId.toInt)
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

  def invokeQueryPlanNormalizeExprId(r: NamedExpression, input: AttributeSeq)
  : NamedExpression = {
    QueryPlan.normalizeExprId(r, input)
  }

  def checkIfRuleNeedToBeApplied(plan: LogicalPlan): Boolean = {
    var isRuleNeedToBeApplied = false
    val relations = CarbonSparkUtil.collectCarbonRelation(plan)
    val isCreateAsSelect = isCreateTableAsSelect(plan)
    if (relations.nonEmpty && !isCreateAsSelect) {
      plan.collect {
        case join@Join(_, _, _, condition) =>
          condition match {
            case Some(x) =>
              x match {
                case _: EqualTo =>
                  return isRuleNeedToBeApplied
                  join
                case _ =>
                  join
              }
            case _ =>
          }
        case _ =>
          isRuleNeedToBeApplied = true
          plan
      }
    }
    isRuleNeedToBeApplied
  }


  def isCreateTableAsSelect(plan: LogicalPlan): Boolean = {
    var isCreateTableAsSelectFlow = false
    if (SparkUtil.isSparkVersionXAndAbove("2.3")) {
      plan collect {
        case CreateHiveTableAsSelectCommand(_, _, _, _) =>
          isCreateTableAsSelectFlow = true
        case CreateDataSourceTableAsSelectCommand(_, _, _, _) =>
          isCreateTableAsSelectFlow = true
        case create: LogicalPlan if (create.getClass.getSimpleName
          .equals("OptimizedCreateHiveTableAsSelectCommand")) =>
          isCreateTableAsSelectFlow = true
      }
    }
    isCreateTableAsSelectFlow
  }

  def closeReader(context: TaskContext, callback: () => Unit): Unit = {
    context.addTaskCompletionListener { context =>
      callback
    }
  }

  def closeReaders(context: TaskContext, secondaryIndexQueryResultProcessor:
  SecondaryIndexQueryResultProcessor): Unit = {
    context.addTaskCompletionListener { context =>
      if (null != secondaryIndexQueryResultProcessor) {
        secondaryIndexQueryResultProcessor.close()
      }
    }
  }

}
