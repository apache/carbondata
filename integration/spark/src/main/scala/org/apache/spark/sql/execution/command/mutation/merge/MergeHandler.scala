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
package org.apache.spark.sql.execution.command.mutation.merge

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.avro.AvroFileFormatFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.command.{ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.mutation.HorizontalCompaction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.SegmentUpdateDetails
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * This class handles the merge actions of UPSERT, UPDATE, DELETE, INSERT
 */
abstract class MergeHandler(
    sparkSession: SparkSession,
    frame: DataFrame,
    targetCarbonTable: CarbonTable,
    stats: Stats,
    srcDS: DataFrame) {

  protected def performTagging: (RDD[Row], String) = {
    val tupleId = frame.queryExecution.analyzed.output.zipWithIndex
      .find(_._1.name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).get._2
    val schema =
      org.apache.spark.sql.types.StructType(Seq(
        StructField(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, StringType)))
    val job = CarbonSparkUtil.createHadoopJob()
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    val insertedRows = stats.insertedRows
    val updatedRows = stats.updatedRows
    val uuid = UUID.randomUUID.toString
    job.setJobID(new JobID(uuid, 0))
    val path = targetCarbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "avro"
    FileOutputFormat.setOutputPath(job, new Path(path))
    val factory = AvroFileFormatFactory.getAvroWriter(sparkSession, job, schema)
    val config = SparkSQLUtil.broadCastHadoopConf(sparkSession.sparkContext, job.getConfiguration)
    frame.queryExecution.toRdd.mapPartitionsWithIndex { case (index, iterator) =>
      val confB = config.value.value
      val task = new TaskID(new JobID(uuid, 0), TaskType.MAP, index)
      val attemptID = new TaskAttemptID(task, index)
      val context = new TaskAttemptContextImpl(confB, attemptID)
      val writer = factory.newInstance(path + CarbonCommonConstants.FILE_SEPARATOR + task.toString,
        schema, context)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          if (iterator.hasNext) {
            true
          } else {
            writer.close()
            false
          }
        }

        override def next(): InternalRow = {
          val row = iterator.next()
          val newArray = new Array[Any](1)
          val tupleID = row.getUTF8String(tupleId)
          if (tupleID == null) {
            insertedRows.add(1)
          } else {
            newArray(0) = tupleID
            writer.write(new GenericInternalRow(newArray))
            updatedRows.add(1)
          }
          null
        }
      }
    }.count()
    val deltaRdd = AvroFileFormatFactory.readAvro(sparkSession, path)
    (deltaRdd, path)
  }

  protected def triggerAction(
      factTimestamp: Long,
      executorErrors: ExecutionErrors,
      deltaRdd: RDD[Row],
      deltaPath: String): (util.List[SegmentUpdateDetails], Seq[Segment]) = {
    val tuple = MergeUtil.triggerAction(sparkSession,
      targetCarbonTable,
      factTimestamp,
      executorErrors,
      deltaRdd)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(deltaPath))
    MergeUtil.updateSegmentStatusAfterUpdateOrDelete(targetCarbonTable, factTimestamp, tuple)
    tuple
  }

  protected def insertDataToTargetTable(updateTableModel: Option[UpdateTableModel]): Seq[Row] = {
    val tableCols =
      targetCarbonTable.getCreateOrderColumn.asScala.map(_.getColName).
        filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val header = tableCols.mkString(",")
    val dataFrame = srcDS.select(tableCols.map(col): _*)
    MergeUtil.insertDataToTargetTable(sparkSession,
      targetCarbonTable,
      header,
      updateTableModel,
      dataFrame)
  }

  protected def tryHorizontalCompaction(): Unit = {
    // Do IUD Compaction.
    HorizontalCompaction.tryHorizontalCompaction(
      sparkSession, targetCarbonTable)
  }

  def handleMerge()
}

case class UpdateHandler(
    sparkSession: SparkSession,
    frame: DataFrame,
    targetCarbonTable: CarbonTable,
    stats: Stats,
    srcDS: DataFrame) extends MergeHandler(sparkSession, frame, targetCarbonTable, stats, srcDS) {

  override def handleMerge(): Unit = {
    assert(frame != null, "The dataframe used to perform merge can be only for insert operation")
    val factTimestamp = System.currentTimeMillis()
    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val (deltaRdd, path) = performTagging
    if (deltaRdd.isEmpty()) {
      return
    }
    val tuple = triggerAction(factTimestamp, executorErrors, deltaRdd, path)
    val updateTableModel = Some(UpdateTableModel(isUpdate = true, factTimestamp,
      executorErrors, tuple._2, Option.empty))
    insertDataToTargetTable(updateTableModel)
    tryHorizontalCompaction()
  }

}

case class DeleteHandler(
    sparkSession: SparkSession,
    frame: DataFrame,
    targetCarbonTable: CarbonTable,
    stats: Stats,
    srcDS: DataFrame) extends MergeHandler(sparkSession, frame, targetCarbonTable, stats, srcDS) {
  override def handleMerge(): Unit = {
    assert(frame != null, "The dataframe used to perform merge can be only for insert operation")
    val factTimestamp = System.currentTimeMillis()
    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val (deleteRDD, path) = performTagging
    if (deleteRDD.isEmpty()) {
      return
    }
    triggerAction(factTimestamp, executorErrors, deleteRDD, path)
    MergeUtil.updateStatusIfJustDeleteOperation(targetCarbonTable, factTimestamp)
    tryHorizontalCompaction()
  }
}

case class InsertHandler(
    sparkSession: SparkSession,
    frame: DataFrame,
    targetCarbonTable: CarbonTable,
    stats: Stats,
    srcDS: DataFrame) extends MergeHandler(sparkSession, frame, targetCarbonTable, stats, srcDS) {
  override def handleMerge(): Unit = {
    insertDataToTargetTable(None)
  }
}

case class UpsertHandler(
    sparkSession: SparkSession,
    frame: DataFrame,
    targetCarbonTable: CarbonTable,
    stats: Stats,
    srcDS: DataFrame) extends MergeHandler(sparkSession, frame, targetCarbonTable, stats, srcDS) {
  override def handleMerge(): Unit = {
    assert(frame != null, "The dataframe used to perform merge can be only for insert operation")
    val factTimestamp = System.currentTimeMillis()
    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val (updateDataRDD, path) = performTagging
    val tuple = triggerAction(factTimestamp, executorErrors, updateDataRDD, path)
    val updateTableModel = Some(UpdateTableModel(isUpdate = true, factTimestamp,
      executorErrors, tuple._2, Option.empty))
    insertDataToTargetTable(updateTableModel)
    tryHorizontalCompaction()
  }
}
