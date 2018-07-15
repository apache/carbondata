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

package org.apache.carbondata.spark.load

import java.util.Comparator

import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.loading.{DataLoadProcessBuilder, FailureCauses}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.sort.sortdata.{NewRowComparator, NewRowComparatorForNormalDims, SortParameters}
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil

/**
 * Use sortBy operator in spark to load the data
 */
object DataLoadProcessBuilderOnSpark {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def loadDataUsingGlobalSort(
      sparkSession: SparkSession,
      dataFrame: Option[DataFrame],
      model: CarbonLoadModel,
      hadoopConf: Configuration): Array[(String, (SegmentStatus, ExecutionErrors))] = {
    val originRDD = if (dataFrame.isDefined) {
      dataFrame.get.rdd
    } else {
      // input data from files
      val columnCount = model.getCsvHeaderColumns.length
      CsvRDDHelper.csvFileScanRDD(sparkSession, model, hadoopConf)
        .map(DataLoadProcessorStepOnSpark.toStringArrayRow(_, columnCount))
    }

    val sc = sparkSession.sparkContext
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.accumulator(0, "Partial Success Accumulator")

    val inputStepRowCounter = sc.accumulator(0, "Input Processor Accumulator")
    val convertStepRowCounter = sc.accumulator(0, "Convert Processor Accumulator")
    val sortStepRowCounter = sc.accumulator(0, "Sort Processor Accumulator")
    val writeStepRowCounter = sc.accumulator(0, "Write Processor Accumulator")

    // 1. Input
    val inputRDD = originRDD
      .mapPartitions(rows => DataLoadProcessorStepOnSpark.toRDDIterator(rows, modelBroadcast))
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.inputFunc(rows, index, modelBroadcast, inputStepRowCounter)
      }

    // 2. Convert
    val convertRDD = inputRDD.mapPartitionsWithIndex { case (index, rows) =>
      DataLoadProcessorStepOnSpark.convertFunc(rows, index, modelBroadcast, partialSuccessAccum,
        convertStepRowCounter)
    }.filter(_ != null)// Filter the bad record

    // 3. Sort
    val configuration = DataLoadProcessBuilder.createConfiguration(model)
    val sortParameters = SortParameters.createSortParameters(configuration)
    val rowComparator: Comparator[Array[AnyRef]] =
      if (sortParameters.getNoDictionaryCount > 0) {
        new NewRowComparator(sortParameters.getNoDictionaryDimnesionColumn)
      } else {
        new NewRowComparatorForNormalDims(sortParameters.getDimColCount)
      }
    object RowOrdering extends Ordering[Array[AnyRef]] {
      def compare(rowA: Array[AnyRef], rowB: Array[AnyRef]): Int = {
        rowComparator.compare(rowA, rowB)
      }
    }

    var numPartitions = CarbonDataProcessorUtil.getGlobalSortPartitions(
      configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS))
    if (numPartitions <= 0) {
      numPartitions = convertRDD.partitions.length
    }

    // Because if the number of partitions greater than 1, there will be action operator(sample) in
    // sortBy operator. So here we cache the rdd to avoid do input and convert again.
    if (numPartitions > 1) {
      convertRDD.persist(StorageLevel.fromString(
        CarbonProperties.getInstance().getGlobalSortRddStorageLevel()))
    }

    import scala.reflect.classTag
    val sortRDD = convertRDD
      .sortBy(_.getData, numPartitions = numPartitions)(RowOrdering, classTag[Array[AnyRef]])
      .mapPartitionsWithIndex { case (index, rows) =>
        DataLoadProcessorStepOnSpark.convertTo3Parts(rows, index, modelBroadcast,
          sortStepRowCounter)
      }

    // 4. Write
    sc.runJob(sortRDD, (context: TaskContext, rows: Iterator[CarbonRow]) =>
      DataLoadProcessorStepOnSpark.writeFunc(rows, context.partitionId, modelBroadcast,
        writeStepRowCounter))

    // clean cache only if persisted and keeping unpersist non-blocking as non-blocking call will
    // not have any functional impact as spark automatically monitors the cache usage on each node
    // and drops out old data partiotions in a least-recently used (LRU) fashion.
    if (numPartitions > 1) {
      convertRDD.unpersist(false)
    }

    // Log the number of rows in each step
    LOGGER.info("Total rows processed in step Input Processor: " + inputStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Converter: " + convertStepRowCounter.value)
    LOGGER.info("Total rows processed in step Sort Processor: " + sortStepRowCounter.value)
    LOGGER.info("Total rows processed in step Data Writer: " + writeStepRowCounter.value)

    // Update status
    if (partialSuccessAccum.value != 0) {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE +
        "Partial_Success"
      val status = SegmentStatus.LOAD_PARTIAL_SUCCESS
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      executionErrors.failureCauses = FailureCauses.BAD_RECORDS
      Array((uniqueLoadStatusId, (status, executionErrors)))
    } else {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE + "Success"
      val status = SegmentStatus.SUCCESS
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      Array((uniqueLoadStatusId, (status, executionErrors)))
    }
  }
}
