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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.processing.csvload.{CSVInputFormat, StringArrayWritable}
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.DataLoadProcessBuilder
import org.apache.carbondata.processing.sortandgroupby.sortdata.{NewRowComparator, NewRowComparatorForNormalDims, SortParameters}
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.storage.StorageLevel

/**
  * Use sortBy operator in spark to load the data
  */
object GlobalSort {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def loadDataUsingGlobalSort(
      sc: SparkContext,
      dataFrame: Option[DataFrame],
      model: CarbonLoadModel,
      currentLoadCount: Int): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    val originRDD = if (dataFrame.isDefined) {
      dataFrame.get.rdd
    } else {
      // input data from files
      val hadoopConfiguration = new Configuration()
      CommonUtil.configureCSVInputFormat(hadoopConfiguration, model)
      hadoopConfiguration.set(FileInputFormat.INPUT_DIR, model.getFactFilePath)
      val columnCount = model.getCsvHeaderColumns.length
      new NewHadoopRDD[NullWritable, StringArrayWritable](
        sc,
        classOf[CSVInputFormat],
        classOf[NullWritable],
        classOf[StringArrayWritable],
        hadoopConfiguration)
        .map(x => GlobalSortOperates.toStringArrayRow(x._2, columnCount))
    }

    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.longAccumulator("Partial Success Accumulator")

    val inputStepRowNumber = sc.longAccumulator("Input Processor Accumulator")
    val convertStepRowNumber = sc.longAccumulator("Convert Processor Accumulator")
    val sortStepRowNumber = sc.longAccumulator("Sort Processor Accumulator")
    val writeStepRowNumber = sc.longAccumulator("Write Processor Accumulator")

    // 1. Input
    val inputRDD = originRDD.mapPartitions(rows => GlobalSortOperates.toRDDIterator(rows, modelBroadcast))
      .mapPartitionsWithIndex { case (index, rows) =>
        GlobalSortOperates.inputFunc(rows, index, currentLoadCount, modelBroadcast, inputStepRowNumber)
      }

    // 2. Convert
    val convertRDD = inputRDD.mapPartitionsWithIndex { case (index, rows) =>
      GlobalSortOperates.convertFunc(rows, index, currentLoadCount, modelBroadcast, partialSuccessAccum,
        convertStepRowNumber)
    }.filter(_ != null)// Filter the bad record

    // 3. Sort
    val configuration = DataLoadProcessBuilder.createConfiguration(model)
    val sortParameters = SortParameters.createSortParameters(configuration)
    object RowOrdering extends Ordering[Array[AnyRef]] {
      def compare(rowA: Array[AnyRef], rowB: Array[AnyRef]): Int = {
        val rowComparator: Comparator[Array[AnyRef]] =
          if (sortParameters.getNoDictionaryCount > 0) {
            new NewRowComparator(sortParameters.getNoDictionaryDimnesionColumn)
          } else {
            new NewRowComparatorForNormalDims(sortParameters.getDimColCount)
          }

        rowComparator.compare(rowA, rowB)
      }
    }

    var numPartitions = CarbonDataProcessorUtil.getGlobalSortPartitions(configuration)
    if (numPartitions <= 0) {
      numPartitions = convertRDD.partitions.length// TODO
    }

    // Because if the number of partitions greater than 1, there will be action operator(sample) in sortBy operator.
    // So here we cache the rdd to avoid do input and convert again.
    if (numPartitions > 1) {
      convertRDD.persist(StorageLevel.MEMORY_AND_DISK)
    }

    import scala.reflect.classTag
    val sortRDD =
      convertRDD.sortBy(_.getData, numPartitions = numPartitions)(RowOrdering, classTag[Array[AnyRef]])
        .mapPartitionsWithIndex { case (index, rows) =>
          GlobalSortOperates.convertTo3Parts(rows, index, currentLoadCount, modelBroadcast, sortStepRowNumber)
        }

    // 4. Write
    sc.runJob(sortRDD, (context: TaskContext, rows: Iterator[CarbonRow]) =>
      GlobalSortOperates.writeFunc(rows, context.partitionId, currentLoadCount, modelBroadcast, writeStepRowNumber))

    // clean cache
    convertRDD.unpersist()

    // Log the number of rows in each step
    LOGGER.audit("Total rows processed in step Input Processor: " + inputStepRowNumber.value)
    LOGGER.audit("Total rows processed in step Data Converter: " + convertStepRowNumber.value)
    LOGGER.audit("Total rows processed in step Sort Processor: " + sortStepRowNumber.value)
    LOGGER.audit("Total rows processed in step Data Writer: " + writeStepRowNumber.value)

    // Update status
    if (partialSuccessAccum.value != 0) {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE + "Partial_Success"
      val loadMetadataDetails = new LoadMetadataDetails()
      loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      executionErrors.failureCauses = FailureCauses.BAD_RECORDS
      Array((uniqueLoadStatusId, (loadMetadataDetails, executionErrors)))
    } else {
      val uniqueLoadStatusId = model.getTableName + CarbonCommonConstants.UNDERSCORE + "Success"
      val loadMetadataDetails = new LoadMetadataDetails()
      loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      Array((uniqueLoadStatusId, (loadMetadataDetails, executionErrors)))
    }
  }
}
