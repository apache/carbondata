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

import scala.util.Random

import org.apache.spark.{Accumulator, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.loading.{DataLoadProcessBuilder, TableProcessingOperations}
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.loading.parser.impl.RowParserImpl
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler
import org.apache.carbondata.processing.loading.steps.{DataConverterProcessorStepImpl, DataWriterProcessorStepImpl}
import org.apache.carbondata.processing.store.{CarbonFactHandler, CarbonFactHandlerFactory}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd.{NewRddIterator, StringArrayRow}
import org.apache.carbondata.spark.util.Util

object DataLoadProcessorStepOnSpark {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def toStringArrayRow(row: StringArrayWritable, columnCount: Int): StringArrayRow = {
    val outRow = new StringArrayRow(new Array[String](columnCount))
    outRow.setValues(row.get())
  }

  def toRDDIterator(
      rows: Iterator[Row],
      modelBroadcast: Broadcast[CarbonLoadModel]): Iterator[Array[AnyRef]] = {
    new Iterator[Array[AnyRef]] {
      val iter = new NewRddIterator(rows, modelBroadcast.value, TaskContext.get())

      override def hasNext: Boolean = iter.hasNext

      override def next(): Array[AnyRef] = iter.next
    }
  }

  /**
   * parse input row
   * @param rows input rows
   * @param index taskNo
   * @param modelBroadcast loadModel
   * @param rowCounter counter for input rows
   * @return parsed rows
   */
  def inputFunc(
      rows: Iterator[Array[AnyRef]],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val rowParser = new RowParserImpl(conf.getDataFields, conf)

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        val row = new CarbonRow(rowParser.parseRow(rows.next()))
        rowCounter.add(1)
        row
      }
    }
  }

  /**
   * convert row such as convert dictionary column use global dictionary
   * @param rows input rows
   * @param index taskNo
   * @param modelBroadcast loadModel
   * @param partialSuccessAccum counter for bad record
   * @param rowCounter counter for converted rows
   * @return converted rows
   */
  def convertFunc(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      partialSuccessAccum: Accumulator[Int],
      rowCounter: Accumulator[Int]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val badRecordLogger = DataConverterProcessorStepImpl.createBadRecordLogger(conf)
    val rowConverter = new RowConverterImpl(conf.getDataFields, conf, badRecordLogger)
    rowConverter.initialize()

    TaskContext.get().addTaskCompletionListener { context =>
      DataConverterProcessorStepImpl.close(badRecordLogger, conf, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum)
    }

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      DataConverterProcessorStepImpl.close(badRecordLogger, conf, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum)

      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        val row = rowConverter.convert(rows.next())
        rowCounter.add(1)
        row
      }
    }
  }

  /**
   * convert carbon row from raw format to 3-parted format
   * @param rawRow rawRow
   * @param index taskNo
   * @param modelBroadcast loadModel
   * @param sortStepRowHandler handler used to convert rows
   * @param rowCounter row counter for sorted rows
   * @return row in 3-parts
   */
  def convertTo3Parts(
      rawRow: CarbonRow,
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      sortStepRowHandler: SortStepRowHandler,
      rowCounter: Accumulator[Int]): CarbonRow = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    val row = new CarbonRow(sortStepRowHandler.convertRawRowTo3Parts(rawRow.getData))
    rowCounter.add(1)
    row
  }

  /**
   * write rows to carbon datafile
   * @param rows rows
   * @param index taskNo
   * @param modelBroadcast loadModel
   * @param rowCounter row counter for written rows
   */
  def writeFunc(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int]) {
    var model: CarbonLoadModel = null
    var tableName: String = null
    var rowConverter: RowConverterImpl = null
    var dataWriter: DataWriterProcessorStepImpl = null
    try {
      model = modelBroadcast.value.getCopyWithTaskNo(index.toString)
      val storeLocation = Array(getTempStoreLocation(index))
      val conf = DataLoadProcessBuilder.createConfiguration(model, storeLocation)

      tableName = model.getTableName

      // When we use sortBy, it means we have 2 stages. Stage1 can't get the finder from Stage2
      // directly because they are in different processes. We need to set cardinality finder in
      // Stage1 again.
      rowConverter = new RowConverterImpl(conf.getDataFields, conf, null)
      rowConverter.initialize()
      conf.setCardinalityFinder(rowConverter)

      dataWriter = new DataWriterProcessorStepImpl(conf)

      val dataHandlerModel = dataWriter.getDataHandlerModel(0)
      var dataHandler: CarbonFactHandler = null
      var rowsNotExist = true
      while (rows.hasNext) {
        if (rowsNotExist) {
          rowsNotExist = false
          dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(dataHandlerModel,
            CarbonFactHandlerFactory.FactHandlerType.COLUMNAR)
          dataHandler.initialise()
        }
        val row = dataWriter.processRow(rows.next(), dataHandler)
        rowCounter.add(1)
        row
      }

      if (!rowsNotExist) {
        dataWriter.finish(dataHandler)
      }
    } catch {
      case e: CarbonDataWriterException =>
        LOGGER.error(e, "Failed for table: " + tableName + " in Data Writer Step")
        throw new CarbonDataLoadingException("Error while initializing data handler : " +
          e.getMessage)
      case e: Exception =>
        LOGGER.error(e, "Failed for table: " + tableName + " in Data Writer Step")
        throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage, e)
    } finally {
      if (rowConverter != null) {
        rowConverter.finish()
      }
      // close the dataWriter once the write in done success or fail. if not closed then thread to
      // to prints the rows processed in each step for every 10 seconds will never exit.
      if(null != dataWriter) {
        dataWriter.close()
      }
      // clean up the folders and files created locally for data load operation
      TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
    }
  }

  private def getTempStoreLocation(index: Int): String = {
    var storeLocation = ""
    // this property is used to determine whether temp location for carbon is inside
    // container temp dir or is yarn application directory.
    val carbonUseLocalDir = CarbonProperties.getInstance()
      .getProperty("carbon.use.local.dir", "false")
    if (carbonUseLocalDir.equalsIgnoreCase("true")) {
      val storeLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
      if (null != storeLocations && storeLocations.nonEmpty) {
        storeLocation = storeLocations(Random.nextInt(storeLocations.length))
      }
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
    } else {
      storeLocation = System.getProperty("java.io.tmpdir")
    }
    storeLocation = storeLocation + '/' + System.nanoTime() + '_' + index
    storeLocation
  }

  private def wrapException(e: Throwable, model: CarbonLoadModel): Unit = {
    e match {
      case e: CarbonDataLoadingException => throw e
      case e: Exception =>
        LOGGER.error(e, "Data Loading failed for table " + model.getTableName)
        throw new CarbonDataLoadingException("Data Loading failed for table " + model.getTableName,
          e)
    }
  }
}
