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

import java.util
import java.util.Comparator

import com.univocity.parsers.common.TextParsingException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Accumulator, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.processing.loading._
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.loading.parser.RowParser
import org.apache.carbondata.processing.loading.parser.impl.{RangeColumnParserImpl, RowParserImpl}
import org.apache.carbondata.processing.loading.row.CarbonRowBatch
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler
import org.apache.carbondata.processing.loading.steps.{DataWriterProcessorStepImpl, SortProcessorStepImpl}
import org.apache.carbondata.processing.sort.sortdata.SortParameters
import org.apache.carbondata.processing.store.{CarbonFactHandler, CarbonFactHandlerFactory}
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil}
import org.apache.carbondata.spark.rdd.{NewRddIterator, StringArrayRow}
import org.apache.carbondata.spark.util.{CommonUtil, DeduplicateHelper}

object DataLoadProcessorStepOnSpark {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def toStringArrayRow(row: InternalRow, columnCount: Int): StringArrayRow = {
    val outRow = new StringArrayRow(new Array[String](columnCount))
    outRow.setValues(row.asInstanceOf[GenericInternalRow].values.asInstanceOf[Array[String]])
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

  def inputFunc(
      rows: Iterator[Array[AnyRef]],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val rowParser = new RowParserImpl(conf.getDataFields, conf)
    val isRawDataRequired = CarbonDataProcessorUtil.isRawDataRequired(conf)
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        var row : CarbonRow = null
        if(isRawDataRequired) {
          val rawRow = rows.next()
           row = new CarbonRow(rowParser.parseRow(rawRow), rawRow)
        } else {
          row = new CarbonRow(rowParser.parseRow(rows.next()))
        }
        rowCounter.add(1)
        row
      }
    }
  }

  def internalInputFunc(
      rows: Iterator[InternalRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Option[Accumulator[Int]],
      rangeField: Option[DataField]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val rowParser: RowParser = if (rangeField.isEmpty) {
      new RowParserImpl(conf.getDataFields, conf)
    } else {
      new RangeColumnParserImpl(rangeField.get, conf)
    }
    val isRawDataRequired = CarbonDataProcessorUtil.isRawDataRequired(conf)
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        var row: CarbonRow = null
        val rawRow =
          rows.next().asInstanceOf[GenericInternalRow].values.asInstanceOf[Array[Object]]
        if (isRawDataRequired) {
          row = new CarbonRow(rowParser.parseRow(rawRow), rawRow)
        } else {
          row = new CarbonRow(rowParser.parseRow(rawRow))
        }
        if (rowCounter.isDefined) {
          rowCounter.get.add(1)
        }
        row
      }
    }
  }

  def inputAndconvertFunc(
      rows: Iterator[Array[AnyRef]],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      partialSuccessAccum: Accumulator[Int],
      rowCounter: Accumulator[Int],
      keepActualData: Boolean = false): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val rowParser = new RowParserImpl(conf.getDataFields, conf)
    val isRawDataRequired = CarbonDataProcessorUtil.isRawDataRequired(conf)
    val badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(conf)
    if (keepActualData) {
      conf.getDataFields.foreach(_.setUseActualData(keepActualData))
    }
    val rowConverter = new RowConverterImpl(conf.getDataFields, conf, badRecordLogger)
    rowConverter.initialize()

    TaskContext.get().addTaskCompletionListener { context =>
      val hasBadRecord: Boolean = CarbonBadRecordUtil.hasBadRecord(model)
      close(conf, badRecordLogger, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum, hasBadRecord)
    }

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      val hasBadRecord : Boolean = CarbonBadRecordUtil.hasBadRecord(model)
      close(conf, badRecordLogger, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum, hasBadRecord)

      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        var row : CarbonRow = null
        if(isRawDataRequired) {
          val rawRow = rows.next()
          row = new CarbonRow(rowParser.parseRow(rawRow), rawRow)
        } else {
          row = new CarbonRow(rowParser.parseRow(rows.next()))
        }
        row = rowConverter.convert(row)
        rowCounter.add(1)
        row
      }
    }
  }

  def convertFunc(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      partialSuccessAccum: Accumulator[Int],
      rowCounter: Accumulator[Int],
      keepActualData: Boolean = false): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(conf)
    if (keepActualData) {
      conf.getDataFields.foreach(_.setUseActualData(keepActualData))
    }
    val rowConverter = new RowConverterImpl(conf.getDataFields, conf, badRecordLogger)
    rowConverter.initialize()

    TaskContext.get().addTaskCompletionListener { context =>
      val hasBadRecord: Boolean = CarbonBadRecordUtil.hasBadRecord(model)
      close(conf, badRecordLogger, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum, hasBadRecord)
    }

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      val hasBadRecord : Boolean = CarbonBadRecordUtil.hasBadRecord(model)
      close(conf, badRecordLogger, rowConverter)
      GlobalSortHelper.badRecordsLogger(model, partialSuccessAccum, hasBadRecord)

      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        rowCounter.add(1)
        rowConverter.convert(rows.next())
      }
    }
  }

  def sampleConvertFunc(
      rows: Iterator[CarbonRow],
      rangeField: DataField,
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel]
  ): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(conf)
    val rowConverter = new RowConverterImpl(Array(rangeField), conf, badRecordLogger)
    rowConverter.initialize()

    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        rowConverter.convert(rows.next())
      }
    }
  }

  def close(conf: CarbonDataLoadConfiguration,
      badRecordLogger: BadRecordsLogger,
      rowConverter: RowConverterImpl): Unit = {
    if (badRecordLogger != null) {
      badRecordLogger.closeStreams()
      CarbonBadRecordUtil.renameBadRecord(conf)
    }
    if (rowConverter != null) {
      rowConverter.finish()
    }
  }

  def deduplicate(
      rows: Iterator[CarbonRow],
      sortComparator: Comparator[Array[AnyRef]],
      uniqueColumnIndex: Int
  ): Iterator[CarbonRow] = {
    new Iterator[CarbonRow] {
      val deduplicateHelper = new DeduplicateHelper(sortComparator, uniqueColumnIndex, 1024)
      var currentRow: CarbonRow = null
      var usedCurrentRow: Boolean = true
      private def getNextRow(): Unit = {
        if (rows.hasNext) {
          val tempRow = rows.next()
          if (deduplicateHelper.isDuplicate(tempRow.getData)) {
            getNextRow()
          } else {
            currentRow = tempRow
          }
        } else {
          currentRow = null
        }
      }

      override def hasNext: Boolean = {
        if (usedCurrentRow) {
          getNextRow()
          usedCurrentRow = false
        }
        if (currentRow == null) {
          deduplicateHelper.close()
        }
        currentRow != null
      }

      override def next(): CarbonRow = {
        usedCurrentRow = true
        currentRow
      }
    }
  }

  def convertTo3Parts(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val sortParameters = SortParameters.createSortParameters(conf)
    val sortStepRowHandler = new SortStepRowHandler(sortParameters)
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        val row =
          new CarbonRow(sortStepRowHandler.convertRawRowTo3Parts(rows.next().getData))
        rowCounter.add(1)
        row
      }
    }
  }

  def writeFunc(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int],
      conf: Configuration) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
        conf.get(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME))
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf)
    var model: CarbonLoadModel = null
    var tableName: String = null
    var rowConverter: RowConverterImpl = null
    var dataWriter: DataWriterProcessorStepImpl = null
    try {
      model = modelBroadcast.value.getCopyWithTaskNo(index.toString)
      val storeLocation = CommonUtil.getTempStoreLocations(index.toString)
      val conf = DataLoadProcessBuilder.createConfiguration(model, storeLocation)

      tableName = model.getTableName

      // When we use sortBy, it means we have 2 stages. Stage1 can't get the finder from Stage2
      // directly because they are in different processes. We need to set cardinality finder in
      // Stage1 again.
      rowConverter = new RowConverterImpl(conf.getDataFields, conf, null)
      rowConverter.initialize()
      conf.setCardinalityFinder(rowConverter)

      dataWriter = new DataWriterProcessorStepImpl(conf)

      val dataHandlerModel = dataWriter.getDataHandlerModel
      var dataHandler: CarbonFactHandler = null
      var rowsNotExist = true
      while (rows.hasNext) {
        if (rowsNotExist) {
          rowsNotExist = false
          dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(dataHandlerModel)
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
        LOGGER.error("Failed for table: " + tableName + " in Data Writer Step", e)
        throw new CarbonDataLoadingException("Error while initializing data handler : " +
          e.getMessage)
      case e: Exception =>
        LOGGER.error("Failed for table: " + tableName + " in Data Writer Step", e)
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

  private def wrapException(e: Throwable, model: CarbonLoadModel): Unit = {
    e match {
      case e: CarbonDataLoadingException => throw e
      case e: TextParsingException =>
        LOGGER.error("Data Loading failed for table " + model.getTableName, e)
        throw new CarbonDataLoadingException("Data Loading failed for table " + model.getTableName,
          e)
      case e: Exception =>
        LOGGER.error("Data Loading failed for table " + model.getTableName, e)
        throw new CarbonDataLoadingException("Data Loading failed for table " + model.getTableName,
          e)
    }
  }

  def sortAndWriteFunc(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int],
      conf: Configuration) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
        conf.get(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME))
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf)
    var model: CarbonLoadModel = null
    var tableName: String = null
    var inputProcessor: NewInputProcessorStepImpl = null
    var rowConverter: RowConverterImpl = null
    var sortProcessor: SortProcessorStepImpl = null
    var dataWriter: DataWriterProcessorStepImpl = null
    try {
      model = modelBroadcast.value.getCopyWithTaskNo(index.toString)
      val storeLocation = CommonUtil.getTempStoreLocations(index.toString)
      val conf = DataLoadProcessBuilder.createConfiguration(model, storeLocation)
      tableName = model.getTableName

      rowConverter = new RowConverterImpl(conf.getDataFields, conf, null)
      rowConverter.initialize()
      conf.setCardinalityFinder(rowConverter)

      inputProcessor = new NewInputProcessorStepImpl(conf, rows)
      sortProcessor = new SortProcessorStepImpl(conf, inputProcessor)
      dataWriter = new DataWriterProcessorStepImpl(conf, sortProcessor)
      dataWriter.initialize()
      dataWriter.execute()
    } catch {
      case e: CarbonDataWriterException =>
        LOGGER.error("Failed for table: " + tableName + " in Data Writer Step", e)
        throw new CarbonDataLoadingException("Error while initializing data handler : " +
                                             e.getMessage)
      case e: Exception =>
        LOGGER.error("Failed for table: " + tableName + " in Data Writer Step", e)
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
}

class NewInputProcessorStepImpl(configuration: CarbonDataLoadConfiguration,
    rows: Iterator[CarbonRow]) extends AbstractDataLoadProcessorStep(configuration, null) {
  /**
   * Tranform the data as per the implementation.
   *
   * @return Array of Iterator with data. It can be processed parallel if implementation class wants
   * @throws CarbonDataLoadingException
   */
  override def execute(): Array[util.Iterator[CarbonRowBatch]] = {
    val batchSize = CarbonProperties.getInstance.getBatchSize
    val iteratorArray = new Array[util.Iterator[CarbonRowBatch]](1)

    iteratorArray(0) = new util.Iterator[CarbonRowBatch] {

      val rowBatch = new CarbonRowBatch(batchSize) {
        var count = 0
        override def next(): CarbonRow = {
          count = count + 1
          rows.next()
        }
        override def hasNext: Boolean = rows.hasNext && count < batchSize

        def reset(): Unit = {
          count = 0
        }
      }

      override def next(): CarbonRowBatch = {
        rowBatch.reset()
        rowBatch
      }

      override def hasNext: Boolean = {
        rows.hasNext
      }
    }

    iteratorArray
  }

  /**
   * Get the step name for logging purpose.
   *
   * @return Step name
   */
  override protected def getStepName: String = {
    "Input Processor for RANGE_SORT"
  }
}
