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

import com.univocity.parsers.common.TextParsingException
import org.apache.spark.{Accumulator, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.loading.{BadRecordsLogger, BadRecordsLoggerProvider, CarbonDataLoadConfiguration, DataLoadProcessBuilder, TableProcessingOperations}
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.loading.parser.impl.RowParserImpl
import org.apache.carbondata.processing.loading.sort.SortStepRowUtil
import org.apache.carbondata.processing.loading.steps.DataWriterProcessorStepImpl
import org.apache.carbondata.processing.sort.sortdata.SortParameters
import org.apache.carbondata.processing.store.{CarbonFactHandler, CarbonFactHandlerFactory}
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil}
import org.apache.carbondata.spark.rdd.{NewRddIterator, StringArrayRow}
import org.apache.carbondata.spark.util.Util

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
        val row = rowConverter.convert(rows.next())
        rowCounter.add(1)
        row
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

  def convertTo3Parts(
      rows: Iterator[CarbonRow],
      index: Int,
      modelBroadcast: Broadcast[CarbonLoadModel],
      rowCounter: Accumulator[Int]): Iterator[CarbonRow] = {
    val model: CarbonLoadModel = modelBroadcast.value.getCopyWithTaskNo(index.toString)
    val conf = DataLoadProcessBuilder.createConfiguration(model)
    val sortParameters = SortParameters.createSortParameters(conf)
    val sortStepRowUtil = new SortStepRowUtil(sortParameters)
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      wrapException(e, model)
    }

    new Iterator[CarbonRow] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): CarbonRow = {
        val row =
          new CarbonRow(sortStepRowUtil.convertRow(rows.next().getData))
        rowCounter.add(1)
        row
      }
    }
  }

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

      val dataHandlerModel = dataWriter.getDataHandlerModel
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
      case e: TextParsingException =>
        LOGGER.error(e, "Data Loading failed for table " + model.getTableName)
        throw new CarbonDataLoadingException("Data Loading failed for table " + model.getTableName,
          e)
      case e: Exception =>
        LOGGER.error(e, "Data Loading failed for table " + model.getTableName)
        throw new CarbonDataLoadingException("Data Loading failed for table " + model.getTableName,
          e)
    }
  }
}
