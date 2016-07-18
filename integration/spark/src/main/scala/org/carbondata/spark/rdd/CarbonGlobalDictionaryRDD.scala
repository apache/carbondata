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

package org.carbondata.spark.rdd

import java.io.{DataInputStream, InputStreamReader}
import java.nio.charset.Charset
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{CarbonTableIdentifier, ColumnIdentifier}
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.locks.CarbonLockFactory
import org.carbondata.core.locks.LockUsage
import org.carbondata.core.util.CarbonTimeStatisticsFactory
import org.carbondata.processing.etl.DataLoadingException
import org.carbondata.spark.load.{CarbonLoaderUtil, CarbonLoadModel}
import org.carbondata.spark.partition.reader.{CSVParser, CSVReader}
import org.carbondata.spark.tasks.DictionaryWriterTask
import org.carbondata.spark.tasks.SortIndexWriterTask
import org.carbondata.spark.util.GlobalDictionaryUtil
import org.carbondata.spark.util.GlobalDictionaryUtil._

/**
 * A partitioner partition by column.
 *
 * @constructor create a partitioner
 * @param numParts the number of partitions
 */
class ColumnPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

trait GenericParser {
  val dimension: CarbonDimension

  def addChild(child: GenericParser): Unit

  def parseString(input: String): Unit
}

case class DictionaryStats(distinctValues: java.util.List[String],
    dictWriteTime: Long, sortIndexWriteTime: Long)
case class PrimitiveParser(dimension: CarbonDimension,
    setOpt: Option[HashSet[String]]) extends GenericParser {
  val (hasDictEncoding, set: HashSet[String]) = setOpt match {
    case None => (false, new HashSet[String])
    case Some(x) => (true, x)
  }

  def addChild(child: GenericParser): Unit = {
  }

  def parseString(input: String): Unit = {
    if (hasDictEncoding && input != null) {
      set.add(input)
    }
  }
}

case class ArrayParser(dimension: CarbonDimension, format: DataFormat) extends GenericParser {
  var children: GenericParser = _

  def addChild(child: GenericParser): Unit = {
    children = child
  }

  def parseString(input: String): Unit = {
    if (StringUtils.isNotEmpty(input)) {
      val splits = format.getSplits(input)
      if (ArrayUtils.isNotEmpty(splits)) {
        splits.foreach { s =>
          children.parseString(s)
        }
      }
    }
  }
}

case class StructParser(dimension: CarbonDimension,
    format: DataFormat) extends GenericParser {
  val children = new ArrayBuffer[GenericParser]

  def addChild(child: GenericParser): Unit = {
    children += child
  }

  def parseString(input: String): Unit = {
    if (StringUtils.isNotEmpty(input)) {
      val splits = format.getSplits(input)
      val len = Math.min(children.length, splits.length)
      for (i <- 0 until len) {
        children(i).parseString(splits(i))
      }
    }
  }
}

case class DataFormat(delimiters: Array[String],
    var delimiterIndex: Int,
    patterns: Array[Pattern]) extends Serializable {
  self =>
  def getSplits(input: String): Array[String] = {
    // -1 in case after splitting the last column is empty, the surrogate key ahs to be generated
    // for empty value too
    patterns(delimiterIndex).split(input, -1)
  }

  def cloneAndIncreaseIndex: DataFormat = {
    DataFormat(delimiters, Math.min(delimiterIndex + 1, delimiters.length - 1), patterns)
  }
}

/**
 * a case class to package some attributes
 */
case class DictionaryLoadModel(table: CarbonTableIdentifier,
    dimensions: Array[CarbonDimension],
    hdfsLocation: String,
    dictfolderPath: String,
    dictFilePaths: Array[String],
    dictFileExists: Array[Boolean],
    isComplexes: Array[Boolean],
    primDimensions: Array[CarbonDimension],
    delimiters: Array[String],
    highCardIdentifyEnable: Boolean,
    highCardThreshold: Int,
    rowCountPercentage: Double,
    columnIdentifier: Array[ColumnIdentifier],
    isFirstLoad: Boolean) extends Serializable

case class ColumnDistinctValues(values: Array[String], rowCount: Long) extends Serializable

/**
 * A RDD to combine distinct values in block.
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev  the input RDD[Row]
 * @param model a model package load info
 */
class CarbonBlockDistinctValuesCombineRDD(
    prev: RDD[Row],
    model: DictionaryLoadModel)
  extends RDD[(Int, ColumnDistinctValues)](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  override def compute(split: Partition,
      context: TaskContext): Iterator[(Int, ColumnDistinctValues)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordLoadCsvfilesToDfTime()
    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]
    var rowCount = 0L
    try {
      // local combine set
      val dimNum = model.dimensions.length
      val primDimNum = model.primDimensions.length
      val columnValues = new Array[HashSet[String]](primDimNum)
      val mapColumnValuesWithId = new HashMap[String, HashSet[String]]
      for (i <- 0 until primDimNum) {
        columnValues(i) = new HashSet[String]
        distinctValuesList += ((i, columnValues(i)))
        mapColumnValuesWithId.put(model.primDimensions(i).getColumnId, columnValues(i))
      }
      val dimensionParsers = new Array[GenericParser](dimNum)
      for (j <- 0 until dimNum) {
        dimensionParsers(j) = GlobalDictionaryUtil.generateParserForDimension(
          Some(model.dimensions(j)),
          GlobalDictionaryUtil.createDataFormat(model.delimiters),
          mapColumnValuesWithId).get
      }
      var row: Row = null
      val rddIter = firstParent[Row].iterator(split, context)
      // generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          rowCount += 1
          for (i <- 0 until dimNum) {
            dimensionParsers(i).parseString(row.getString(i))
          }
        }
      }
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordLoadCsvfilesToDfTime()
    } catch {
      case ex: Exception =>
        LOGGER.error(ex)
    }
    distinctValuesList.map { iter =>
      val valueList = iter._2.toArray
      (iter._1, ColumnDistinctValues(valueList, rowCount))
    }.iterator
  }
}

/**
 * A RDD to generate dictionary file for each column
 *
 * @constructor create a RDD with RDD[Row]
 * @param prev  the input RDD[Row]
 * @param model a model package load info
 */
class CarbonGlobalDictionaryGenerateRDD(
    prev: RDD[(Int, ColumnDistinctValues)],
    model: DictionaryLoadModel)
  extends RDD[(Int, String, Boolean)](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[(Int, ColumnDistinctValues)].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, String, Boolean)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName)
    var status = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    var isHighCardinalityColumn = false
    val iter = new Iterator[(Int, String, Boolean)] {
      var dictionaryForDistinctValueLookUp: org.carbondata.core.cache.dictionary.Dictionary = _
      var dictionaryForSortIndexWriting: org.carbondata.core.cache.dictionary.Dictionary = _
      var dictionaryForDistinctValueLookUpCleared: Boolean = false
      val dictLock = CarbonLockFactory.getCarbonLockObj(model.table,
        model.columnIdentifier(split.index).getColumnId + LockUsage.LOCK)
      // generate distinct value list
      try {
        val t1 = System.currentTimeMillis
        val valuesBuffer = new mutable.HashSet[String]
        val rddIter = firstParent[(Int, ColumnDistinctValues)].iterator(split, context)
        var rowCount = 0L
        val dicShuffleStartTime = System.currentTimeMillis()
        CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordGlobalDicGenTotalTime(
          dicShuffleStartTime)
        breakable {
          while (rddIter.hasNext) {
            val distinctValueList = rddIter.next()._2
            valuesBuffer ++= distinctValueList.values
            rowCount += distinctValueList.rowCount
            // check high cardinality
            if (model.isFirstLoad && model.highCardIdentifyEnable
                && !model.isComplexes(split.index)
                && model.dimensions(split.index).isColumnar()) {
              isHighCardinalityColumn = GlobalDictionaryUtil.isHighCardinalityColumn(
                valuesBuffer.size, rowCount, model)
              if (isHighCardinalityColumn) {
                break
              }
            }
          }
        }
        val combineListTime = (System.currentTimeMillis() - t1)
        if (isHighCardinalityColumn) {
          LOGGER.info("column " + model.table.getTableUniqueName + "." +
                      model.primDimensions(split.index).getColName + " is high cardinality column")
        } else {
          if (dictLock.lockWithRetries()) {
            logInfo(s"Successfully able to get the dictionary lock for ${
              model.primDimensions(split.index).getColName
            }")
          } else {
            sys
              .error(s"Dictionary file ${
                model.primDimensions(split.index).getColName
              } is locked for updation. Please try after some time")
          }
          val t2 = System.currentTimeMillis
          dictionaryForDistinctValueLookUp = if (model.dictFileExists(split.index)) {
            CarbonLoaderUtil.getDictionary(model.table,
              model.columnIdentifier(split.index),
              model.hdfsLocation,
              model.primDimensions(split.index).getDataType
            )
          } else {
            null
          }
          val dictCacheTime = (System.currentTimeMillis - t2)
          val t3 = System.currentTimeMillis()
          val dictWriteTask = new DictionaryWriterTask(valuesBuffer,
            dictionaryForDistinctValueLookUp,
            model,
            split.index)
          // execute dictionary writer task to get distinct values
          val distinctValues = dictWriteTask.execute()
          val dictWriteTime = (System.currentTimeMillis() - t3)
          val t4 = System.currentTimeMillis()
          // if new data came than rewrite sort index file
          if (distinctValues.size() > 0) {
            val sortIndexWriteTask = new SortIndexWriterTask(model,
              split.index,
              dictionaryForDistinctValueLookUp,
              distinctValues)
            sortIndexWriteTask.execute()
          }
          val sortIndexWriteTime = (System.currentTimeMillis() - t4)
          // After sortIndex writing, update dictionaryMeta
          dictWriteTask.updateMetaData()
          // clear the value buffer after writing dictionary data
          valuesBuffer.clear
          org.carbondata.core.util.CarbonUtil
            .clearDictionaryCache(dictionaryForDistinctValueLookUp);
          dictionaryForDistinctValueLookUpCleared = true
          LOGGER.info("\n columnName:" + model.primDimensions(split.index).getColName +
              "\n columnId:" + model.primDimensions(split.index).getColumnId +
              "\n new distinct values count:" + distinctValues.size() +
              "\n combine lists:" + combineListTime +
              "\n create dictionary cache:" + dictCacheTime +
              "\n sort list, distinct and write:" + dictWriteTime +
              "\n write sort info:" + sortIndexWriteTime)
        }
      } catch {
        case ex: Exception =>
          status = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          LOGGER.error(ex)
      } finally {
        if (!dictionaryForDistinctValueLookUpCleared) {
          org.carbondata.core.util.CarbonUtil
            .clearDictionaryCache(dictionaryForDistinctValueLookUp);
        }
        org.carbondata.core.util.CarbonUtil.clearDictionaryCache(dictionaryForSortIndexWriting);
        if (dictLock != null) {
          if (dictLock.unlock()) {
            logInfo(s"Dictionary ${
              model.primDimensions(split.index).getColName
            } Unlocked Successfully.")
          } else {
            logError(s"Unable to unlock Dictionary ${
              model.primDimensions(split.index).getColName
            }")
          }
        }
      }
      var finished = false

      override def hasNext: Boolean = {

        if (!finished) {
          finished = true
          finished
        } else {
          !finished
        }
      }

      override def next(): (Int, String, Boolean) = {
        (split.index, status, isHighCardinalityColumn)
      }
    }
    iter
  }
}

/**
 * Set column dictionry patition format
 *
 * @param id  partition id
 * @param dimension  current carbon dimension
 */
class CarbonColumnDictPatition(id: Int, dimension: CarbonDimension)
  extends Partition {
  override val index: Int = id
  val preDefDictDimension = dimension
}


/**
 * Use external column dict to generate global dictionary
 *
 * @param carbonLoadModel  carbon load model
 * @param sparkContext  spark context
 * @param table  carbon table identifier
 * @param dimensions  carbon dimenisons having predefined dict
 * @param hdfsLocation  carbon base store path
 * @param dictFolderPath  path of dictionary folder
*/
class CarbonColumnDictGenerateRDD(carbonLoadModel: CarbonLoadModel,
    dictionaryLoadModel: DictionaryLoadModel,
    sparkContext: SparkContext,
    table: CarbonTableIdentifier,
    dimensions: Array[CarbonDimension],
    hdfsLocation: String,
    dictFolderPath: String)
  extends RDD[(Int, ColumnDistinctValues)](sparkContext, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val primDimensions = dictionaryLoadModel.primDimensions
    val primDimLength = primDimensions.length
    val result = new Array[Partition](primDimLength)
    for (i <- 0 until primDimLength) {
      result(i) = new CarbonColumnDictPatition(i, primDimensions(i))
    }
    result
  }

  override def compute(split: Partition, context: TaskContext)
  : Iterator[(Int, ColumnDistinctValues)] = {
    val theSplit = split.asInstanceOf[CarbonColumnDictPatition]
    val primDimension = theSplit.preDefDictDimension
    // read the column dict data
    val preDefDictFilePath = carbonLoadModel.getPredefDictFilePath(primDimension)
    var csvReader: CSVReader = null
    var inputStream: DataInputStream = null
    var colDictData: java.util.Iterator[Array[String]] = null
    try {
      inputStream = FileFactory.getDataInputStream(preDefDictFilePath,
        FileFactory.getFileType(preDefDictFilePath))
      csvReader = new CSVReader(new InputStreamReader(inputStream, Charset.defaultCharset),
        CSVReader.DEFAULT_SKIP_LINES, new CSVParser(carbonLoadModel.getCsvDelimiter.charAt(0)))
      // read the column data to list iterator
      colDictData = csvReader.readAll.iterator
    } catch {
      case ex: Exception =>
        logError(s"Error in reading pre-defined " +
          s"dictionary file:${ex.getMessage}")
    } finally {
      if (csvReader != null) {
        try {
          csvReader.close
        } catch {
          case ex: Exception =>
            logError(s"Error in closing csvReader of " +
            s"pre-defined dictionary file:${ex.getMessage}")
        }
      }
      if (inputStream != null) {
        try {
          inputStream.close
        } catch {
          case ex: Exception =>
            logError(s"Error in closing inputStream of " +
            s"pre-defined dictionary file:${ex.getMessage}")
        }
      }
    }
    val mapIdWithSet = new HashMap[String, HashSet[String]]
    val columnValues = new HashSet[String]
    val distinctValues = (theSplit.index, columnValues)
    mapIdWithSet.put(primDimension.getColumnId, columnValues)
    // use parser to generate new dict value
    val dimensionParser = GlobalDictionaryUtil.generateParserForDimension(
      Some(primDimension),
      createDataFormat(carbonLoadModel.getDelimiters),
      mapIdWithSet).get
    // parse the column data
    while (colDictData.hasNext) {
      dimensionParser.parseString(colDictData.next()(0))
    }
    Array((distinctValues._1,
      ColumnDistinctValues(distinctValues._2.toArray, 0L))).iterator
  }
}
