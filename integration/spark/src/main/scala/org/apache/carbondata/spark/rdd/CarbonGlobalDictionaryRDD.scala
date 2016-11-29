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

package org.apache.carbondata.spark.rdd

import java.io.{DataInputStream, InputStreamReader}
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.Breaks.{break, breakable}

import au.com.bytecode.opencsv.CSVReader
import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.carbondata.common.factory.CarbonCommonFactory
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.tasks.{DictionaryWriterTask, SortIndexWriterTask}
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.spark.util.GlobalDictionaryUtil
import org.apache.carbondata.spark.util.GlobalDictionaryUtil._

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
    isFirstLoad: Boolean,
    hdfsTempLocation: String,
    lockType: String,
    zooKeeperUrl: String,
    serializationNullFormat: String) extends Serializable

case class ColumnDistinctValues(values: Array[String], rowCount: Long) extends Serializable

/**
 * A RDD to combine all dictionary distinct values.
 *
 * @constructor create a RDD with RDD[(String, Iterable[String])]
 * @param prev  the input RDD[(String, Iterable[String])]
 * @param model a model package load info
 */
class CarbonAllDictionaryCombineRDD(
    prev: RDD[(String, Iterable[String])],
    model: DictionaryLoadModel)
  extends RDD[(Int, ColumnDistinctValues)](prev) {

  override def getPartitions: Array[Partition] = {
    firstParent[(String, Iterable[String])].partitions
  }

  override def compute(split: Partition, context: TaskContext
  ): Iterator[(Int, ColumnDistinctValues)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]
    /*
     * for all dictionary, all columns need to encoding and checking
     * isHighCardinalityColumn, so no need to calculate rowcount
     */
    val rowCount = 0L
    try {
      val dimensionParsers =
        GlobalDictionaryUtil.createDimensionParsers(model, distinctValuesList)
      val dimNum = model.dimensions.length
      // Map[dimColName -> dimColNameIndex]
      val columnIndexMap = new HashMap[String, Int]()
      for (j <- 0 until dimNum) {
        columnIndexMap.put(model.dimensions(j).getColName, j)
      }

      var row: (String, Iterable[String]) = null
      val rddIter = firstParent[(String, Iterable[String])].iterator(split, context)
      // generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          columnIndexMap.get(row._1) match {
            case Some(index) =>
              for (record <- row._2) {
                dimensionParsers(index).parseString(record)
              }
            case None =>
          }
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(ex)
        throw ex
    }

    distinctValuesList.map { iter =>
      val valueList = iter._2.toArray
      (iter._1, ColumnDistinctValues(valueList, rowCount))
    }.iterator
  }
}

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
  extends RDD[(Int, ColumnDistinctValues)](prev) {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  override def compute(split: Partition,
      context: TaskContext): Iterator[(Int, ColumnDistinctValues)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordLoadCsvfilesToDfTime()
    val distinctValuesList = new ArrayBuffer[(Int, HashSet[String])]
    var rowCount = 0L
    try {
      val dimensionParsers =
        GlobalDictionaryUtil.createDimensionParsers(model, distinctValuesList)
      val dimNum = model.dimensions.length
      var row: Row = null
      val rddIter = firstParent[Row].iterator(split, context)
      val formatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
          .CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      val format = new SimpleDateFormat(formatString)
      // generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          rowCount += 1
          for (i <- 0 until dimNum) {
            dimensionParsers(i).parseString(CarbonScalaUtil.getString(row.get(i),
                model.serializationNullFormat, model.delimiters(0), model.delimiters(1), format))
          }
        }
      }
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordLoadCsvfilesToDfTime()
    } catch {
      case ex: Exception =>
        LOGGER.error(ex)
        throw ex
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
  extends RDD[(Int, String, Boolean)](prev) {

  override def getPartitions: Array[Partition] = firstParent[(Int, ColumnDistinctValues)].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, String, Boolean)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var status = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    var isHighCardinalityColumn = false
    val iter = new Iterator[(Int, String, Boolean)] {
      var dictionaryForDistinctValueLookUp:
      org.apache.carbondata.core.cache.dictionary.Dictionary = _
      var dictionaryForSortIndexWriting: org.apache.carbondata.core.cache.dictionary.Dictionary = _
      var dictionaryForDistinctValueLookUpCleared: Boolean = false
      val pathService = CarbonCommonFactory.getPathService
      val carbonTablePath = pathService.getCarbonTablePath(model.hdfsLocation, model.table)
      if (StringUtils.isNotBlank(model.hdfsTempLocation )) {
         CarbonProperties.getInstance.addProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION,
           model.hdfsTempLocation)
      }
      if (StringUtils.isNotBlank(model.lockType)) {
        CarbonProperties.getInstance.addProperty(CarbonCommonConstants.LOCK_TYPE,
          model.lockType)
      }
      if (StringUtils.isNotBlank(model.zooKeeperUrl)) {
        CarbonProperties.getInstance.addProperty(CarbonCommonConstants.ZOOKEEPER_URL,
          model.zooKeeperUrl)
      }
      val dictLock = CarbonLockFactory
        .getCarbonLockObj(carbonTablePath.getRelativeDictionaryDirectory,
          model.columnIdentifier(split.index).getColumnId + LockUsage.LOCK)
      var isDictionaryLocked = false
      // generate distinct value list
      try {
        val t1 = System.currentTimeMillis
        val valuesBuffer = new mutable.HashSet[String]
        val rddIter = firstParent[(Int, ColumnDistinctValues)].iterator(split, context)
        var rowCount = 0L
        CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordDicShuffleAndWriteTime()
        breakable {
          while (rddIter.hasNext) {
            val distinctValueList = rddIter.next()._2
            valuesBuffer ++= distinctValueList.values
            rowCount += distinctValueList.rowCount
            // check high cardinality
            if (model.isFirstLoad && model.highCardIdentifyEnable
                && !model.isComplexes(split.index)
                && model.dimensions(split.index).isColumnar) {
              isHighCardinalityColumn = GlobalDictionaryUtil.isHighCardinalityColumn(
                valuesBuffer.size, rowCount, model)
              if (isHighCardinalityColumn) {
                break
              }
            }
          }
        }
        val combineListTime = System.currentTimeMillis() - t1
        if (isHighCardinalityColumn) {
          LOGGER.info(s"column ${ model.table.getTableUniqueName }." +
                      s"${
                        model.primDimensions(split.index)
                          .getColName
                      } is high cardinality column")
        } else {
          isDictionaryLocked = dictLock.lockWithRetries()
          if (isDictionaryLocked) {
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
          val fileType = FileFactory.getFileType(model.dictFilePaths(split.index))
          model.dictFileExists(split.index) = FileFactory
            .isFileExist(model.dictFilePaths(split.index), fileType)
          dictionaryForDistinctValueLookUp = if (model.dictFileExists(split.index)) {
            CarbonLoaderUtil.getDictionary(model.table,
              model.columnIdentifier(split.index),
              model.hdfsLocation,
              model.primDimensions(split.index).getDataType
            )
          } else {
            null
          }
          val dictCacheTime = System.currentTimeMillis - t2
          val t3 = System.currentTimeMillis()
          val dictWriteTask = new DictionaryWriterTask(valuesBuffer,
            dictionaryForDistinctValueLookUp,
            model,
            split.index)
          // execute dictionary writer task to get distinct values
          val distinctValues = dictWriteTask.execute()
          val dictWriteTime = System.currentTimeMillis() - t3
          val t4 = System.currentTimeMillis()
          // if new data came than rewrite sort index file
          if (distinctValues.size() > 0) {
            val sortIndexWriteTask = new SortIndexWriterTask(model,
              split.index,
              dictionaryForDistinctValueLookUp,
              distinctValues)
            sortIndexWriteTask.execute()
          }
          val sortIndexWriteTime = System.currentTimeMillis() - t4
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordDicShuffleAndWriteTime()
          // After sortIndex writing, update dictionaryMeta
          dictWriteTask.updateMetaData()
          // clear the value buffer after writing dictionary data
          valuesBuffer.clear
          CarbonUtil.clearDictionaryCache(dictionaryForDistinctValueLookUp)
          dictionaryForDistinctValueLookUpCleared = true
          LOGGER.info(s"\n columnName: ${ model.primDimensions(split.index).getColName }" +
                      s"\n columnId: ${ model.primDimensions(split.index).getColumnId }" +
                      s"\n new distinct values count: ${ distinctValues.size() }" +
                      s"\n combine lists: $combineListTime" +
                      s"\n create dictionary cache: $dictCacheTime" +
                      s"\n sort list, distinct and write: $dictWriteTime" +
                      s"\n write sort info: $sortIndexWriteTime")
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex)
          throw ex
      } finally {
        if (!dictionaryForDistinctValueLookUpCleared) {
          CarbonUtil.clearDictionaryCache(dictionaryForDistinctValueLookUp)
        }
        CarbonUtil.clearDictionaryCache(dictionaryForSortIndexWriting)
        if (dictLock != null && isDictionaryLocked) {
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
 * @param id        partition id
 * @param dimension current carbon dimension
 */
class CarbonColumnDictPatition(id: Int, dimension: CarbonDimension)
  extends Partition {
  override val index: Int = id
  val preDefDictDimension = dimension
}


/**
 * Use external column dict to generate global dictionary
 *
 * @param carbonLoadModel carbon load model
 * @param sparkContext    spark context
 * @param table           carbon table identifier
 * @param dimensions      carbon dimenisons having predefined dict
 * @param hdfsLocation    carbon base store path
 * @param dictFolderPath  path of dictionary folder
 */
class CarbonColumnDictGenerateRDD(carbonLoadModel: CarbonLoadModel,
    dictionaryLoadModel: DictionaryLoadModel,
    sparkContext: SparkContext,
    table: CarbonTableIdentifier,
    dimensions: Array[CarbonDimension],
    hdfsLocation: String,
    dictFolderPath: String)
  extends RDD[(Int, ColumnDistinctValues)](sparkContext, Nil) {

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
        carbonLoadModel.getCsvDelimiter.charAt(0))
      // read the column data to list iterator
      colDictData = csvReader.readAll.iterator
    } catch {
      case ex: Exception =>
        logError(s"Error in reading pre-defined " +
                 s"dictionary file:${ ex.getMessage }")
        throw ex
    } finally {
      if (csvReader != null) {
        try {
          csvReader.close()
        } catch {
          case ex: Exception =>
            logError(s"Error in closing csvReader of " +
                     s"pre-defined dictionary file:${ ex.getMessage }")
        }
      }
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case ex: Exception =>
            logError(s"Error in closing inputStream of " +
                     s"pre-defined dictionary file:${ ex.getMessage }")
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
