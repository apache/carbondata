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
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.breakable

import au.com.bytecode.opencsv.CSVReader
import com.univocity.parsers.common.TextParsingException
import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.tasks.{DictionaryWriterTask, SortIndexWriterTask}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, GlobalDictionaryUtil}

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

case class PrimitiveParser(dimension: CarbonDimension,
    setOpt: Option[mutable.HashSet[String]]) extends GenericParser {
  val (hasDictEncoding, set: mutable.HashSet[String]) = setOpt match {
    case None => (false, new mutable.HashSet[String])
    case Some(x) => (true, x)
  }

  def addChild(child: GenericParser): Unit = {
  }

  def parseString(input: String): Unit = {
    if (hasDictEncoding && input != null) {
      if (set.size < CarbonLoadOptionConstants.MAX_EXTERNAL_DICTIONARY_SIZE) {
        set.add(input)
      } else {
        throw new NoRetryException(s"Cannot provide more than ${
          CarbonLoadOptionConstants.MAX_EXTERNAL_DICTIONARY_SIZE } dictionary values")
      }
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
case class DictionaryLoadModel(table: AbsoluteTableIdentifier,
    dimensions: Array[CarbonDimension],
    hdfsLocation: String,
    dictfolderPath: String,
    dictFilePaths: Array[String],
    dictFileExists: Array[Boolean],
    isComplexes: Array[Boolean],
    primDimensions: Array[CarbonDimension],
    delimiters: Array[String],
    columnIdentifier: Array[ColumnIdentifier],
    hdfsTempLocation: String,
    lockType: String,
    zooKeeperUrl: String,
    serializationNullFormat: String,
    defaultTimestampFormat: String,
    defaultDateFormat: String) extends Serializable

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
  extends CarbonRDD[(Int, ColumnDistinctValues)](prev) {

  override def getPartitions: Array[Partition] = {
    firstParent[(String, Iterable[String])].partitions
  }

    override def internalCompute(split: Partition, context: TaskContext
  ): Iterator[(Int, ColumnDistinctValues)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val distinctValuesList = new ArrayBuffer[(Int, mutable.HashSet[String])]
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
      val columnIndexMap = new mutable.HashMap[String, Int]()
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

class StringArrayRow(var values: Array[String]) extends Row {

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def getString(i: Int): String = values(i)

  private def reset(): Unit = {
    for (i <- 0 until values.length) {
      values(i) = null
    }
  }

  override def copy(): Row = {
    val tmpValues = new Array[String](values.length)
    System.arraycopy(values, 0, tmpValues, 0, values.length)
    new StringArrayRow(tmpValues)
  }

  def setValues(values: Array[String]): StringArrayRow = {
    reset()
    if (values != null) {
      val minLength = Math.min(this.values.length, values.length)
      System.arraycopy(values, 0, this.values, 0, minLength)
    }
    this
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
  extends CarbonRDD[(Int, ColumnDistinctValues)](prev) {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions
  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(Int, ColumnDistinctValues)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordLoadCsvfilesToDfTime()
    val distinctValuesList = new ArrayBuffer[(Int, mutable.HashSet[String])]
    var rowCount = 0L
    try {
      val dimensionParsers =
        GlobalDictionaryUtil.createDimensionParsers(model, distinctValuesList)
      val dimNum = model.dimensions.length
      var row: Row = null
      val rddIter = firstParent[Row].iterator(split, context)
      val timeStampFormat = new SimpleDateFormat(model.defaultTimestampFormat)
      val dateFormat = new SimpleDateFormat(model.defaultDateFormat)
      // generate block distinct value set
      while (rddIter.hasNext) {
        row = rddIter.next()
        if (row != null) {
          rowCount += 1
          for (i <- 0 until dimNum) {
            dimensionParsers(i).parseString(CarbonScalaUtil.getString(row.get(i),
              model.serializationNullFormat,
              model.delimiters(0),
              model.delimiters(1),
              timeStampFormat,
              dateFormat))
          }
        }
      }
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordLoadCsvfilesToDfTime()
    } catch {
      case txe: TextParsingException =>
        throw txe
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
  extends CarbonRDD[(Int, SegmentStatus)](prev) {

  override def getPartitions: Array[Partition] = firstParent[(Int, ColumnDistinctValues)].partitions

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(Int, SegmentStatus)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var status = SegmentStatus.SUCCESS
    val iter = new Iterator[(Int, SegmentStatus)] {
      var dictionaryForDistinctValueLookUp: Dictionary = _
      var dictionaryForDistinctValueLookUpCleared: Boolean = false
      val dictionaryColumnUniqueIdentifier: DictionaryColumnUniqueIdentifier = new
          DictionaryColumnUniqueIdentifier(
        model.table,
        model.columnIdentifier(split.index),
        model.columnIdentifier(split.index).getDataType)
      if (StringUtils.isNotBlank(model.hdfsTempLocation)) {
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
      val dictLock: ICarbonLock = CarbonLockFactory
        .getCarbonLockObj(model.table,
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
          }
        }
        val combineListTime = System.currentTimeMillis() - t1
        isDictionaryLocked = dictLock.lockWithRetries()
        if (isDictionaryLocked) {
          logInfo(s"Successfully able to get the dictionary lock for ${
            model.primDimensions(split.index).getColName
          }")
        } else {
          sys.error(s"Dictionary file ${
              model.primDimensions(split.index).getColName
          } is locked for updation. Please try after some time")
        }
        val t2 = System.currentTimeMillis
        val fileType = FileFactory.getFileType(model.dictFilePaths(split.index))
        val isDictFileExists = FileFactory.isFileExist(model.dictFilePaths(split.index), fileType)
        dictionaryForDistinctValueLookUp = if (isDictFileExists) {
          CarbonLoaderUtil.getDictionary(model.table,
            model.columnIdentifier(split.index),
            model.primDimensions(split.index).getDataType
          )
        } else {
          null
        }
        val dictCacheTime = System.currentTimeMillis - t2
        val t3 = System.currentTimeMillis()
        val dictWriteTask = new DictionaryWriterTask(valuesBuffer,
          dictionaryForDistinctValueLookUp,
          dictionaryColumnUniqueIdentifier,
          model.primDimensions(split.index).getColumnSchema,
          isDictFileExists
        )
        // execute dictionary writer task to get distinct values
        val distinctValues = dictWriteTask.execute()
        val dictWriteTime = System.currentTimeMillis() - t3
        val t4 = System.currentTimeMillis()
        // if new data came than rewrite sort index file
        if (distinctValues.size() > 0) {
          val sortIndexWriteTask = new SortIndexWriterTask(dictionaryColumnUniqueIdentifier,
            model.primDimensions(split.index).getDataType,
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
      } catch {
        case dictionaryException: NoRetryException =>
          LOGGER.error(dictionaryException)
          status = SegmentStatus.LOAD_FAILURE
        case ex: Exception =>
          LOGGER.error(ex)
          throw ex
      } finally {
        if (!dictionaryForDistinctValueLookUpCleared) {
          CarbonUtil.clearDictionaryCache(dictionaryForDistinctValueLookUp)
        }
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

      override def next(): (Int, SegmentStatus) = {
        (split.index, status)
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
  val preDefDictDimension: CarbonDimension = dimension
}


/**
 * Use external column dict to generate global dictionary
 *
 * @param carbonLoadModel carbon load model
 * @param sparkContext    spark context
 * @param table           carbon table identifier
 * @param dimensions      carbon dimenisons having predefined dict
 * @param dictFolderPath  path of dictionary folder
 */
class CarbonColumnDictGenerateRDD(carbonLoadModel: CarbonLoadModel,
    dictionaryLoadModel: DictionaryLoadModel,
    sparkContext: SparkContext,
    table: CarbonTableIdentifier,
    dimensions: Array[CarbonDimension],
    dictFolderPath: String)
  extends CarbonRDD[(Int, ColumnDistinctValues)](sparkContext, Nil,
    sparkContext.hadoopConfiguration) {

  override def getPartitions: Array[Partition] = {
    val primDimensions = dictionaryLoadModel.primDimensions
    val primDimLength = primDimensions.length
    val result = new Array[Partition](primDimLength)
    for (i <- 0 until primDimLength) {
      result(i) = new CarbonColumnDictPatition(i, primDimensions(i))
    }
    result
  }

  override def internalCompute(split: Partition, context: TaskContext)
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
    val mapIdWithSet = new mutable.HashMap[String, mutable.HashSet[String]]
    val columnValues = new mutable.HashSet[String]
    val distinctValues = (theSplit.index, columnValues)
    mapIdWithSet.put(primDimension.getColumnId, columnValues)
    // use parser to generate new dict value
    val dimensionParser = GlobalDictionaryUtil.generateParserForDimension(
      Some(primDimension),
      GlobalDictionaryUtil.createDataFormat(carbonLoadModel.getDelimiters),
      mapIdWithSet).get
    // parse the column data
    while (colDictData.hasNext) {
      dimensionParser.parseString(colDictData.next()(0))
    }
    Array((distinctValues._1,
      ColumnDistinctValues(distinctValues._2.toArray, 0L))).iterator
  }
}
