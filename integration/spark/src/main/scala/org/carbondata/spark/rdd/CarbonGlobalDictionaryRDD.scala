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

import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.control.Breaks.{break, breakable}

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark.{Logging, Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{CarbonTableIdentifier, ColumnIdentifier}
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.load.CarbonLoaderUtil
import org.carbondata.spark.util.GlobalDictionaryUtil

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
    setOpt: Option[HashSet[String]]) extends GenericParser {
  val (hasDictEncoding, set: HashSet[String]) = setOpt match {
    case None => (false, new HashSet[String])
    case Some(x) => (true, x)
  }

  def addChild(child: GenericParser): Unit = {
  }

  def parseString(input: String): Unit = {
    if (hasDictEncoding) {
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
      // generate distinct value list
      try {
        val t1 = System.currentTimeMillis
        dictionaryForDistinctValueLookUp = if (model.dictFileExists(split.index)) {
          CarbonLoaderUtil.getDictionary(model.table,
            model.columnIdentifier(split.index),
            model.hdfsLocation,
            model.primDimensions(split.index).getDataType
          )
        } else {
          null
        }
        val t2 = System.currentTimeMillis
        val valuesBuffer = new mutable.HashSet[String]
        val rddIter = firstParent[(Int, ColumnDistinctValues)].iterator(split, context)
        var rowCount = 0L
        breakable {
          while (rddIter.hasNext) {
            val distinctValueList = rddIter.next()._2
            valuesBuffer ++= distinctValueList.values
            rowCount += distinctValueList.rowCount
            // check high cardinality
            if (model.isFirstLoad && model.highCardIdentifyEnable
                && !model.isComplexes(split.index)) {
              isHighCardinalityColumn = GlobalDictionaryUtil.isHighCardinalityColumn(
                valuesBuffer.size, rowCount, model)
              if (isHighCardinalityColumn) {
                break
              }
            }
          }
        }

        if (isHighCardinalityColumn) {
          LOGGER.info("column " + model.table.getTableUniqueName + "." +
            model.primDimensions(split.index).getColName + " is high cardinality column")
        } else {
          val t3 = System.currentTimeMillis
          val distinctValueCount = GlobalDictionaryUtil.generateAndWriteNewDistinctValueList(
            valuesBuffer, dictionaryForDistinctValueLookUp, model, split.index)
          // clear the value buffer after writing dictionary data
          valuesBuffer.clear
          org.carbondata.core.util.CarbonUtil
            .clearDictionaryCache(dictionaryForDistinctValueLookUp);
          dictionaryForDistinctValueLookUpCleared = true
          val t4 = System.currentTimeMillis
          if (distinctValueCount > 0) {
            dictionaryForSortIndexWriting = CarbonLoaderUtil.getDictionary(model.table,
              model.columnIdentifier(split.index),
              model.hdfsLocation,
              model.primDimensions(split.index).getDataType)
            GlobalDictionaryUtil.writeGlobalDictionaryColumnSortInfo(model, split.index,
              dictionaryForSortIndexWriting)
            val t5 = System.currentTimeMillis
            LOGGER.info("\n columnName:" + model.primDimensions(split.index).getColName +
              "\n columnId:" + model.primDimensions(split.index).getColumnId +
              "\n new distinct values count:" + distinctValueCount +
              "\n create dictionary cache:" + (t2 - t1) +
              "\n combine lists:" + (t3 - t2) +
              "\n sort list, distinct and write:" + (t4 - t3) +
              "\n write sort info:" + (t5 - t4))
          }
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
