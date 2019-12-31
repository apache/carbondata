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

import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.{ArrayUtils, StringUtils}
import org.apache.spark._
import org.apache.spark.sql.Row

import org.apache.carbondata.core.constants.CarbonLoadOptionConstants
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.processing.loading.exception.NoRetryException

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
case class DictionaryLoadModel(
    table: AbsoluteTableIdentifier,
    dimensions: Array[CarbonDimension],
    hdfsLocation: String,
    dictfolderPath: String,
    dictFilePaths: Array[String],
    dictFileExists: Array[Boolean],
    isComplexes: Array[Boolean],
    primDimensions: Array[CarbonDimension],
    delimiters: Array[String],
    columnIdentifier: Array[ColumnIdentifier],
    isFirstLoad: Boolean,
    hdfsTempLocation: String,
    lockType: String,
    zooKeeperUrl: String,
    serializationNullFormat: String,
    defaultTimestampFormat: String,
    defaultDateFormat: String) extends Serializable

case class ColumnDistinctValues(values: Array[String], rowCount: Long) extends Serializable

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
