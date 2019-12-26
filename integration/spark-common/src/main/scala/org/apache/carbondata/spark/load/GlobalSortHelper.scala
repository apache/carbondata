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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.comparator._
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

object GlobalSortHelper {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   *
   * @param loadModel       Carbon load model instance
   * @param badRecordsAccum Accumulator to maintain the load state if 0 then success id !0 then
   *                        partial successfull
   * @param hasBadRecord    if <code>true<code> then load bad records vice versa.
   */
  def badRecordsLogger(loadModel: CarbonLoadModel,
      badRecordsAccum: LongAccumulator, hasBadRecord: Boolean): Unit = {
    if (hasBadRecord) {
      LOGGER.error("Data Load is partially success for table " + loadModel.getTableName)
      badRecordsAccum.add(1)
    } else {
      LOGGER.info("Data loading is successful for table " + loadModel.getTableName)
    }
  }

  def sortBy(updatedRdd: RDD[InternalRow],
      numPartitions: Int,
      dataTypes: Seq[DataType]
  ): RDD[InternalRow] = {
    val keyExtractors = generateKeyExtractor(dataTypes)
    val rowComparator = generateRowComparator(dataTypes)
    import scala.reflect.classTag
    updatedRdd.sortBy(x => getKey(x, keyExtractors), true, numPartitions)(
      rowComparator, classTag[Array[AnyRef]])
  }

  def getKey(row: InternalRow, keyExtractors: Array[KeyExtractor]): Array[AnyRef] = {
    val length = Math.min(row.numFields, keyExtractors.length)
    val key = new Array[AnyRef](keyExtractors.length)
    for( i <- 0 until length) {
      key(i) = keyExtractors(i).getData(row)
    }
    key
  }

  def generateKeyExtractor(dataTypes: Seq[DataType]): Array[KeyExtractor] = {
    dataTypes
      .zipWithIndex
      .map { attr =>
        attr._1 match {
          case StringType => UTF8StringKeyExtractor(attr._2)
          case ShortType => ShortKeyExtractor(attr._2)
          case IntegerType => IntKeyExtractor(attr._2)
          case LongType => LongKeyExtractor(attr._2)
          case DoubleType => DoubleKeyExtractor(attr._2)
          case FloatType => FloatKeyExtractor(attr._2)
          case ByteType => ByteKeyExtractor(attr._2)
          case BooleanType => BooleanKeyExtractor(attr._2)
          case BinaryType => BinaryKeyExtractor(attr._2)
          case decimal: DecimalType =>
            DecimalKeyExtractor(attr._2, decimal.precision, decimal.scale)
          case _ =>
            throw new UnsupportedOperationException("unsupported sort by " + attr._1)
        }
      }
      .toArray
  }

  def generateRowComparator(dataTypes: Seq[DataType]): InternalRowComparator = {
    val comparators = dataTypes
      .zipWithIndex
      .map { attr =>
        val comparator = attr._1 match {
          case StringType => new StringSerializableComparator()
          case ShortType => new ShortSerializableComparator()
          case IntegerType => new IntSerializableComparator()
          case LongType => new LongSerializableComparator()
          case DoubleType => new DoubleSerializableComparator()
          case FloatType => new FloatSerializableComparator()
          case ByteType => new ByteArraySerializableComparator()
          case BooleanType => new BooleanSerializableComparator()
          case BinaryType => new ByteArraySerializableComparator()
          case _: DecimalType => new DecimalSerializableComparator()
          case _ =>
            throw new UnsupportedOperationException("unsupported compare " + attr._1)
        }
        comparator.asInstanceOf[Comparator[AnyRef]]
      }
      .toArray
    InternalRowComparator(comparators)
  }
}

abstract class KeyExtractor(index: Int) extends Serializable {
  def getData(row: InternalRow): AnyRef = {
    if (row.isNullAt(index)) {
      null
    } else {
      getNotNull(row)
    }
  }

  def getNotNull(row: InternalRow): AnyRef
}

case class BooleanKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Boolean.box(row.getBoolean(index))
  }
}

case class ByteKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Byte.box(row.getByte(index))
  }
}

case class ShortKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Short.box(row.getShort(index))
  }
}

case class IntKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Int.box(row.getInt(index))
  }
}

case class LongKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Long.box(row.getLong(index))
  }
}

case class FloatKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Float.box(row.getFloat(index))
  }
}

case class DoubleKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    Double.box(row.getDouble(index))
  }
}

case class DecimalKeyExtractor(index: Int, precision: Int, scale: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    row.getDecimal(index, precision, scale)
  }
}

case class UTF8StringKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    row.getUTF8String(index)
  }
}

case class BinaryKeyExtractor(index: Int) extends KeyExtractor(index) {
  override def getNotNull(row: InternalRow): AnyRef = {
    row.getBinary(index)
  }
}

case class InternalRowComparator(
    comparators: Array[Comparator[AnyRef]]
) extends Ordering[Array[AnyRef]] {
  override def compare(o1: Array[AnyRef], o2: Array[AnyRef]): Int = {
    var temp = 0
    for (i <- 0 until comparators.length) {
      temp = comparators(i).compare(o1(i), o2(i))
      if (temp != 0) {
        return temp
      }
    }
    temp
  }
}
