/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import java.io.{DataInput, DataOutput}

import org.apache.spark.sql.catalyst.expressions.{AggregateExpression1, AggregateFunction1, GenericMutableRow}
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder
import org.carbondata.query.aggregator.{MeasureAggregator, CustomMeasureAggregator}
import org.carbondata.query.expression.ColumnExpression
import org.carbondata.query.molapfilterinterface.RowIntf

import scala.collection.JavaConverters._

/**
  * Custom Aggregator serialized and used to pushdown all aggregate functions from spark layer with expressions to Molap layer
  */
@SerialVersionUID(-3787749110799088697l)
class SparkUnknownMolapAggregator(partialAggregate: AggregateExpression1) extends CustomMeasureAggregator {

  def this() = this(null) //For serializattion

  @transient var partialFunction: AggregateFunction1 = null

  @transient var allColumns: java.util.List[ColumnExpression] = null

  val result = scala.collection.mutable.MutableList[GenericMutableRow]()

  def getPartialFunction = {
    if (partialFunction == null) {
      partialFunction = partialAggregate.newInstance
    }
    partialFunction
  }

  var isRowsAggregated: Boolean = false

  override def agg(newVal: Double) = {

    throw new UnsupportedOperationException("agg(double) is not a valid method for aggregation");
  }

  override def agg(newVal: Any) = {
    throw new UnsupportedOperationException("agg(Object) is not a valid method for aggregation");
  }

  override def agg(newVal: MolapReadDataHolder, index: Int) = {
    throw new UnsupportedOperationException("agg(MolapReadDataHolder, int) is not a valid method for aggregation");
  }

  override def getByteArray(): Array[Byte] = {
    throw new UnsupportedOperationException("getByteArray  is not implemented yet");
  }

  override def getDoubleValue(): java.lang.Double = {
    throw new UnsupportedOperationException("getValue() is not a valid method for result");
  }

  override def getLongValue(): java.lang.Long = {
    throw new UnsupportedOperationException("getLongValue() is not a valid method for result");
  }

  override def getBigDecimalValue(): java.math.BigDecimal = {
    throw new UnsupportedOperationException("getBigDecimalValue() is not a valid method for result");
  }

  override def getValueObject(): Object = {

    result.iterator.foreach(v => getPartialFunction.update(v))

    val output = getPartialFunction.eval(null)

    output.asInstanceOf[Object];
  }

  override def merge(aggregator: MeasureAggregator) = {
    if (result.size > 0) {
      result.iterator.foreach(v => {
        getPartialFunction.update(v)
      })

      //clear result after submitting to partial function
      result.clear
    }

    if (aggregator.isInstanceOf[SparkUnknownMolapAggregator]) {
      aggregator.asInstanceOf[SparkUnknownMolapAggregator].result.iterator.foreach(v => {
        getPartialFunction.update(v)
      })

      aggregator.asInstanceOf[SparkUnknownMolapAggregator].result.clear
    } else {
      throw new Exception("Invalid merge expected type is" + this.getClass().getName());
    }
  }

  override def isFirstTime(): Boolean = {
    isRowsAggregated
  }

  override def writeData(output: DataOutput) = {
    throw new UnsupportedOperationException();
  }

  override def readData(inPut: DataInput) = {
    throw new UnsupportedOperationException();
  }

  override def merge(value: Array[Byte]) {

    throw new UnsupportedOperationException();
  }

  override def get(): MeasureAggregator = {
    //Get means, Partition level aggregation is done and pending for merge with other or getValue
    // So evaluate and store the temporary result here

    //    result += getPartialFunction.eval(null);
    this
  }

  override def compareTo(aggre: MeasureAggregator): Int = {
    return 0
  }

  override def getCopy(): MeasureAggregator = {
    return new SparkUnknownMolapAggregator(partialAggregate)
  }

  override def setNewValue(newVal: Object) = {

  }

  override def getColumns() = {
    if (allColumns == null) {
      allColumns = partialAggregate.flatMap(_ collect { case a: MolapBoundReference => a.colExp }).asJava
    }
    allColumns
  }

  override def agg(row: RowIntf) = {
    isRowsAggregated = true
    val values = row.getValues().toSeq.map { value =>
      value match {
        case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
        // solve: java.math.BigDecimal cannot be cast to org.apache.spark.sql.types.Decimal
        case d: java.math.BigDecimal =>      {
            val javaDecVal = new java.math.BigDecimal(d.toString())
            val scalaDecVal = new scala.math.BigDecimal(javaDecVal)
            val decConverter = new org.apache.spark.sql.types.Decimal()

            decConverter.set(scalaDecVal)
        }
        case _ => value
      }
    }
    result += new GenericMutableRow(values.map(a => a.asInstanceOf[Any]).toArray)
  }

}