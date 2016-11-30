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

package org.apache.spark.sql

import java.util.{ArrayList, List}

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, GenericMutableRow}

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.scan.expression.{ColumnExpression, ExpressionResult, UnknownExpression}
import org.apache.carbondata.scan.expression.conditional.ConditionalExpression
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException
import org.apache.carbondata.scan.filter.intf.{ExpressionType, RowIntf}
import org.apache.carbondata.spark.util.CarbonScalaUtil

class SparkUnknownExpression(var sparkExp: SparkExpression)
  extends UnknownExpression with ConditionalExpression {

  private var evaluateExpression: (InternalRow) => Any = sparkExp.eval
  private var isExecutor: Boolean = false
  children.addAll(getColumnList())

  override def evaluate(carbonRowInstance: RowIntf): ExpressionResult = {

    val values = carbonRowInstance.getValues.toSeq.map {
      case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case d: java.math.BigDecimal =>
        val javaDecVal = new java.math.BigDecimal(d.toString)
        val scalaDecVal = new scala.math.BigDecimal(javaDecVal)
        val decConverter = new org.apache.spark.sql.types.Decimal()
        decConverter.set(scalaDecVal)
      case value => value
    }
    try {
      val result = evaluateExpression(
        new GenericMutableRow(values.map(a => a.asInstanceOf[Any]).toArray))
      val sparkRes = if (isExecutor) {
        result.asInstanceOf[InternalRow].get(0, sparkExp.dataType)
      } else {
        result
      }
      new ExpressionResult(CarbonScalaUtil.convertSparkToCarbonDataType(sparkExp.dataType),
        sparkRes
      )
    } catch {
      case e: Exception => throw new FilterUnsupportedException(e.getMessage)
    }
  }

  override def getFilterExpressionType: ExpressionType = {
    ExpressionType.UNKNOWN
  }

  override def getString: String = {
    sparkExp.toString()
  }

  def setEvaluateExpression(evaluateExpression: (InternalRow) => Any): Unit = {
    this.evaluateExpression = evaluateExpression
    isExecutor = true
  }

  def getColumnList: java.util.List[ColumnExpression] = {

    val lst = new java.util.ArrayList[ColumnExpression]()
    getColumnListFromExpressionTree(sparkExp, lst)
    lst
  }
  def getLiterals: java.util.List[ExpressionResult] = {

    val lst = new java.util.ArrayList[ExpressionResult]()
    lst
  }

  def getAllColumnList: java.util.List[ColumnExpression] = {
    val lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    lst
  }

  def isSingleDimension: Boolean = {
    val lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    if (lst.size == 1 && lst.get(0).isDimension) {
      true
    } else {
      false
    }
  }

  def getColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
      list: java.util.List[ColumnExpression]): Unit = {
    sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
  }


  def getAllColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
      list: List[ColumnExpression]): List[ColumnExpression] = {
    sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
    list
  }

  def isDirectDictionaryColumns: Boolean = {
    val lst = new ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    if (lst.get(0).getCarbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      true
    } else {
      false
    }
  }
}
