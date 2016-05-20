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

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, GenericMutableRow}

import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.query.carbonfilterinterface.{ExpressionType, RowIntf}
import org.carbondata.query.expression.{ColumnExpression, Expression, ExpressionResult}
import org.carbondata.query.expression.conditional.ConditionalExpression
import org.carbondata.query.expression.exception.FilterUnsupportedException
import org.carbondata.spark.util.CarbonScalaUtil

class SparkUnknownExpression(sparkExp: SparkExpression)
  extends Expression with ConditionalExpression {

  children.addAll(getColumnList())

  override def evaluate(carbonRowInstance: RowIntf): ExpressionResult = {

    val values = carbonRowInstance.getValues().toSeq.map { value =>
      value match {
        case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
        case d: java.math.BigDecimal =>
          val javaDecVal = new java.math.BigDecimal(d.toString())
          val scalaDecVal = new scala.math.BigDecimal(javaDecVal)
          val decConverter = new org.apache.spark.sql.types.Decimal()
          decConverter.set(scalaDecVal)
        case _ => value
      }
    }
    try {
      val sparkRes = sparkExp.eval(
        new GenericMutableRow(values.map(a => a.asInstanceOf[Any]).toArray)
      )

      new ExpressionResult(CarbonScalaUtil.convertSparkToCarbonDataType(sparkExp.dataType),
        sparkRes
      );
    }
    catch {
      case e: Exception => throw new FilterUnsupportedException(e.getMessage());
    }
  }

  override def getFilterExpressionType(): ExpressionType = {
    ExpressionType.UNKNOWN
  }

  override def getString(): String = {
    sparkExp.toString()
  }


  def getColumnList(): java.util.List[ColumnExpression] = {

    val lst = new java.util.ArrayList[ColumnExpression]()
    getColumnListFromExpressionTree(sparkExp, lst)
    lst
  }

  def getAllColumnList(): java.util.List[ColumnExpression] = {
    val lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    lst
  }

  def isSingleDimension(): Boolean = {
    var lst = new java.util.ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    if (lst.size == 1 && lst.get(0).isDimension) {
      true
    }
    else {
      false
    }
  }

  def getColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
    list: java.util.List[ColumnExpression]): Unit = {
    sparkCurrentExp match {
      case carbonBoundRef: CarbonBoundReference =>
        val foundExp = list.asScala
          .find(p => p.getColumnName() == carbonBoundRef.colExp.getColumnName())
        if (foundExp.isEmpty) {
          carbonBoundRef.colExp.setColIndex(list.size)
          list.add(carbonBoundRef.colExp)
        } else {
          carbonBoundRef.colExp.setColIndex(foundExp.get.getColIndex())
        }
      case _ => sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
    }
  }


  def getAllColumnListFromExpressionTree(sparkCurrentExp: SparkExpression,
    list: List[ColumnExpression]): List[ColumnExpression] = {
    sparkCurrentExp match {
      case carbonBoundRef: CarbonBoundReference => list.add(carbonBoundRef.colExp)
      case _ => sparkCurrentExp.children.foreach(getColumnListFromExpressionTree(_, list))
    }
    list
  }

  def isDirectDictionaryColumns(): Boolean = {
    var lst = new ArrayList[ColumnExpression]()
    getAllColumnListFromExpressionTree(sparkExp, lst)
    if (lst.get(0).getCarbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      true
    }
    else {
      false
    }
  }
}
