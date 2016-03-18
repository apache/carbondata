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

package com.huawei.datasight.spark.rdd

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BinaryArithmetic
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class SqlUdf(fn: (InternalRow) => Any, exprs: Expression*) extends Expression with CodegenFallback {
  self: Product =>

  override def children = exprs

  override def dataType = StringType

  override def nullable = true

  // exprs.last.nullable
  override def references = AttributeSet(exprs.flatMap(_.references.iterator))

  override def foldable = !children.exists(!_.foldable)

  override def toString = fn.toString + "(" + exprs.foreach(" " + _.toString) + ")"

  type EvaluatedType = Any

  override def eval(input: InternalRow): Any = {
    val exprEvals = exprs.map {
      _.eval(input) match {
        case d: MeasureAggregator => d.getValue()
        case others => others
      }
    }
    evalConvert(exprEvals)
  }

  @inline
  def evalConvert(a: Seq[Any]): Any = {
    fn(new GenericMutableRow(a.toArray))
  }

}


trait AddColumnExpression {
  def getExpression: Expression
}

trait GroupbyExpression extends AddColumnExpression {
  def getGroupByColumn: Expression
}


case class Range(udf: Expression, expr: Expression) extends GroupbyExpression {
  override def getExpression: Expression = udf

  override def getGroupByColumn: Expression = expr
}

case class AddColumn(udf: Expression) extends AddColumnExpression {
  override def getExpression: Expression = udf
}
