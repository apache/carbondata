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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, LeafExpression, NamedExpression}
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.core.scan.expression.ColumnExpression

case class CarbonBoundReference(colExp: ColumnExpression, dataType: DataType, nullable: Boolean)
  extends LeafExpression with NamedExpression with CodegenFallback {

  type EvaluatedType = Any

  override def toString: String = s"input[" + colExp.getColIndex + "]"

  override def eval(input: InternalRow): Any = input.get(colExp.getColIndex, dataType)

  override def name: String = colExp.getColumnName

  override def toAttribute: Attribute = throw new UnsupportedOperationException

  override def exprId: ExprId = throw new UnsupportedOperationException

  override def qualifier: Option[String] = null

  override def newInstance(): NamedExpression = throw new UnsupportedOperationException
}
