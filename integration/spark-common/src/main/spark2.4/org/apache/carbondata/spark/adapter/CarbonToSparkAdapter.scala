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

package org.apache.carbondata.spark.adapter

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.{DataType, Metadata}

object CarbonToSparkAdapter {
  def createFilePartition(index: Int, files: ArrayBuffer[PartitionedFile]) = {
    FilePartition(index, files.toArray)
  }

  def createAttributeReference(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata,
      exprId: ExprId,
      qualifier: Option[String],
      attrRef : NamedExpression = null): AttributeReference = {
    val qf = if (qualifier.nonEmpty) Seq(qualifier.get) else Seq.empty
    AttributeReference(
      name,
      dataType,
      nullable,
      metadata)(exprId, qf)
  }

  def createAliasRef(
      child: Expression,
      name: String,
      exprId: ExprId = NamedExpression.newExprId,
      qualifier: Option[String] = None,
      explicitMetadata: Option[Metadata] = None,
      namedExpr : Option[NamedExpression] = None ) : Alias = {
    Alias(child, name)(
      exprId,
      if (qualifier.nonEmpty) Seq(qualifier.get) else Seq(),
      explicitMetadata)
  }
}
