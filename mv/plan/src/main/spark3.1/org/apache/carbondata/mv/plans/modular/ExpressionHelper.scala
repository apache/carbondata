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

package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, CollapseProject, CollapseRepartition, CollapseWindow, ColumnPruning, CombineFilters, CombineUnions, ConstantFolding, EliminateLimits, EliminateOuterJoin, EliminateSerialization, EliminateSorts, FoldablePropagation, NullPropagation, PushDownPredicates, PushPredicateThroughJoin, PushProjectionThroughUnion, RemoveDispensableExpressions, RemoveRedundantAliases, ReorderAssociativeOperator, ReorderJoin, RewriteCorrelatedScalarSubquery, SimplifyBinaryComparison, SimplifyCaseConversionExpressions, SimplifyCasts, SimplifyConditionals}
import org.apache.spark.sql.catalyst.plans.{logical, JoinType, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, LogicalPlan, Statistics, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, Metadata}
import scala.reflect.ClassTag

import org.apache.carbondata.mv.plans.util.BirdcageOptimizer

object ExpressionHelper {

  def createReference(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata,
      exprId: ExprId,
      qualifier: Option[String],
      attrRef: NamedExpression = null): AttributeReference = {
    val qf = if (qualifier.nonEmpty) Seq(qualifier.get) else Seq.empty
    AttributeReference(name, dataType, nullable, metadata)(exprId, qf)
  }

  def createAlias(
      child: Expression,
      name: String,
      exprId: ExprId,
      qualifier: Option[String]): Alias = {
    val qf = if (qualifier.nonEmpty) Seq(qualifier.get) else Seq.empty
    Alias(child, name)(exprId, qf, None)
  }

  def getTheLastQualifier(reference: AttributeReference): String = {
    reference.qualifier.reverse.head
  }

}
