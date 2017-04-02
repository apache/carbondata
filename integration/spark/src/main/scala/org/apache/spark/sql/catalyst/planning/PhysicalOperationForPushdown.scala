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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import org.apache.carbondata.core.scan.model.QueryDimension

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperationForPushdown extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan,
      Seq[QueryDimension], Int, Seq[Expression], Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _, sorts, limitValue, grpExp, aggExp)
    = collectSortsAndProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child, sorts, limitValue, grpExp, aggExp))
  }

  def collectSortsAndProjectsAndFilters(plan: LogicalPlan): (Option[Seq[NamedExpression]],
      Seq[Expression], LogicalPlan, Map[Attribute, Expression],
      Seq[QueryDimension], Int, Seq[Expression], Seq[NamedExpression]) =
    plan match {

      case CarbonPushDownToScan(order, limit, groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases,
          _, _, _, _) = collectSortsAndProjectsAndFilters(child)

        (fields, filters, other, aliases, order,
          limit, groupingExpressions, aggregateExpressions)

      case Project(fields, child) =>
        val (_, filters, other, aliases, sorts, limitValue, groupingExpressions,
          aggregateExpressions) = collectSortsAndProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields),
          sorts, limitValue, groupingExpressions, aggregateExpressions)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, sorts, limitValue,
          groupingExpressions, aggregateExpressions) = collectSortsAndProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition),
          other, aliases, sorts, limitValue, groupingExpressions, aggregateExpressions)

      case other =>
        (None, Nil, other, Map.empty, Nil, 0, Nil, Nil)
    }

  def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}
