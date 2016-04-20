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

import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SplitEvaluation
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AggregateExpression1
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.PartialAggregate1
import org.carbondata.integration.spark.agg._
import scala.collection.mutable.MutableList
import org.apache.spark.sql.cubemodel.tableModel
import org.apache.spark.sql.hive.HiveContext

/**
  * Top command
  */
case class Top(count: Int, topOrBottom: Int, dim: NamedExpression, msr: NamedExpression, child: LogicalPlan) extends UnaryNode {
  def output = child.output

  override def references = {
    val list = List(dim, msr)
    AttributeSet(list.flatMap(_.references))
  }
}

object getDB {

  def getDatabaseName(dbName: Option[String], sqlContext: SQLContext): String =
    dbName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)

}

/**
  * Shows schemas
  */
case class ShowSchemaCommand(cmd: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("result", StringType, nullable = false)())
}

/**
  * Shows AggregateTables of a schema
  */
case class ShowCreateCubeCommand(cm: tableModel) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("createCubeCmd", StringType, nullable = false)())
}

/**
  * Shows AggregateTables of a schema
  */
case class ShowAggregateTablesCommand(schemaNameOp: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("tableName", StringType, nullable = false)())
}

/**
  * Shows cubes in schema
  */
case class ShowCubeCommand(schemaNameOp: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("cubeName", StringType, nullable = false)(),
      AttributeReference("isRegisteredWithSpark", BooleanType, nullable = false)())
}


/**
  * Shows cubes in schema
  */
case class ShowAllCubeCommand() extends LogicalPlan with Command /*with ControlCommand*/ {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("schemaName", StringType, nullable = false)(),
      AttributeReference("cubeName", StringType, nullable = false)(),
      AttributeReference("isRegisteredWithSpark", BooleanType, nullable = false)())
}

case class SuggestAggregateCommand(
                                    script: Option[String],
                                    sugType: Option[String],
                                    schemaName: Option[String],
                                    cubeName: String) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("SuggestionType", StringType, nullable = false)(),
      AttributeReference("Suggestion", StringType, nullable = false)())
}

/**
  * Shows cubes in schema
  */
case class ShowTablesDetailedCommand(schemaNameOp: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("TABLE_CAT", StringType, nullable = true)(),
      AttributeReference("TABLE_SCHEM", StringType, nullable = false)(),
      AttributeReference("TABLE_NAME", StringType, nullable = false)(),
      AttributeReference("TABLE_TYPE", StringType, nullable = false)(),
      AttributeReference("REMARKS", StringType, nullable = false)())
}

/**
  * Shows Loads in a cube
  */
case class ShowLoadsCommand(schemaNameOp: Option[String], cube: String, limit: Option[String])
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty

  override def output =
    Seq(AttributeReference("LoadSequenceId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", TimestampType, nullable = false)(),
      AttributeReference("Load End Time", TimestampType, nullable = false)())
}

/**
  * Describe formatted for hive table
  */
case class DescribeFormattedCommand(sql: String, tblIdentifier: Seq[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[AttributeReference] =
    Seq(AttributeReference("col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("comment", StringType, nullable = false)())
}

/**
  * A pattern that matches any number of project or filter operations on top of another relational
  * operator.  All filter operators are collected and their conditions are broken up and returned
  * together with the top project operator.
  * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
  * necessary.
  */
object PhysicalOperation1 extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Option[Seq[Expression]], Option[Seq[SortOrder]], Option[Expression], LogicalPlan)

  def apply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(plan)

    Some((fields.getOrElse(child.output), filters, groupby, sortOrder, limit, child))
  }

  /**
    * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
    * examples for alias in-lining/substitution.  Before:
    * {{{
    *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
    *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
    * }}}
    * After:
    * {{{
    *   SELECT key AS c1 FROM t1 WHERE key > 10
    *   SELECT key AS c2 FROM t1 WHERE key > 10
    * }}}
    */
  def collectProjectsAndFilters(plan: LogicalPlan):
  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression], Option[Seq[Expression]], Option[Seq[SortOrder]], Option[Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields), groupby, sortOrder, limit)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases, groupby, sortOrder, limit)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases, _, sortOrder, limit) = collectProjectsAndFilters(child)

        var aggExps: Seq[AggregateExpression1] = Nil
        aggregateExpressions.foreach(v => {
          val list = findAggreagateExpression(v)
          aggExps = aggExps ++ list
        })

        (fields, filters, other, aliases ++ collectAliases(aggregateExpressions), Some(aggregateExpressions), sortOrder, limit)
      case Sort(order, _, child) =>
        val (fields, filters, other, aliases, groupby, _, limit) = collectProjectsAndFilters(child)
        val substitutedOrder = order.map(s => SortOrder(substitute(aliases)(s.child), s.direction))
        (fields, filters, other, aliases, groupby, Some(substitutedOrder), limit)
      case Limit(limitExpr, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, _) = collectProjectsAndFilters(child)
        (fields, filters, other, aliases, groupby, sortOrder, Some(limitExpr))
      case other =>
        (None, Nil, other, Map.empty, None, None, None)
    }

  def findAggreagateExpression(expr: Expression): Seq[AggregateExpression1] = {
    val exprList = expr match {
      case d: AggregateExpression1 => d :: Nil
      case Alias(ref, name) => findAggreagateExpression(ref)
      case other => {
        var listout: Seq[AggregateExpression1] = Nil

        other.children.foreach(v => {
          val list = findAggreagateExpression(v)
          listout = listout ++ list
        })
        listout
      }
    }
    exprList
  }

  def collectProjectsAndFilters1(plan: LogicalPlan):
  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression], Option[Seq[Expression]], Option[Seq[SortOrder]], Option[Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields), groupby, sortOrder, limit)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases, groupby, sortOrder, limit)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases, _, sortOrder, limit) = collectProjectsAndFilters(child)
        val aggExps = aggregateExpressions.map {
          case Alias(ref, name) => ref
          case other => other
        }.filter {
          case d: AggregateExpression1 => true
          case _ => false
        }
        (fields, filters, other, aliases ++ collectAliases(aggregateExpressions), Some(aggExps), sortOrder, limit)
      case Sort(order, _, child) =>
        val (fields, filters, other, aliases, groupby, _, limit) = collectProjectsAndFilters(child)
        val substitutedOrder = order.map(s => SortOrder(substitute(aliases)(s.child), s.direction))
        (fields, filters, other, aliases, groupby, Some(substitutedOrder), limit)
      case Limit(limitExpr, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, _) = collectProjectsAndFilters(child)
        (fields, filters, other, aliases, groupby, sortOrder, Some(limitExpr))
      case other =>
        (None, Nil, other, Map.empty, None, None, None)
    }

  def collectAliases(fields: Seq[Expression]) = fields.collect {
    case a@Alias(child, _) => a.toAttribute.asInstanceOf[Attribute] -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = expr.transform {
    case a@Alias(ref: AttributeReference, name) =>
      aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

    case a: AttributeReference =>
      aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
  }
}

/**
  * Matches a logical aggregation that can be performed on distributed data in two steps.  The first
  * operates on the data in each partition performing partial aggregation for each group.  The second
  * occurs after the shuffle and completes the aggregation.
  *
  * This pattern will only match if all aggregate expressions can be computed partially and will
  * return the rewritten aggregation expressions for both phases.
  *
  * The returned values for this match are as follows:
  * - Grouping attributes for the final aggregation.
  * - Aggregates for the final aggregation.
  * - Grouping expressions for the partial aggregation.
  * - Partial aggregate expressions.
  * - Input to the aggregation.
  */
object PartialAggregation {
  type ReturnType =
  (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def convertAggregatesForPushdown(convertUnknown: Boolean, rewrittenAggregateExpressions: Seq[Expression]) = {
    var counter: Int = 0
    var updatedExpressions = MutableList[Expression]()
    rewrittenAggregateExpressions.foreach(v => {
      val updated = convertAggregate(v, counter, convertUnknown)
      updatedExpressions += updated
      counter = counter + 1
    })
    updatedExpressions.toSeq
  }

  def makePositionLiteral(expr: Expression, index: Int): PositionLiteral = {
    val posLiteral = PositionLiteral(expr, MeasureAggregatorUDT)
    posLiteral.setPosition(index)
    posLiteral
  }

  def convertAggregate(current: Expression, index: Int, convertUnknown: Boolean): Expression =
    if (convertUnknown) {
      current.transform {
        case a@SumCarbon(_, _) => a
        case a@AverageCarbon(_, _) => a
        case a@MinCarbon(_, _) => a
        case a@MaxCarbon(_, _) => a
        case a@SumDistinctCarbon(_, _) => a
        case a@CountDistinctCarbon(_) => a
        case a@CountCarbon(_) => a
        case anyAggr: AggregateExpression1 => anyAggr
      }
    } else {
      current.transform {
        case a@Sum(attr: AttributeReference) => SumCarbon(makePositionLiteral(attr, index))
        case a@Sum(cast@Cast(attr: AttributeReference, _)) => SumCarbon(makePositionLiteral(attr, index), cast.dataType)
        case a@Average(attr: AttributeReference) => AverageCarbon(makePositionLiteral(attr, index))
        case a@Average(cast@Cast(attr: AttributeReference, _)) => AverageCarbon(makePositionLiteral(attr, index), cast.dataType)
        case a@Min(attr: AttributeReference) => MinCarbon(makePositionLiteral(attr, index))
        case a@Min(cast@Cast(attr: AttributeReference, _)) => MinCarbon(makePositionLiteral(attr, index), cast.dataType)
        case a@Max(attr: AttributeReference) => MaxCarbon(makePositionLiteral(attr, index))
        case a@Max(cast@Cast(attr: AttributeReference, _)) => MaxCarbon(makePositionLiteral(attr, index), cast.dataType)
        case a@SumDistinct(attr: AttributeReference) => SumDistinctCarbon(makePositionLiteral(attr, index))
        case a@SumDistinct(cast@Cast(attr: AttributeReference, _)) => SumDistinctCarbon(makePositionLiteral(attr, index), cast.dataType)
        case a@CountDistinct(attr: AttributeReference) => CountDistinctCarbon(makePositionLiteral(attr, index))
        case a@CountDistinct(childSeq) if (childSeq.size == 1) =>
          childSeq(0) match {
            case attr: AttributeReference => CountDistinctCarbon(makePositionLiteral(attr, index))
            case _ => a
          }
        case a@Count(s@Literal(_, _)) =>
          CountCarbon(makePositionLiteral(s, index))
        case a@Count(attr: AttributeReference) =>
          if (attr.name.equals("*")) {
            CountCarbon(makePositionLiteral(Literal("*"), index))
          } else {
            CountCarbon(makePositionLiteral(attr, index))
          }
      }
    }

  def unapply(plan: LogicalPlan): Option[ReturnType] = unapply((plan, false))

  def unapply(combinedPlan: (LogicalPlan, Boolean)): Option[ReturnType] = combinedPlan._1 match {
    case Aggregate(groupingExpressions, aggregateExpressionsOrig, child) =>

      //if detailed query dont convert aggregate expressions to Carbon Aggregate expressions
      val aggregateExpressions =
        if (combinedPlan._2) aggregateExpressionsOrig
        else convertAggregatesForPushdown(false, aggregateExpressionsOrig)
      // Collect all aggregate expressions.
      val allAggregates =
        aggregateExpressions.flatMap(_ collect { case a: AggregateExpression1 => a })
      // Collect all aggregate expressions that can be computed partially.
      val partialAggregates =
        aggregateExpressions.flatMap(_ collect { case p: PartialAggregate1 => p })

      // Only do partial aggregation if supported by all aggregate expressions.
      if (allAggregates.size == partialAggregates.size) {
        // Create a map of expressions to their partial evaluations for all aggregate expressions.
        val partialEvaluations: Map[TreeNodeRef, SplitEvaluation] =
          partialAggregates.map(a => (new TreeNodeRef(a), a.asPartial)).toMap

        // We need to pass all grouping expressions though so the grouping can happen a second
        // time. However some of them might be unnamed so we alias them allowing them to be
        // referenced in the second aggregation.
        val namedGroupingExpressions: Map[Expression, NamedExpression] = groupingExpressions.map {
          case n: NamedExpression => (n, n)
          case other => (other, Alias(other, "PartialGroup")())
        }.toMap

        // Replace aggregations with a new expression that computes the result from the already
        // computed partial evaluations and grouping values.
        val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
          case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
            partialEvaluations(new TreeNodeRef(e)).finalEvaluation

          case e: Expression =>
            // Should trim aliases around `GetField`s. These aliases are introduced while
            // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
            // (Should we just turn `GetField` into a `NamedExpression`?)
            namedGroupingExpressions.collectFirst {
              case (expr, ne) if expr semanticEquals e => ne.toAttribute
            }.getOrElse(e)
        }).asInstanceOf[Seq[NamedExpression]]

        val partialComputation =
          (namedGroupingExpressions.values ++
            partialEvaluations.values.flatMap(_.partialEvaluations)).toSeq

        // Convert the other aggregations for push down to Carbon layer. Here don't touch earlier converted native carbon aggregators.
        val convertedPartialComputation =
          if (combinedPlan._2) partialComputation
          else convertAggregatesForPushdown(true, partialComputation).asInstanceOf[Seq[NamedExpression]]

        val namedGroupingAttributes = namedGroupingExpressions.values.map(_.toAttribute).toSeq

        Some(
          (namedGroupingAttributes,
            rewrittenAggregateExpressions,
            groupingExpressions,
            convertedPartialComputation,
            child))
      } else {
        None
      }
    case _ => None
  }
}


