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

import scala.collection.mutable.MutableList

import org.apache.spark.sql.agg.{CarbonAverage, CarbonCount}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.execution.command.tableModel
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}
import org.apache.spark.sql.types._

/**
 * Top command
 */
case class Top(count: Int, topOrBottom: Int, dim: NamedExpression, msr: NamedExpression,
    child: LogicalPlan) extends UnaryNode {
  def output: Seq[Attribute] = child.output

  override def references: AttributeSet = {
    val list = List(dim, msr)
    AttributeSet(list.flatMap(_.references))
  }
}

object getDB {

  def getDatabaseName(dbName: Option[String], sqlContext: SQLContext): String = {
    dbName.getOrElse(sqlContext.asInstanceOf[HiveContext].catalog.client.currentDatabase)
  }

}

/**
 * Shows schemas
 */
case class ShowSchemaCommand(cmd: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }
}

/**
 * Shows AggregateTables of a schema
 */
case class ShowCreateCubeCommand(cm: tableModel) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("createCubeCmd", StringType, nullable = false)())
  }
}

/**
 * Shows AggregateTables of a schema
 */
case class ShowAggregateTablesCommand(schemaNameOp: Option[String])
  extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("tableName", StringType, nullable = false)())
  }
}

/**
 * Shows cubes in schema
 */
case class ShowCubeCommand(schemaNameOp: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("tableName", StringType, nullable = false)(),
      AttributeReference("isRegisteredWithSpark", BooleanType, nullable = false)())
  }
}


/**
 * Shows cubes in schema
 */
case class ShowAllCubeCommand() extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("dbName", StringType, nullable = false)(),
      AttributeReference("tableName", StringType, nullable = false)(),
      AttributeReference("isRegisteredWithSpark", BooleanType, nullable = false)())
  }
}

case class SuggestAggregateCommand(
    script: Option[String],
    sugType: Option[String],
    schemaName: Option[String],
    cubeName: String) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("SuggestionType", StringType, nullable = false)(),
      AttributeReference("Suggestion", StringType, nullable = false)())
  }
}

/**
 * Shows cubes in schema
 */
case class ShowTablesDetailedCommand(schemaNameOp: Option[String])
  extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("TABLE_CAT", StringType, nullable = true)(),
      AttributeReference("TABLE_SCHEM", StringType, nullable = false)(),
      AttributeReference("TABLE_NAME", StringType, nullable = false)(),
      AttributeReference("TABLE_TYPE", StringType, nullable = false)(),
      AttributeReference("REMARKS", StringType, nullable = false)())
  }
}

/**
 * Shows Loads in a cube
 */
case class ShowLoadsCommand(schemaNameOp: Option[String], cube: String, limit: Option[String])
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("LoadSequenceId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", TimestampType, nullable = false)(),
      AttributeReference("Load End Time", TimestampType, nullable = false)())
  }
}

/**
 * Describe formatted for hive table
 */
case class DescribeFormattedCommand(sql: String, tblIdentifier: TableIdentifier)
  extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[AttributeReference] = {
    Seq(AttributeReference("col_name", StringType, nullable = false)(),
      AttributeReference("data_type", StringType, nullable = false)(),
      AttributeReference("comment", StringType, nullable = false)())
  }
}

case class CarbonDictionaryCatalystDecoder(
    relations: Seq[CarbonDecoderRelation],
    profile: CarbonProfile,
    aliasMap: CarbonAliasDecoderRelation,
    isOuter: Boolean,
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

abstract class CarbonProfile(attributes: Seq[Attribute]) extends Serializable {
  def isEmpty: Boolean = attributes.isEmpty
}

case class IncludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class ExcludeProfile(attributes: Seq[Attribute]) extends CarbonProfile(attributes)

case class FakeCarbonCast(child: Literal, dataType: DataType)
  extends LeafExpression with CodegenFallback {

  override def toString: String = s"FakeCarbonCast($child as ${ dataType.simpleString })"

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeCheckResult.TypeCheckSuccess
  }

  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = child.value
}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation1 extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Option[Seq[Expression]],
    Option[Seq[SortOrder]], Option[Expression], LogicalPlan)

  def apply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _, groupby, sortOrder, limit) =
      collectProjectsAndFilters(plan)

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
  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan,
    Map[Attribute, Expression], Option[Seq[Expression]],
    Option[Seq[SortOrder]], Option[Expression]) = {
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(
          child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(
          substitutedFields), groupby, sortOrder, limit)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, limit) =
          collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(
          substitutedCondition), other, aliases, groupby, sortOrder, limit)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases, _, sortOrder, limit) = collectProjectsAndFilters(
          child)

        var aggExps: Seq[AggregateExpression] = Nil
        aggregateExpressions.foreach(v => {
          val list = findAggreagateExpression(v)
          aggExps = aggExps ++ list
        })

        (fields, filters, other, aliases ++ collectAliases(aggregateExpressions), Some(
          aggregateExpressions), sortOrder, limit)
      case Sort(order, _, child) =>
        val (fields, filters, other, aliases, groupby, _, limit) = collectProjectsAndFilters(child)
        val substitutedOrder = order.map(s => SortOrder(substitute(aliases)(s.child), s.direction))
        (fields, filters, other, aliases, groupby, Some(substitutedOrder), limit)
      case Limit(limitExpr, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, _) = collectProjectsAndFilters(
          child)
        (fields, filters, other, aliases, groupby, sortOrder, Some(limitExpr))
      case other =>
        (None, Nil, other, Map.empty, None, None, None)
    }
  }

  def findAggreagateExpression(expr: Expression): Seq[AggregateExpression] = {
    val exprList = expr match {
      case d: AggregateExpression => d :: Nil
      case Alias(ref, name) => findAggreagateExpression(ref)
      case other =>
        var listout: Seq[AggregateExpression] = Nil

        other.children.foreach(v => {
          val list = findAggreagateExpression(v)
          listout = listout ++ list
        })
        listout
    }
    exprList
  }

  def collectProjectsAndFilters1(plan: LogicalPlan):
  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression],
    Option[Seq[Expression]], Option[Seq[SortOrder]], Option[Expression]) = {
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases, groupby, sortOrder, limit) = collectProjectsAndFilters(
          child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(
          substitutedFields), groupby, sortOrder, limit)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, limit) =
          collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(
          substitutedCondition), other, aliases, groupby, sortOrder, limit)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases, _, sortOrder, limit) = collectProjectsAndFilters(
          child)
        val aggExps = aggregateExpressions.map {
          case Alias(ref, name) => ref
          case others => others
        }.filter {
          case d: AggregateExpression => true
          case _ => false
        }
        (fields, filters, other, aliases ++ collectAliases(aggregateExpressions), Some(
          aggExps), sortOrder, limit)
      case Sort(order, _, child) =>
        val (fields, filters, other, aliases, groupby, _, limit) = collectProjectsAndFilters(child)
        val substitutedOrder = order.map(s => SortOrder(substitute(aliases)(s.child), s.direction))
        (fields, filters, other, aliases, groupby, Some(substitutedOrder), limit)
      case Limit(limitExpr, child) =>
        val (fields, filters, other, aliases, groupby, sortOrder, _) = collectProjectsAndFilters(
          child)
        (fields, filters, other, aliases, groupby, sortOrder, Some(limitExpr))
      case other =>
        (None, Nil, other, Map.empty, None, None, None)
    }
  }

  private def collectAliases(fields: Seq[Expression]) = {
    fields.collect {
      case a@Alias(child, _) => a.toAttribute -> child
    }.toMap
  }

  private def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = {
    expr.transform {
      case a@Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}

case class PositionLiteral(expr: Expression, intermediateDataType: DataType)
  extends LeafExpression with CodegenFallback {
  override def dataType: DataType = expr.dataType

  override def nullable: Boolean = false

  type EvaluatedType = Any
  var position = -1

  def setPosition(pos: Int): Unit = position = pos

  override def toString: String = s"PositionLiteral($position : $expr)"

  override def eval(input: InternalRow): Any = {
    if (position != -1) {
      input.get(position, intermediateDataType)
    } else {
      expr.eval(input)
    }
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
object CarbonAggregation {
  type ReturnType = (Seq[Expression], Seq[NamedExpression], LogicalPlan)

  private def convertAggregatesForPushdown(convertUnknown: Boolean,
      rewrittenAggregateExpressions: Seq[Expression],
      oneAttr: AttributeReference) = {
    if (canBeConvertedToCarbonAggregate(rewrittenAggregateExpressions)) {
      var counter: Int = 0
      var updatedExpressions = MutableList[Expression]()
      rewrittenAggregateExpressions.foreach(v => {
        val updated = convertAggregate(v, counter, convertUnknown, oneAttr)
        updatedExpressions += updated
        counter = counter + 1
      })
      updatedExpressions
    } else {
      rewrittenAggregateExpressions
    }
  }

  def makePositionLiteral(expr: Expression, index: Int, dataType: DataType): PositionLiteral = {
    val posLiteral = PositionLiteral(expr, dataType)
    posLiteral.setPosition(index)
    posLiteral
  }

  def convertAggregate(current: Expression,
      index: Int,
      convertUnknown: Boolean,
      oneAttr: AttributeReference): Expression = {
    if (!convertUnknown && canBeConverted(current)) {
      current.transform {
        case Average(attr: AttributeReference) =>
          val convertedDataType = transformArrayType(attr)
          CarbonAverage(makePositionLiteral(convertedDataType, index, convertedDataType.dataType))
        case Average(Cast(attr: AttributeReference, dataType)) =>
          val convertedDataType = transformArrayType(attr)
          CarbonAverage(
              makePositionLiteral(convertedDataType, index, convertedDataType.dataType))
        case Count(Seq(s: Literal)) =>
          CarbonCount(s, Some(makePositionLiteral(transformLongType(oneAttr), index, LongType)))
        case Count(Seq(attr: AttributeReference)) =>
          CarbonCount(makePositionLiteral(transformLongType(attr), index, LongType))
        case Sum(attr: AttributeReference) =>
          Sum(makePositionLiteral(attr, index, attr.dataType))
        case Sum(Cast(attr: AttributeReference, dataType)) =>
          Sum(Cast(makePositionLiteral(attr, index, attr.dataType), dataType))
        case Min(attr: AttributeReference) => Min(makePositionLiteral(attr, index, attr.dataType))
        case Min(Cast(attr: AttributeReference, dataType)) =>
          Min(Cast(makePositionLiteral(attr, index, attr.dataType), dataType))
        case Max(attr: AttributeReference) =>
          Max(makePositionLiteral(attr, index, attr.dataType))
        case Max(Cast(attr: AttributeReference, dataType)) =>
          Max(Cast(makePositionLiteral(attr, index, attr.dataType), dataType))
      }
    } else {
      current
    }
  }

  def canBeConverted(current: Expression): Boolean = current match {
    case Alias(AggregateExpression(Average(attr: AttributeReference), _, false), _) => true
    case Alias(AggregateExpression(Average(Cast(attr: AttributeReference, _)), _, false), _) => true
    case Alias(AggregateExpression(Count(Seq(s: Literal)), _, false), _) => true
    case Alias(AggregateExpression(Count(Seq(attr: AttributeReference)), _, false), _) => true
    case Alias(AggregateExpression(Sum(attr: AttributeReference), _, false), _) => true
    case Alias(AggregateExpression(Sum(Cast(attr: AttributeReference, _)), _, false), _) => true
    case Alias(AggregateExpression(Min(attr: AttributeReference), _, false), _) => true
    case Alias(AggregateExpression(Min(Cast(attr: AttributeReference, _)), _, false), _) => true
    case Alias(AggregateExpression(Max(attr: AttributeReference), _, false), _) => true
    case Alias(AggregateExpression(Max(Cast(attr: AttributeReference, _)), _, false), _) => true
    case _ => false
  }

  def transformArrayType(attr: AttributeReference): AttributeReference = {
    AttributeReference(attr.name, ArrayType(DoubleType), attr.nullable, attr.metadata)(attr.exprId,
      attr.qualifiers)
  }

  def transformLongType(attr: AttributeReference): AttributeReference = {
    AttributeReference(attr.name, LongType, attr.nullable, attr.metadata)(attr.exprId,
      attr.qualifiers)
  }

  /**
   * There should be sync between carbonOperators validation and here. we should not convert to
   * carbon aggregates if the validation does not satisfy.
   */
  def canBeConvertedToCarbonAggregate(expressions: Seq[Expression]): Boolean = {
    val detailQuery = expressions.map {
      case attr@AttributeReference(_, _, _, _) => true
      case Alias(agg: AggregateExpression, name) => true
      case _ => false
    }.exists(!_)
    !detailQuery
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = unapply((plan, false))

  def unapply(combinedPlan: (LogicalPlan, Boolean)): Option[ReturnType] = {
    val oneAttr = getOneAttribute(combinedPlan._1)
    combinedPlan._1 match {
      case Aggregate(groupingExpressions, aggregateExpressionsOrig, child) =>

        // if detailed query dont convert aggregate expressions to Carbon Aggregate expressions
        val aggregateExpressions =
          if (combinedPlan._2) {
            aggregateExpressionsOrig
          }
          else {
            convertAggregatesForPushdown(false, aggregateExpressionsOrig, oneAttr)
          }
        Some((groupingExpressions, aggregateExpressions.asInstanceOf[Seq[NamedExpression]], child))
      case _ => None
    }
  }

  def getOneAttribute(plan: LogicalPlan): AttributeReference = {
    var relation: LogicalRelation = null
    plan collect {
      case l: LogicalRelation => relation = l
    }
    if (relation != null) {
      relation.output.find { p =>
        p.dataType match {
          case n: NumericType => true
          case _ => false
        }
      }.getOrElse(relation.output.head)
    } else {
      null
    }
  }
}


