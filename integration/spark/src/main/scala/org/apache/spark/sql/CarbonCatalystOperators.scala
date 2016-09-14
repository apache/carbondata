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

import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, _}
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
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
 * Shows Loads in a table
 */
case class ShowLoadsCommand(databaseNameOp: Option[String], table: String, limit: Option[String])
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("SegmentSequenceId", StringType, nullable = false)(),
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

  override def output: Seq[AttributeReference] =
    Seq(AttributeReference("result", StringType, nullable = false)())
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

 /**
  * It checks if query is count(*) then push down to carbon
  *
  * The returned values for this match are as follows:
  * - whether its count star
  * - count star attribute
  * - child plan
  */
object CountStarPlan {
  type ReturnType = (Boolean, MutableList[Attribute], LogicalPlan)

   /**
    * It return count star query attribute.
    */
  private def getCountStarAttribute(expressions: Seq[Expression]): MutableList[Attribute] = {
    var outputColumns = scala.collection.mutable.MutableList[Attribute]()
    expressions.foreach { expr =>
      expr match {
        case par@Alias(_, _) =>
          val head = par.children.head
          if(head.isInstanceOf[Count] && head.asInstanceOf[Count].child.isInstanceOf[Literal]) {
           outputColumns += par.toAttribute
          }
      }
    }
    outputColumns
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case Aggregate(groupingExpressions, aggregateExpressionsOrig, child) if (groupingExpressions
        .isEmpty && strictCountStar(child)) =>
        val colAttr = countStarAttr(aggregateExpressionsOrig)
        if (None != colAttr) {
          Some(true, countStarAttr(aggregateExpressionsOrig).get, child)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * check if child
   */
  def strictCountStar(child: LogicalPlan): Boolean = {
    child collect {
      case cd: Filter => return false
    }
    return true
  }
  def countStarAttr(aggregateExpressionsOrig: Seq[NamedExpression]):
  Option[MutableList[Attribute]] = {
    val preCheckEval = getPartialEvaluation(aggregateExpressionsOrig)
    preCheckEval match {
      case Some(exprs) =>
        val colAttr = getCountStarAttribute(exprs)
        if (colAttr.nonEmpty) {
          Some(colAttr)
        } else {
          None
        }
      case _ => None
    }


  }

  def getPartialEvaluation(aggregateExpressions: Seq[Expression]):
  Option[(Seq[NamedExpression])] = {
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

      // Replace aggregations with a new expression that computes the result from the already
      // computed partial evaluations and grouping values.
      val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
        case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
          partialEvaluations(new TreeNodeRef(e)).finalEvaluation
      }
      ).asInstanceOf[Seq[NamedExpression]]
      Some(rewrittenAggregateExpressions)
    } else {
      None
    }

  }
}
