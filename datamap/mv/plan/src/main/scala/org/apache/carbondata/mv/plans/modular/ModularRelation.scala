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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}

import org.apache.carbondata.mv.plans.modular.Flags._

object ModularRelation {
  def apply(outputList: NamedExpression*): ModularRelation = {
    new ModularRelation(
      "test",
      "test",
      outputList,
      NoFlags,
      Seq.empty)
  }
}

case class ModularRelation(
    databaseName: String,
    tableName: String,
    outputList: Seq[NamedExpression],
    flags: FlagSet,
    rest: Seq[Seq[Any]]) extends LeafNode {
  protected override def computeStats(spark: SparkSession): Statistics = {
    val plan = spark.table(s"${ databaseName }.${ tableName }").queryExecution.optimizedPlan
    val stats = plan.stats
    SparkSQLUtil.getStatisticsObj(outputList, plan, stats)
  }

  override def output: Seq[Attribute] = outputList.map(_.toAttribute)

  override def adjacencyList: Map[Int, Seq[(Int, JoinType)]] = Map.empty

  def fineEquals(that: Any): Boolean = {
    that match {
      case that: ModularRelation =>
        if ((databaseName != null && tableName != null && databaseName == that.databaseName &&
          tableName == that.tableName && output.toString == that.output.toString) ||
          (databaseName == null && tableName == null && that.databaseName == null &&
            that.tableName == null && output.toString == that.output.toString)) {
          true
        } else {
          false
        }
      case _ => false
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: ModularRelation =>
        if ((databaseName != null && tableName != null && databaseName == that.databaseName &&
             tableName == that.tableName) ||
            (databaseName == null && tableName == null && that.databaseName == null &&
             that.tableName == null && output.toString == (that.output).toString)) {
          true
        } else {
          false
        }
      case _ => false
    }
  }
}

object HarmonizedRelation {
  def canHarmonize(source: ModularPlan): Boolean = {
    source match {
      case g@GroupBy(
      _,
      _,
      _,
      _,
      Select(_, _, _, _, _, dim :: Nil, NoFlags, Nil, Nil, _),
      NoFlags,
      Nil, _) if dim.isInstanceOf[ModularRelation] =>
        val check =
          g.outputList.forall {
            case alias: Alias =>
              alias.child.isInstanceOf[AttributeReference] ||
              alias.child.isInstanceOf[Literal] ||
              alias.child.isInstanceOf[Expression] ||
              (alias.child match {
                case AggregateExpression(First(_, _), _, _, _) => true
                case AggregateExpression(Last(_, _), _, _, _) => true
                case _ => false
              })
            case col =>
              col.isInstanceOf[AttributeReference] ||
              col.isInstanceOf[Literal] ||
              (col.asInstanceOf[Expression] match {
                case AggregateExpression(First(_, _), _, _, _) => true
                case AggregateExpression(Last(_, _), _, _, _) => true
                case _ => false
              })
        }
        if (check) {
          true
        } else {
          false
        }
      case _ => false
    }
  }
}

// support harmonization for dimension table
case class HarmonizedRelation(source: ModularPlan) extends LeafNode {
  require(HarmonizedRelation.canHarmonize(source), "invalid plan for harmonized relation")
  lazy val tableName = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation]
    .tableName
  lazy val databaseName = source.asInstanceOf[GroupBy].child.children(0)
    .asInstanceOf[ModularRelation].databaseName
  lazy val tag: Option[Attribute] = {
    source match {
      case GroupBy(head :: tail, _, _, _, _, _, _, _)
        if (head.isInstanceOf[Alias] && head.asInstanceOf[Alias].child == Literal(1)) =>
        Some(head.toAttribute)
      case _ => None
    }
  }

  //  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = source.stats
  // (spark, conf)
  protected override def computeStats(spark: SparkSession): Statistics = {
    val plan = spark.table(s"${ databaseName }.${ tableName }").queryExecution.optimizedPlan
    val stats = plan.stats
    val output = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation]
      .outputList.map(_.toAttribute)
    val aliasMap = AttributeMap(
      source.asInstanceOf[GroupBy].outputList.collect {
        case a@Alias(ar: Attribute, _) => (ar, a.toAttribute)
        case a@Alias(AggregateExpression(First(ar: Attribute, _), _, _, _), _) =>
          (ar, a.toAttribute)
        case a@Alias(AggregateExpression(Last(ar: Attribute, _), _, _, _), _) =>
          (ar, a.toAttribute)
      })
    SparkSQLUtil.getStatisticsObj(output, plan, stats, Option(aliasMap))
  }

  override def output: Seq[Attribute] = source.output

  // two harmonized modular relations are equal only if orders of output columns of
  // their source plans are exactly the same
  override def equals(that: Any): Boolean = {
    def canonicalize(source: ModularPlan): ModularPlan = {
      source.transform {
        case gb@GroupBy(head :: tail, _, _, _, _, _, _, _)
          if head.isInstanceOf[Alias] && head.asInstanceOf[Alias].child == Literal(1) =>
          gb.copy(outputList = tail)
      }
    }

    that match {
      case that: HarmonizedRelation =>
        val s1 = canonicalize(source)
        val s2 = canonicalize(that.source)
        val r = s1.sameResult(s2)
        r
      case _ => false
    }
  }

  def addTag: HarmonizedRelation = {
    val source1 = source.transform { case gb@GroupBy(head :: tail, _, _, _, _, _, _, _)
      if !head.isInstanceOf[Alias] || !(head.asInstanceOf[Alias].child == Literal(1)) =>
      val exprId = NamedExpression.newExprId
      gb.copy(outputList = Alias(Literal(1), s"gen_tag_${ exprId.id }")(exprId) +: gb.outputList)
    }
    HarmonizedRelation(source1)
  }

  def hasTag: Boolean = {
    tag match {
      case Some(_) => true
      case None => false
    }
  }

}

object SparkSQLUtil {
  def getStatisticsObj(outputList: Seq[NamedExpression],
      plan: LogicalPlan, stats: Statistics,
      aliasMap: Option[AttributeMap[Attribute]] = None)
  : Statistics = {
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq.head
    val attributes : AttributeMap[ColumnStat] = stats.attributeStats
    var attributeStats = AttributeMap(attributes.iterator
      .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
    if (aliasMap.isDefined) {
      attributeStats = AttributeMap(
        attributeStats.map(pair => (aliasMap.get(pair._1), pair._2)).toSeq)
    }
    val hints = stats.hints
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, hints)
  }
}
