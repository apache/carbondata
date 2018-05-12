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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.internal.SQLConf

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

case class ModularRelation(databaseName: String,
    tableName: String,
    outputList: Seq[NamedExpression],
    flags: FlagSet,
    rest: Seq[Seq[Any]]) extends LeafNode {
  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = {
    val plan = spark.table(s"${ databaseName }.${ tableName }").queryExecution.optimizedPlan
    val stats = plan.stats(conf)
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq(0)
    val attributeStats = AttributeMap(stats.attributeStats.iterator
      .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, stats.hints)
  }

  override def output: Seq[Attribute] = outputList.map(_.toAttribute)

  override def adjacencyList: Map[Int, Seq[(Int, JoinType)]] = Map.empty

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
      Nil, _) if (dim.isInstanceOf[ModularRelation]) =>
        if (g.outputList
          .forall(col => col.isInstanceOf[AttributeReference] ||
                         (col.isInstanceOf[Alias] &&
                          col.asInstanceOf[Alias].child.isInstanceOf[AttributeReference]))) {
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

  //  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = source.stats
  // (spark, conf)
  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = {
    val plan = spark.table(s"${ databaseName }.${ tableName }").queryExecution.optimizedPlan
    val stats = plan.stats(conf)
    val output = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation]
      .outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq(0)
    val aliasMap = AttributeMap(
      source.asInstanceOf[GroupBy].outputList.collect {
        case a: Alias if (a.child.isInstanceOf[Attribute]) => (a.child.asInstanceOf[Attribute], a
          .toAttribute)
      })
    val aStatsIterator = stats.attributeStats.iterator.map { pair => (rewrites(pair._1), pair._2) }
    val attributeStats = AttributeMap(aStatsIterator
      .map(pair => ((aliasMap.get(pair._1)).getOrElse(pair._1), pair._2)).toSeq)

    Statistics(stats.sizeInBytes, None, attributeStats, stats.hints)
  }

  override def output: Seq[Attribute] = source.output

  // two harmonized modular relations are equal only if orders of output columns of
  // their source plans are exactly the same
  override def equals(that: Any): Boolean = {
    that match {
      case that: HarmonizedRelation => source.sameResult(that.source)
      case _ => false
    }
  }
}
