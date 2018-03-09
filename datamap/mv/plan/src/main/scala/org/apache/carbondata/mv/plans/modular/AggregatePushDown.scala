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

import scala.collection._

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Divide, ExprId, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._

trait AggregatePushDown { // self: ModularPlan =>

  def findPushThroughAggregates(outputList: Seq[NamedExpression],
      selAliasMap: AttributeMap[Attribute],
      fact: ModularRelation): Map[Int, (NamedExpression, Seq[NamedExpression])] = {
    var pushable = true
    val map = scala.collection.mutable.Map[Int, (NamedExpression, Seq[NamedExpression])]()
    outputList.zipWithIndex.foreach {
      // TODO: find out if the first two case as follows really needed.  Comment out for now.
      //      case (attr: Attribute, i) if (fact.outputSet.contains(attr)) => pushable = false
      //      case (alias: Alias, i) if (alias.child.isInstanceOf[Attribute] && fact.outputSet
      // .contains(alias.child.asInstanceOf[Attribute])) => pushable = false
      case (alias: Alias, i) if (alias.child.isInstanceOf[AggregateExpression]) =>
        val res = transformAggregate(
          alias.child
            .asInstanceOf[AggregateExpression],
          selAliasMap,
          i,
          fact,
          map,
          Some((alias.name, alias.exprId)))
        if (res.isEmpty) {
          pushable = false
        }
      case (agg: AggregateExpression, i) =>
        val res = transformAggregate(
          agg,
          selAliasMap,
          i,
          fact,
          map,
          None)
        if (res.isEmpty) {
          pushable = false
        }
      case _ =>
    }
    if (!pushable) {
      Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
    } else {
      map
    }
  }

  private def transformAggregate(aggregate: AggregateExpression,
      selAliasMap: AttributeMap[Attribute],
      ith: Int,
      fact: ModularRelation,
      map: scala.collection.mutable.Map[Int, (NamedExpression, Seq[NamedExpression])],
      aliasInfo: Option[(String, ExprId)]) = {
    aggregate match {
      case cnt@AggregateExpression(Count(exprs), _, false, _) if (exprs.length == 1 && exprs(0)
        .isInstanceOf[Attribute]) =>
        val tAttr = selAliasMap.get(exprs(0).asInstanceOf[Attribute]).getOrElse(exprs(0))
          .asInstanceOf[Attribute]
        if (fact.outputSet.contains(tAttr)) {
          val cnt1 = AggregateExpression(Count(tAttr), cnt.mode, false)
          val alias = Alias(cnt1, cnt1.toString)()
          val tSum = AggregateExpression(Sum(alias.toAttribute), cnt.mode, false, cnt.resultId)
          val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
          map += (ith -> (Alias(tSum, name)(exprId = id), Seq(alias)))
        } else {
          Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
        }
      case cnt@AggregateExpression(Count(exprs), _, false, _) if (exprs.length == 1 && exprs(0)
        .isInstanceOf[Literal]) =>
        val cnt1 = AggregateExpression(Count(exprs(0)), cnt.mode, false)
        val alias = Alias(cnt1, cnt1.toString)()
        val tSum = AggregateExpression(Sum(alias.toAttribute), cnt.mode, false, cnt.resultId)
        val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
        map += (ith -> (Alias(tSum, name)(exprId = id), Seq(alias)))
      case sum@AggregateExpression(Sum(expr), _, false, _) if (expr.isInstanceOf[Attribute]) =>
        val tAttr = selAliasMap.get(expr.asInstanceOf[Attribute]).getOrElse(expr)
          .asInstanceOf[Attribute]
        if (fact.outputSet.contains(tAttr)) {
          val sum1 = AggregateExpression(Sum(tAttr), sum.mode, false)
          val alias = Alias(sum1, sum1.toString)()
          val tSum = AggregateExpression(Sum(alias.toAttribute), sum.mode, false, sum.resultId)
          val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
          map += (ith -> (Alias(tSum, name)(exprId = id), Seq(alias)))
        } else {
          Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
        }
      case sum@AggregateExpression(Sum(expr), _, false, _) if (expr.isInstanceOf[Literal]) =>
        val sum1 = AggregateExpression(Sum(expr), sum.mode, false)
        val alias = Alias(sum1, sum1.toString)()
        val tSum = AggregateExpression(Sum(alias.toAttribute), sum.mode, false, sum.resultId)
        val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
        map += (ith -> (Alias(tSum, name)(exprId = id), Seq(alias)))
      case max@AggregateExpression(Max(expr), _, false, _) if (expr.isInstanceOf[Attribute]) =>
        val tAttr = selAliasMap.get(expr.asInstanceOf[Attribute]).getOrElse(expr)
          .asInstanceOf[Attribute]
        if (fact.outputSet.contains(tAttr)) {
          val max1 = AggregateExpression(Sum(tAttr), max.mode, false)
          val alias = Alias(max1, max1.toString)()
          val tMax = AggregateExpression(Max(alias.toAttribute), max.mode, false, max.resultId)
          val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
          map += (ith -> (Alias(tMax, name)(exprId = id), Seq(alias)))
        } else {
          Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
        }
      case min@AggregateExpression(Min(expr), _, false, _) if (expr.isInstanceOf[Attribute]) =>
        val tAttr = selAliasMap.get(expr.asInstanceOf[Attribute]).getOrElse(expr)
          .asInstanceOf[Attribute]
        if (fact.outputSet.contains(tAttr)) {
          val min1 = AggregateExpression(Min(tAttr), min.mode, false)
          val alias = Alias(min1, min1.toString)()
          val tMin = AggregateExpression(Max(alias.toAttribute), min.mode, false, min.resultId)
          val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
          map += (ith -> (Alias(tMin, name)(exprId = id), Seq(alias)))
        } else {
          Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
        }
      case avg@AggregateExpression(Average(expr), _, false, _) if (expr
        .isInstanceOf[Attribute]) =>
        val tAttr = selAliasMap.get(expr.asInstanceOf[Attribute]).getOrElse(expr)
          .asInstanceOf[Attribute]
        if (fact.outputSet.contains(tAttr)) {
          val savg = AggregateExpression(Sum(tAttr), avg.mode, false)
          val cavg = AggregateExpression(Count(tAttr), avg.mode, false)
          val sAvg = Alias(savg, savg.toString)()
          val cAvg = Alias(cavg, cavg.toString)()
          val tAvg = Divide(sAvg.toAttribute, cAvg.toAttribute)
          val (name, id) = aliasInfo.getOrElse(("", NamedExpression.newExprId))
          map += (ith -> (Alias(tAvg, name)(exprId = id), Seq(sAvg, cAvg)))
        } else {
          Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
        }
      case _ => Map.empty[Int, (NamedExpression, Seq[NamedExpression])]
    }
  }
}
