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

package org.apache.carbondata.mv.plans.util

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.modular.JoinEdge



/**
 * SelectModule is extracted from logical plan of SPJG query.  All join conditions
 * filter, and project operators in a single Aggregate-less subtree of logical plan
 * are collected.
 *
 * The returned values for this match are as follows:
 *  - Conditions for equi-join
 *  - Conditions for filter
 *  - Project list for project
 *
 */
object ExtractSelectModule extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[Expression], Map[Int, String],
    Seq[JoinEdge], Seq[LogicalPlan], FlagSet, Seq[Seq[Any]], Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (outputs, inputs, predicates, joinedges, children, isSelect, _, flags, fspecs, wspecs) =
      collectProjectsFiltersJoinsAndSort(plan)
    if (!isSelect) {
      None
    } else {
      Some(
        outputs,
        inputs,
        predicates,
        collectChildAliasMappings(
          AttributeSet(outputs).toSeq ++ AttributeSet(predicates).toSeq,
          children),
        joinedges,
        children,
        flags,
        fspecs,
        wspecs)
    }
  }

  def collectProjectsFiltersJoinsAndSort(plan: LogicalPlan): (Seq[NamedExpression],
    Seq[Expression], Seq[Expression], Seq[JoinEdge], Seq[LogicalPlan], Boolean, Map[Attribute,
    Expression], FlagSet, Seq[Seq[Any]], Seq[Seq[Any]]) = {
    plan match {
      case Project(fields, child) =>
        val (_, inputs, predicates, joinedges, children, _, aliases, flags, fspecs, wspecs) =
          collectProjectsFiltersJoinsAndSort(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (substitutedFields, inputs, predicates, joinedges, children, true, collectAliases(
          substitutedFields), flags, fspecs, wspecs)

      case Filter(condition, child) =>
        val (outputs, inputs, predicates, joinedges, children, _, aliases, flags, fspecs, wspecs)
        = collectProjectsFiltersJoinsAndSort(child)
        val substitutedCondition = substitute(aliases)(condition)
        (outputs, inputs, predicates.flatMap(splitConjunctivePredicates) ++
                          splitConjunctivePredicates(substitutedCondition), joinedges, children,
          true, aliases, flags, fspecs, wspecs)

      case Sort(order, global, child) =>
        val (outputs, inputs, predicates, joinedges, children, _, aliases, flags, fspecs, wspecs)
        = collectProjectsFiltersJoinsAndSort(child)
        val substitutedOrder = order.map(substitute(aliases))
        (outputs, inputs, predicates, joinedges, children, true, aliases, if (global) {
          flags.setFlag(SORT).setFlag(GLOBAL)
        } else {
          flags.setFlag(SORT)
        }, Seq(Seq(order)) ++ fspecs, wspecs)

      case Join(left, right, joinType, condition) =>
        val (loutputs, linputs, lpredicates, ljoinedges, lchildren, _, laliases, lflags, lfspecs,
        lwspecs) = collectProjectsFiltersJoinsAndSort(left)
        val (routputs, rinputs, rpredicates, rjoinedges, rchildren, _, raliases, rflags, rfspecs,
        rwspecs) = collectProjectsFiltersJoinsAndSort(right)
        val (lcondition, rcondition, ccondition) = split(condition, lchildren, rchildren)
        val joinEdge = collectJoinEdge(ccondition, lchildren, rchildren, joinType)
        val adjustedJoinEdges = rjoinedges
          .map(e => JoinEdge(e.left + lchildren.size, e.right + lchildren.size, e.joinType))
        val output: Seq[Attribute] = {
          joinType match {
            case LeftSemi =>
              left.output
            case LeftOuter =>
              left.output ++ right.output.map(_.withNullability(true))
            case RightOuter =>
              left.output.map(_.withNullability(true)) ++ right.output
            case FullOuter =>
              left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
            case LeftAnti =>
              left.output
            case _ =>
              left.output ++ right.output
          }
        }
        if (lfspecs.isEmpty && rfspecs.isEmpty && lflags == NoFlags && rflags == NoFlags &&
            lwspecs.isEmpty && rwspecs.isEmpty) {
          (output, (linputs ++ rinputs), lpredicates.flatMap(splitConjunctivePredicates) ++
                                         rpredicates.flatMap(splitConjunctivePredicates) ++
                                         lcondition ++ rcondition ++ ccondition, ljoinedges ++
                                                                                 joinEdge ++
                                                                                 adjustedJoinEdges,
            lchildren ++ rchildren, true, laliases ++ raliases, NoFlags, Seq.empty, Seq.empty)
        } else {
          throw new UnsupportedOperationException(
            s"unsupported join: \n left child ${ left } " +
            s"\n right child ${ right }")
        }

      // when select * is executed with limit, ColumnPruning rule will remove the project node from
      // the plan during optimization, so if child of Limit is relation, then make the select node
      // and make the modular plan
      case Limit(limitExpr, lr: LogicalRelation) =>
        (lr.output, lr.output, Nil, Nil, Seq(lr), true, Map.empty, NoFlags, Seq.empty, Seq
          .empty)

      case other =>
        (other.output, other.output, Nil, Nil, Seq(other), false, Map.empty, NoFlags, Seq.empty, Seq
          .empty)
    }
  }

  def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = {
    fields.collect {
      case a@Alias(child, _) => a.toAttribute -> child
    }.toMap
  }

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a@Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifier)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifier)).getOrElse(a)
    }
  }

  def collectChildAliasMappings(attributeSet: Seq[Attribute], children: Seq[LogicalPlan]
  ): Map[Int, String] = {
    val aq = attributeSet.filter(_.qualifier.nonEmpty)
    children.zipWithIndex.flatMap {
      case (child, i) =>
        aq.find(child.outputSet.contains(_))
          .flatMap(_.qualifier.headOption)
          .map((i, _))
    }.toMap
  }

  def split(condition: Option[Expression],
      lchildren: Seq[LogicalPlan],
      rchildren: Seq[LogicalPlan]): (Seq[Expression], Seq[Expression], Seq[Expression]) = {
    val left = lchildren.map(_.outputSet).foldLeft(AttributeSet(Set.empty))(_ ++ _)
    val right = rchildren.map(_.outputSet).foldLeft(AttributeSet(Set.empty))(_ ++ _)
    val conditions = condition.map(splitConjunctivePredicates).getOrElse(Nil)
    val (leftEvaluationCondition, rest) = conditions.partition(_.references subsetOf left)
    val (rightEvaluationCondition, commonCondition) = rest.partition(_.references subsetOf right)
    (leftEvaluationCondition, rightEvaluationCondition, commonCondition)
  }

  /*
   * collectJoinEdge only valid when condition are common condition of above split, left and
   * right children correspond
   * to respective two children parameters of above split
   *
   */
  def collectJoinEdge(condition: Seq[Expression],
      lchildren: Seq[LogicalPlan],
      rchildren: Seq[LogicalPlan],
      joinType: JoinType): Seq[JoinEdge] = {
    val common = condition.map(_.references).foldLeft(AttributeSet(Set.empty))(_ ++ _)
    val lIdxSeq = lchildren
      .collect { case x if x.outputSet.intersect(common).nonEmpty => lchildren.indexOf(x) }
    val rIdxSeq = rchildren
      .collect { case x if x.outputSet.intersect(common).nonEmpty => rchildren.indexOf(x) +
                                                                     lchildren.size
      }
    for (l <- lIdxSeq; r <- rIdxSeq) yield {
      JoinEdge(l, r, joinType)
    }
  }

}

object ExtractSelectModuleForWindow extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[Expression], Map[Int, String],
    Seq[JoinEdge], Seq[LogicalPlan], FlagSet, Seq[Seq[Any]], Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    collectSelectFromWindowChild(plan)
  }

  def collectSelectFromWindowChild(plan: LogicalPlan): Option[(Seq[NamedExpression],
    Seq[Expression], Seq[Expression], Map[Int, String], Seq[JoinEdge], Seq[LogicalPlan], FlagSet,
    Seq[Seq[Any]], Seq[Seq[Any]])] = {
    plan match {
      case agg@Aggregate(_, _, _) =>
        Some(
          agg.aggregateExpressions,
          agg.child.output,
          Seq.empty,
          Map.empty,
          Seq.empty,
          Seq(agg),
          NoFlags,
          Seq.empty,
          Seq.empty)
      case ExtractSelectModule(
      output,
      input,
      predicate,
      aliasmap,
      joinedge,
      children,
      flags,
      fspec,
      wspec) =>
        Some(output, input, predicate, aliasmap, joinedge, children, flags, fspec, wspec)
      case Window(exprs, _, _, child) =>
        val ret: Option[(Seq[NamedExpression], Seq[Expression], Seq[Expression], Map[Int,
          String], Seq[JoinEdge], Seq[LogicalPlan], FlagSet, Seq[Seq[Any]], Seq[Seq[Any]])] =
          collectSelectFromWindowChild(
            child)
        ret.map(r => (r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, Seq(Seq(exprs)) ++ r._9))
      case other => None
    }
  }
}

/**
 * GroupByModule is extracted from the Aggregate node of logical plan.
 * The groupingExpressions, aggregateExpressions are collected.
 *
 * The returned values for this match are as follows:
 *  - Grouping attributes for the Aggregate node.
 *  - Aggregates for the Aggregate node.
 *  - Project list for project
 *
 */

object ExtractGroupByModule extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[Expression], Option[String],
    LogicalPlan, FlagSet, Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case a@logical.Aggregate(_, _, e@Expand(_, _, p: Project)) if isGroupingSet(a, e, p) =>
        // Assumption: Aggregate's groupingExpressions is composed of
        // 1) the grouping attributes
        // 2) gid, which is always the last one
        val g = a.groupingExpressions.map(_.asInstanceOf[Attribute])
        val numOriginalOutput = e.output.size - g.size
        Some(
          a.aggregateExpressions,
          e.output,
          a.groupingExpressions,
          None,
          p,
          NoFlags.setFlag(EXPAND),
          Seq(Seq(e.projections, e.output, numOriginalOutput)))
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
        Some(
          aggregateExpressions,
          child.output,
          groupingExpressions,
          None,
          child,
          NoFlags,
          Seq.empty)
      case other => None
    }
  }

  private def isGroupingSet(a: Aggregate, e: Expand, p: Project): Boolean = {
    assert(a.child == e && e.child == p)

    if (a.groupingExpressions.forall(_.isInstanceOf[Attribute])) {
      val g = a.groupingExpressions.map(_.asInstanceOf[Attribute])
      sameOutput(
        e.output.drop(e.output.size - g.size),
        a.groupingExpressions.map(_.asInstanceOf[Attribute]))
    } else {
      false
    }
  }

  private def sameOutput(output1: Seq[Attribute], output2: Seq[Attribute]): Boolean = {
    output1.size == output2.size &&
    output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))
  }
}

object ExtractUnionModule extends PredicateHelper {
  type ReturnType = (Seq[LogicalPlan], FlagSet, Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case u: Union =>
        val children = collectUnionChildren(u)
        Some(children, NoFlags, Seq.empty)
      case _ => None
    }
  }

  private def collectUnionChildren(plan: LogicalPlan): List[LogicalPlan] = {
    plan match {
      case Union(children) => children.toList match {
        case head :: Nil => collectUnionChildren(head)
        case head :: tail => collectUnionChildren(head) ++ collectUnionChildren(Union(tail))
        case Nil => Nil
      }
      case other => other :: Nil
    }
  }
}

object ExtractTableModule extends PredicateHelper {
  type ReturnType = (String, String, Seq[NamedExpression], Seq[LogicalPlan], FlagSet, Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      // uncomment for cloudera1 version
      //      case m: CatalogRelation =>
      //        Some(m.tableMeta.database, m.tableMeta.identifier.table, m.output, Nil, NoFlags,
      //          Seq.empty)
      //       uncomment for apache version
      case m: HiveTableRelation =>
        Some(m.tableMeta.database, m.tableMeta.identifier.table, m.output, Nil, NoFlags,
          Seq.empty)
      case l: LogicalRelation =>
        val tableIdentifier = l.catalogTable.map(_.identifier)
        val database = tableIdentifier.flatMap(_.database).orNull
        val table = tableIdentifier.map(_.table).orNull
        Some(database, table, l.output, Nil, NoFlags, Seq.empty)
      case l: LocalRelation => // used for unit test
        Some(null, null, l.output, Nil, NoFlags, Seq.empty)
      case _ =>
          None
    }
  }
}
