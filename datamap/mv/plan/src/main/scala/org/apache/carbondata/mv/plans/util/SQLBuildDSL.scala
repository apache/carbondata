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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, BitwiseAnd, Cast, Expression, Grouping, GroupingID, Literal, NamedExpression, ShiftRight}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types.{ByteType, IntegerType}

import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.modular.ModularPlan

trait SQLBuildDSL {

  abstract class Fragment extends Product {
    // this class corresponding to TreeNode of SparkSQL or Tree of Scala compiler
    def canEqual(that: Any): Boolean = {
      throw new UnsupportedOperationException
    }

    def productArity: Int = throw new UnsupportedOperationException

    def productElement(n: Int): Any = throw new UnsupportedOperationException

    // TODO: add library needed for SQL building
    def isUnsupported: Boolean

    def Supported: Boolean

    // def pos: Position
    def alias: Option[String]
  }

  case class SPJGFragment(
      select: Seq[NamedExpression] = Nil, // TODO: more general, change to Seq[Either[Fragment,
      // NamedExpression]]
      from: Seq[(Fragment, Option[JoinType], Seq[Expression])] = Nil,
      where: Seq[Expression] = Nil,
      groupby: (Seq[Expression], Seq[Seq[Expression]]) = (Nil, Nil),
      having: Seq[Expression] = Nil,
      alias: Option[String] = None,
      modifiers: (FlagSet, Seq[Seq[Any]])) extends Fragment {
    override def isUnsupported: Boolean = {
      select == Nil || from == Nil ||
      from.map(_._1.isUnsupported).foldLeft(false)(_ || _)
    }

    override def Supported: Boolean = !isUnsupported
  }

  //  TODO: find a scheme to break up fragmentExtract() using unapply
  //
  //  object SPJGFragment {
  //    type ReturnType = (Seq[NamedExpression],Seq[(Fragment, Option[JoinType],Seq[Expression])
  // ],Seq[Expression],Seq[Expression],Seq[Expression],Option[String])
  //
  //    def unapply(plan: ModularPlan): Option[ReturnType] = fragmentExtract(plan, None)
  //  }

  case class TABLEFragment(
      table: Seq[String] = Nil,
      alias: Option[String] = None,
      modifiers: (FlagSet, Seq[Seq[Any]])) extends Fragment {
    override def isUnsupported: Boolean = table == Nil

    override def Supported: Boolean = !isUnsupported
  }

  case class UNIONFragment(
      union: Seq[Fragment] = Nil,
      alias: Option[String] = None,
      modifiers: (FlagSet, Seq[Seq[Any]])) extends Fragment {
    override def isUnsupported: Boolean = {
      union == Nil ||
      union.map(_.isUnsupported).foldLeft(false)(_ || _)
    }

    override def Supported: Boolean = !isUnsupported
  }

  val UnsupportedFragment: Fragment = new Fragment {
    def isUnsupported = true

    def Supported = false

    def alias = None
  }

  /**
   * Turns a bunch of string segments into a single string and separate each segment by a space.
   * The segments are trimmed so only a single space appears in the separation.
   * For example, `build("a", " b ", " c")` becomes "a b c".
   */
  protected def build(segments: String*): String = {
    segments.map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

  def fragmentExtract(plan: ModularPlan, alias: Option[String]): Fragment = {
    if (plan.rewritten) {
      fragmentExtract_Rewritten(plan, alias)
    } else {
      fragmentExtract_NonRewritten(plan, alias)
    }
  }

  // Rewritten portion of query plan
  private def fragmentExtract_Rewritten(
      plan: ModularPlan,
      alias: Option[String]): Fragment = {
    plan match {
      case s1@modular.Select(_, _, _, _, _,
        Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
        _, _, _) if (!s1.skip && !g.skip && !s2.skip) =>
        extractRewrittenOrNonRewrittenSelectGroupBySelect(s1, g, s2, alias)

      case s1@modular.Select(_, _, _, _, _,
        Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
        _, _, _) if (s1.skip && g.skip && s2.skip) =>
        extractRewrittenOrNonRewrittenSelectGroupBySelect(s1, g, s2, alias)

      case s1@modular.Select(_, _, _, _, _,
        Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
        _, _, _) if (!s1.skip && !g.skip && s2.skip) =>
        extractRewrittenSelectGroupBy(s1, g, alias)

      case s1@modular.Select(_, _, _, _, _, Seq(s2@modular.Select(_, _, _, _, _, _, _, _, _)),
        _, _, _) if (!s1.skip && s2.skip) =>
        extractRewrittenSelect(s1, alias)

      case other => extractSimpleOperator(other, alias)
    }
  }

  // Non-rewritten portion of query plan
  private def fragmentExtract_NonRewritten(
      plan: ModularPlan,
      alias: Option[String]): Fragment = {
    plan match {
      case s1@modular.Select(_, _, _, _, _,
        Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
        _, _, _) if (s1.aliasMap.isEmpty && !g.rewritten) =>
        extractRewrittenOrNonRewrittenSelectGroupBySelect(s1, g, s2, alias)

      case g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)
        if (g.alias.isEmpty && !s2.rewritten) =>
        val fragmentList = s2.children.zipWithIndex
          .map { case (child, index) => fragmentExtract(child, s2.aliasMap.get(index)) }
        val fList = s2.joinEdges.map {
          e => {
            (e.right, (fragmentList(e.right), Some(e.joinType), s2
              .extractRightEvaluableConditions(s2.children(e.left), s2.children(e.right))))
          }
        }.toMap
        val from = (0 to fragmentList.length - 1)
          .map(index => fList.get(index).getOrElse((fragmentList(index), None, Nil)))
        val excludesPredicate = from.flatMap(_._3).toSet
        val (select, (groupByExprs, groupingSet)) = addGroupingSetIfNeeded(g, s2)
        SPJGFragment(
          select,
          from,
          s2.predicateList.filter { p => !excludesPredicate(p) },
          (groupByExprs, groupingSet),
          Nil,
          alias,
          (g.flags, g.flagSpec))

      case g@modular.GroupBy(_, _, _, _, _, _, _) if (g.alias.nonEmpty) =>
        val from = Seq((fragmentExtract(g.child, g.alias), None, Nil))
        SPJGFragment(
          g.outputList,
          from,
          Nil,
          (g.predicateList, Seq.empty),
          Nil,
          alias,
          (g.flags, g.flagSpec))

      case other => extractSimpleOperator(other, alias)
    }
  }

  // used in both rewritten and non-rewritten cases
  // currently in rewritten cases we don't consider grouping set
  private def extractRewrittenOrNonRewrittenSelectGroupBySelect(
      s1: modular.Select,
      g: modular.GroupBy,
      s2: modular.Select,
      alias: Option[String]): Fragment = {
    val fragmentList = s2.children.zipWithIndex
      .map { case (child, index) => fragmentExtract(child, s2.aliasMap.get(index)) }
    val fList = s2.joinEdges.map {
      e => {
        (e.right, (fragmentList(e.right), Some(e.joinType), s2
          .extractRightEvaluableConditions(s2.children(e.left), s2.children(e.right))))
      }
    }.toMap
    val from = (0 to fragmentList.length - 1)
      .map(index => fList.get(index).getOrElse((fragmentList(index), None, Nil)))
    val excludesPredicate = from.flatMap(_._3).toSet
    val aliasMap = AttributeMap(g.outputList.collect { case a: Alias => (a.toAttribute, a) })
    val windowExprs = s1.windowSpec
      .map { case Seq(expr) => expr.asInstanceOf[Seq[NamedExpression]] }
      .foldLeft(Seq.empty.asInstanceOf[Seq[NamedExpression]])(_ ++ _)
    val having = s1.predicateList
      .map { case attr: Attribute => aliasMap.get(attr).map(_.child)
        .getOrElse(attr);
      case expr: Expression => expr.transform { case a: Alias => a.child };
      case other => other
      }

    val (select_g, (groupByExprs, groupingSet)) = addGroupingSetIfNeeded(g, s2)

    val gSet = AttributeSet(g.outputList.map(_.toAttribute))
    val sSet = AttributeSet(s1.outputList.map(_.toAttribute))
    val select = if (groupingSet.nonEmpty) {
      if (gSet.equals(sSet) && windowExprs.isEmpty) {
        select_g
      } else {
        throw new UnsupportedOperationException
      }
    } else
    // TODO:  how to handle alias of attribute in MV
    {
      s1.outputList.map { attr => aliasMap.get(attr.toAttribute).getOrElse(attr) } ++ windowExprs
    }

    SPJGFragment(
      select, // select
      from, // from
      s2.predicateList.filter { p => !excludesPredicate(p) }, // where
      (groupByExprs, groupingSet), // group by
      having, // having
      alias,
      (s1.flags, s1.flagSpec))
  }

  // used in rewritten cases only -- don't consider grouping set
  private def extractRewrittenSelectGroupBy(
      s1: modular.Select,
      g: modular.GroupBy,
      alias: Option[String]): Fragment = {
    val fragment = fragmentExtract(g.child, g.alias)
    val from = Seq((fragment, None, Nil))
    val aliasMap = AttributeMap(g.outputList.collect { case a: Alias => (a.toAttribute, a) })
    val windowExprs = s1.windowSpec
      .map { case Seq(expr) => expr.asInstanceOf[Seq[NamedExpression]] }
      .foldLeft(Seq.empty.asInstanceOf[Seq[NamedExpression]])(_ ++ _)
    val having = s1.predicateList
      .map { case attr: Attribute => aliasMap.get(attr).map(_.child)
        .getOrElse(attr);
      case expr: Expression => expr.transform { case a: Alias => a.child };
      case other => other
      }

    val (select_g, (groupByExprs, groupingSet)) = (g.outputList, (g.predicateList, Seq.empty))

    // TODO:  how to handle alias of attribute in MV
    val select = s1.outputList.map { attr => aliasMap.get(attr.toAttribute).getOrElse(attr) } ++
                 windowExprs
    SPJGFragment(
      select, // select
      from, // from
      Nil, // where
      (groupByExprs, groupingSet), // group by
      having, // having
      alias,
      (s1.flags, s1.flagSpec))
  }

  private def extractRewrittenSelect(s1: modular.Select, alias: Option[String]): Fragment = {
    val fragment = fragmentExtract(s1.children(0), s1.aliasMap.get(0))
    val from = Seq((fragment, None, Nil))
    val windowExprs = s1.windowSpec
      .map { case Seq(expr) => expr.asInstanceOf[Seq[NamedExpression]] }
      .foldLeft(Seq.empty.asInstanceOf[Seq[NamedExpression]])(_ ++ _)
    val select = s1.outputList.map(_.toAttribute)

    SPJGFragment(
      select, // select
      from, // from
      s1.predicateList, // where
      (Nil, Nil), // group by
      Nil, // having
      alias,
      (s1.flags, s1.flagSpec))
  }

  private def extractSimpleOperator(
      operator: ModularPlan,
      alias: Option[String]): Fragment = {
    operator match {
      case s@modular.Select(_, _, _, _, _, _, _, _, _) =>
        val fragmentList = s.children.zipWithIndex
          .map { case (child, index) => fragmentExtract(child, s.aliasMap.get(index)) }
        val fList = s.joinEdges.map {
          e => {
            (e.right, (fragmentList(e.right), Some(e.joinType), s
              .extractRightEvaluableConditions(s.children(e.left), s.children(e.right))))
          }
        }.toMap
        val from = (0 to fragmentList.length - 1)
          .map(index => fList.get(index).getOrElse((fragmentList(index), None, Nil)))
        val excludesPredicate = from.flatMap(_._3).toSet
        val windowExprs = s.windowSpec
          .map { case Seq(expr) => expr.asInstanceOf[Seq[NamedExpression]] }
          .foldLeft(Seq.empty.asInstanceOf[Seq[NamedExpression]])(_ ++ _)
        val select = s.outputList ++ windowExprs

        SPJGFragment(
          select, // select
          from, // from
          s.predicateList.filter { p => !excludesPredicate(p) }, // where
          (Nil, Nil), // group by
          Nil, // having
          alias,
          (s.flags, s.flagSpec))

      case u@modular.Union(_, _, _) =>
        UNIONFragment(
          u.children.zipWithIndex.map { case (child, index) => fragmentExtract(child, None) },
          alias,
          (u.flags, u.flagSpec))

      case d@modular.ModularRelation(_, _, _, _, _) =>
        if (d.databaseName != null && d.tableName != null) {
          TABLEFragment(
            Seq(d.databaseName, d.tableName), alias, (d.flags, d.rest))
        } else {
          TABLEFragment(Seq((d.output).toString()), alias, (d.flags, d.rest))
        }

      case h@modular.HarmonizedRelation(_) =>
        fragmentExtract(h.source, alias)

      case _ => UnsupportedFragment
    }
  }

  private def addGroupingSetIfNeeded(g: modular.GroupBy, s: modular.Select) = {
    if (g.flags.hasFlag(EXPAND)) {
      assert(g.predicateList.length > 1)
      val flagsNeedExprs =
        for {flag <- pickledListOrder if (g.flags.hasFlag(flag))} yield {
          flag
        }
      flagsNeedExprs.zip(g.flagSpec).collect {
        case (EXPAND, Seq(projections_, output_, numOriginalOutput_)) =>
          val output = output_.asInstanceOf[Seq[Attribute]]
          val projections = projections_.asInstanceOf[Seq[Seq[Expression]]]
          val numOriginalOutput = numOriginalOutput_.asInstanceOf[Int]

          // The last column of Expand is always grouping ID
          val gid = output.last

          val groupByAttributes = g.predicateList.dropRight(1).map(_.asInstanceOf[Attribute])
          // Assumption: project's projectList is composed of
          // 1) the original output (Project's child.output),
          // 2) the aliased group by expressions.
          val expandedAttributes = s.output.drop(numOriginalOutput)
          val groupByExprs = s.outputList.drop(numOriginalOutput).map(_.asInstanceOf[Alias].child)

          // a map from group by attributes to the original group by expressions.
          val groupByAttrMap = AttributeMap(groupByAttributes.zip(groupByExprs))
          // a map from expanded attributes to the original group by expressions.
          val expandedAttrMap = AttributeMap(expandedAttributes.zip(groupByExprs))

          val groupingSet: Seq[Seq[Expression]] = projections.map { project =>
            // Assumption: expand.projections is composed of
            // 1) the original output (Project's child.output),
            // 2) expanded attributes(or null literal)
            // 3) gid, which is always the last one in each project in Expand
            project.drop(numOriginalOutput).dropRight(1).collect {
              case attr: Attribute if expandedAttrMap.contains(attr) => expandedAttrMap(attr)
            }
          }

          val aggExprs = g.outputList.map { case aggExpr =>
            val originalAggExpr = aggExpr.transformDown {
              // grouping_id() is converted to VirtualColumn.groupingIdName by Analyzer. Revert
              // it back.
              case ar: AttributeReference if ar == gid => GroupingID(Nil)
              case ar: AttributeReference if groupByAttrMap.contains(ar) => groupByAttrMap(ar)
              case a@Cast(
              BitwiseAnd(
              ShiftRight(ar: AttributeReference, Literal(value: Any, IntegerType)),
              Literal(1, IntegerType)), ByteType, None) if ar == gid =>
                // for converting an expression to its original SQL format grouping(col)
                val idx = groupByExprs.length - 1 - value.asInstanceOf[Int]
                groupByExprs.lift(idx).map(Grouping).getOrElse(a)
            }

            originalAggExpr match {
              // Ancestor operators may reference the output of this grouping set,
              // and we use exprId to generate a unique name for each attribute, so we should
              // make sure the transformed aggregate expression won't change the output,
              // i.e. exprId and alias name should remain the same.
              case ne: NamedExpression if ne.exprId == aggExpr.exprId => ne
              case e => Alias(e, aggExpr.name)(exprId = aggExpr.exprId)
            }
          }
          (aggExprs, (groupByExprs, groupingSet))

      }.head
    } else {
      (g.outputList, (g.predicateList, Seq.empty))
    }
  }
}

object SQLBuildDSL extends SQLBuildDSL
