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

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, PredicateHelper, _}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.mv.plans.{Pattern, _}
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util._

object SimpleModularizer extends ModularPatterns {
  def patterns: Seq[Pattern] = {
    SelectModule ::
    GroupByModule ::
    UnionModule ::
    DataSourceModule :: Nil
  }

  override protected def collectPlaceholders(plan: ModularPlan): Seq[(ModularPlan, LogicalPlan)] = {
    plan.collect {
      case placeholder@ModularizeLater(logicalPlan) => placeholder -> logicalPlan
    }
  }

  override protected def prunePlans(plans: Iterator[ModularPlan]): Iterator[ModularPlan] = {
    plans
    //    plans.filter(_.collect { case n if n.subqueries.nonEmpty => n }.isEmpty)
    // TODO: find out why the following stmt not working
    //    plans.filter(_.find { case n if n.subqueries.nonEmpty => true }.isEmpty)
  }

  override protected def makeupAliasMappings(
      plans: Iterator[ModularPlan]): Iterator[ModularPlan] = {
    def makeup(plan: ModularPlan): ModularPlan = {
      plan transform {
        case g@GroupBy(_, _, _, _, s@Select(_, _, _, aliasmap, _, children, _, _, _, _), _, _, _) =>
          val aq = AttributeSet(g.outputList).filter(_.qualifier.nonEmpty)
          val makeupmap: Map[Int, String] = children.zipWithIndex.flatMap {
            case (child, i) =>
              aq.find(child.outputSet.contains(_))
                .flatMap(_.qualifier.headOption)
                .map((i, _))
          }.toMap
          g.copy(child = s.copy(aliasMap = makeupmap ++ aliasmap))
      }
    }

    plans.map(makeup)
  }
}

abstract class ModularPattern extends GenericPattern[ModularPlan] {

  override protected def modularizeLater(plan: LogicalPlan): ModularPlan = ModularizeLater(plan)
}

case class ModularizeLater(plan: LogicalPlan) extends LeafNode {
  override def output: Seq[Attribute] = plan.output
}

abstract class ModularPatterns extends Modularizer[ModularPlan] {

  //  self: MQOContext#SparkyModeler =>

  object SelectModule extends Pattern with PredicateHelper {

    private[this] def makeSelectModule(
        output: Seq[NamedExpression],
        input: Seq[Expression],
        predicate: Seq[Expression],
        aliasmap: Map[Int, String],
        joinedge: Seq[JoinEdge],
        flags: FlagSet,
        children: Seq[ModularPlan],
        flagSpec: Seq[Seq[Any]],
        windowSpec: Seq[Seq[Any]]) = {
      Seq(Select(
        output,
        input,
        predicate,
        aliasmap,
        joinedge,
        children,
        flags,
        flagSpec,
        windowSpec))
    }

    def apply(plan: LogicalPlan): Seq[ModularPlan] = {
      plan match {
        case Distinct(
          ExtractSelectModule(output, input, predicate, aliasmap, joinedge, children, flags1,
          fspec1, wspec)) =>
          val flags = flags1.setFlag(DISTINCT)
          makeSelectModule(output, input, predicate, aliasmap, joinedge, flags,
            children.map(modularizeLater), fspec1, wspec)

        case Limit(
          limitExpr,
          Distinct(
            ExtractSelectModule(output, input, predicate, aliasmap, joinedge, children,
              flags1, fspec1, wspec))) =>
          val flags = flags1.setFlag(DISTINCT).setFlag(LIMIT)
          makeSelectModule(output, input, predicate, aliasmap, joinedge, flags,
            children.map(modularizeLater), Seq(Seq(limitExpr)) ++ fspec1, wspec)

        // if select * is with limit, then projection is removed from plan, so send the parent plan
        // to ExtractSelectModule to make the select node
        case limit@Limit(limitExpr, lr: LogicalRelation) =>
          val (output, input, predicate, aliasmap, joinedge, children, flags1,
          fspec1, wspec) = ExtractSelectModule.unapply(limit).get
          val flags = flags1.setFlag(LIMIT)
          makeSelectModule(output, input, predicate, aliasmap, joinedge, flags,
            children.map(modularizeLater), Seq(Seq(limitExpr)) ++ fspec1, wspec)

        case Limit(
          limitExpr,
          ExtractSelectModule(output, input, predicate, aliasmap, joinedge, children, flags1,
            fspec1, wspec)) =>
          val flags = flags1.setFlag(LIMIT)
          makeSelectModule(output, input, predicate, aliasmap, joinedge, flags,
            children.map(modularizeLater), Seq(Seq(limitExpr)) ++ fspec1, wspec)

        case ExtractSelectModule(output, input, predicate, aliasmap, joinedge, children, flags1,
        fspec1, wspec) =>
          makeSelectModule(output, input, predicate, aliasmap, joinedge, flags1,
            children.map(modularizeLater), fspec1, wspec)

        case Window(exprs, _, _,
          ExtractSelectModuleForWindow(output, input, predicate, aliasmap, joinedge, children,
            flags1, fspec1, wspec)) =>
          val sel1 = plan.asInstanceOf[Window].child match {
            case agg: Aggregate => children.map (modularizeLater)
            case other => makeSelectModule (output, input, predicate, aliasmap, joinedge, flags1,
              children.map (modularizeLater), fspec1, wspec)
          }
          makeSelectModule(
            output.map(_.toAttribute),
            output.map(_.toAttribute),
            Seq.empty,
            Map.empty,
            Seq.empty,
            NoFlags,
            sel1,
            Seq.empty,
            Seq(Seq(exprs)) ++ wspec)

        case _ => Nil
      }
    }
  }

  object GroupByModule extends Pattern with PredicateHelper {

    private[this] def makeGroupByModule(
        output: Seq[NamedExpression],
        input: Seq[Expression],
        predicate: Seq[Expression],
        flags: FlagSet,
        alias: Option[String],
        child: ModularPlan,
        fspec: Seq[Seq[Any]]) = {
      val groupby = Some(GroupBy(output, input, predicate, alias, child, flags, fspec))
      groupby.map(Seq(_)).getOrElse(Nil)
    }

    def apply(plan: LogicalPlan): Seq[ModularPlan] = {
      plan match {
        case Limit(
        limitExpr,
        ExtractGroupByModule(output, input, predicate, alias, child, flags1, fspec1)) =>
          val flags = flags1.setFlag(LIMIT)
          makeGroupByModule(
            output,
            input,
            predicate,
            flags,
            alias,
            modularizeLater(child),
            Seq(Seq(limitExpr)) ++ fspec1)
        case ExtractGroupByModule(output, input, predicate, alias, child, flags1, fspec1) =>
          makeGroupByModule(output, input, predicate, flags1, alias, modularizeLater(child), fspec1)
        case _ => Nil
      }
    }
  }

  object UnionModule extends Pattern with PredicateHelper {

    private[this] def makeUnionModule(
        flags: FlagSet,
        children: Seq[ModularPlan],
        fspec: Seq[Seq[Any]]) = {
      Seq(modular.Union(children, flags, fspec))
    }

    def apply(plan: LogicalPlan): Seq[ModularPlan] = {
      plan match {
        case Distinct(ExtractUnionModule(children, flags1, fspec1)) =>
          val flags = flags1.setFlag(DISTINCT)
          makeUnionModule(flags, children.map(modularizeLater), fspec1)
        case Limit(limitExpr, Distinct(ExtractUnionModule(children, flags1, fspec1))) =>
          val flags = flags1.setFlag(DISTINCT).setFlag(LIMIT)
          makeUnionModule(flags, children.map(modularizeLater), Seq(Seq(limitExpr)) ++ fspec1)
        case Limit(limitExpr, ExtractUnionModule(children, flags1, fspec1)) =>
          val flags = flags1.setFlag(LIMIT)
          makeUnionModule(flags, children.map(modularizeLater), Seq(Seq(limitExpr)) ++ fspec1)
        case ExtractUnionModule(children, flags1, fspec1) =>
          makeUnionModule(flags1, children.map(modularizeLater), fspec1)
        case _ => Nil
      }
    }
  }

  object DataSourceModule extends Pattern with Flags with PredicateHelper {

    private[this] def makeDataSourceModule(
        databaseName: String,
        tableName: String,
        output: Seq[NamedExpression],
        flags: FlagSet,
        fspec: Seq[Seq[Any]]) = {
      Seq(ModularRelation(databaseName, tableName, output, flags, fspec))
    }

    def apply(plan: LogicalPlan): Seq[ModularPlan] = {
      plan match {
        case ExtractTableModule(databaseName, tableName, output, Nil, flags1, fspec1) =>
          makeDataSourceModule(databaseName, tableName, output, flags1, fspec1)
        case _ => Nil
      }
    }
  }

}
