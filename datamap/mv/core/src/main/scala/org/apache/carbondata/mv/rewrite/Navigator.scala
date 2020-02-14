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

package org.apache.carbondata.mv.rewrite

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, Literal, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.preagg.TimeSeriesFunctionEnum
import org.apache.carbondata.mv.expressions.modular._
import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular._
import org.apache.carbondata.mv.session.MVSession
import org.apache.carbondata.mv.timeseries.{Granularity, TimeSeriesFunction}

private[mv] class Navigator(catalog: SummaryDatasetCatalog, session: MVSession) {

  var modularPlan: java.util.Set[ModularPlan] = new java.util.HashSet[ModularPlan]()

  def rewriteWithSummaryDatasets(plan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    val replaced = plan.transformAllExpressions {
      case s: ModularSubquery =>
        if (s.children.isEmpty) {
          rewriteWithSummaryDatasetsCore(s.plan, rewrite) match {
            case Some(rewrittenPlan) => ScalarModularSubquery(rewrittenPlan, s.children, s.exprId)
            case None => s
          }
        }
        else throw new UnsupportedOperationException(s"Rewrite expression $s isn't supported")
      case o => o
    }
    rewriteWithSummaryDatasetsCore(replaced, rewrite).getOrElse(replaced)
  }

  def rewriteWithSummaryDatasetsCore(plan: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    val rewrittenPlan = plan transformDown {
      case currentFragment =>
        if (currentFragment.rewritten || !currentFragment.isSPJGH) currentFragment
        else {
          val compensation =
            (for { dataset <- catalog.lookupFeasibleSummaryDatasets(currentFragment).toStream
                   subsumer <- session.sessionState.modularizer.modularize(
                     session.sessionState.optimizer.execute(dataset.plan)).map(_.semiHarmonized)
                   subsumee <- unifySubsumee(currentFragment)
                   comp <- subsume(
                     unifySubsumer2(
                       unifySubsumer1(
                         subsumer,
                         subsumee,
                         dataset.relation),
                       subsumee),
                     subsumee, rewrite)
                 } yield comp).headOption
          compensation.map(_.setRewritten).getOrElse(currentFragment)
        }
    }
    if (rewrittenPlan.fastEquals(plan)) {
      if (modularPlan.asScala.exists(p => p.sameResult(rewrittenPlan))) {
        return None
      }
      getRolledUpModularPlan(rewrittenPlan, rewrite)
    } else {
      Some(rewrittenPlan)
    }
  }

  /**
   * Check if modular plan can be rolled up by rewriting and matching the modular plan
   * with existing mv datasets.
   * @param rewrittenPlan to check if can be rolled up
   * @param rewrite
   * @return new modular plan
   */
  def getRolledUpModularPlan(rewrittenPlan: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    var canDoRollUp = true
    // check if modular plan contains timeseries udf
    val timeSeriesUdf = rewrittenPlan match {
      case s: Select =>
        getTimeSeriesUdf(s.outputList)
      case g: GroupBy =>
        getTimeSeriesUdf(g.outputList)
      case _ => (null, null)
    }
    // set canDoRollUp to false, in case of Join queries and if filter has timeseries udf function
    // TODO: support rollUp for join queries
    rewrite.optimizedPlan.transformDown {
      case join@Join(_, _, _, _) =>
        canDoRollUp = false
        join
      case f@Filter(condition: Expression, _) =>
        condition.collect {
          case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
            canDoRollUp = false
        }
        f
    }
    if (null != timeSeriesUdf._2 && canDoRollUp) {
      // check for rollup and rewrite the plan
      // collect all the datasets which contains timeseries datamap's
      val dataSets = catalog.lookupFeasibleSummaryDatasets(rewrittenPlan)
      var granularity: java.util.List[TimeSeriesFunctionEnum] = new java.util
      .ArrayList[TimeSeriesFunctionEnum]()
      // Get all the lower granularities for the query from datasets
      dataSets.foreach { dataset =>
        if (dataset.dataMapSchema.isTimeSeries) {
          dataset.plan.transformExpressions {
            case a@Alias(udf: ScalaUDF, _) =>
              if (udf.function.isInstanceOf[TimeSeriesFunction]) {
                val gran = udf.children.last.asInstanceOf[Literal].toString().toUpperCase()
                if (Granularity.valueOf(timeSeriesUdf._2.toUpperCase).ordinal() <=
                    Granularity.valueOf(gran).ordinal()) {
                  granularity.add(TimeSeriesFunctionEnum.valueOf(gran))
                }
              }
              a
          }
        }
      }
      if (!granularity.isEmpty) {
        granularity = granularity.asScala.sortBy(_.getOrdinal)(Ordering[Int].reverse).asJava
        var orgTable: String = null
        // get query Table
        rewrittenPlan.collect {
          case m: ModularRelation =>
            orgTable = m.tableName
        }
        var queryGranularity: String = null
        var newRewrittenPlan = rewrittenPlan
        // replace granularities in the plan and check if plan can be changed
        breakable {
          granularity.asScala.foreach { func =>
            newRewrittenPlan = rewriteGranularityInPlan(rewrittenPlan, func.getName)
            modularPlan.add(newRewrittenPlan)
            val logicalPlan = session
              .sparkSession
              .sql(newRewrittenPlan.asCompactSQL)
              .queryExecution
              .optimizedPlan
            modularPlan.clear()
            var rolledUpTable: String = null
            logicalPlan.collect {
              case l: LogicalRelation =>
                rolledUpTable = l.catalogTable.get.identifier.table
            }
            if (!rolledUpTable.equalsIgnoreCase(orgTable)) {
              queryGranularity = func.getName
              break()
            }
          }
        }
        if (null != queryGranularity) {
          // rewrite the plan and set it as rolled up plan
          val modifiedPlan = rewriteWithSummaryDatasetsCore(newRewrittenPlan, rewrite)
          if (modifiedPlan.isDefined) {
            modifiedPlan.get.map(_.setRolledUp())
            modifiedPlan
          } else {
            None
          }
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  def subsume(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    if (subsumer.getClass == subsumee.getClass) {
      (subsumer.children, subsumee.children) match {
        case (Nil, Nil) => None
        case (r, e) if r.forall(_.isInstanceOf[modular.LeafNode]) &&
                       e.forall(_.isInstanceOf[modular.LeafNode]) =>
          val iter = session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
          if (iter.hasNext) Some(iter.next)
          else None

        case (rchild :: Nil, echild :: Nil) =>
          val compensation = subsume(rchild, echild, rewrite)
          val oiter = compensation.map {
            case comp if comp.eq(rchild) =>
              session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
            case _ =>
              session.sessionState.matcher.execute(subsumer, subsumee, compensation, rewrite)
          }
          oiter.flatMap { case iter if iter.hasNext => Some(iter.next)
                          case _ => None }

        case _ => None
      }
    } else {
      None
    }
  }

  // add Select operator as placeholder on top of subsumee to facilitate matching
  def unifySubsumee(subsumee: ModularPlan): Option[ModularPlan] = {
    subsumee match {
      case gb @ modular.GroupBy(_, _, _, _,
        modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _) =>
        Some(
          Select(gb.outputList, gb.outputList, Nil, Map.empty, Nil, gb :: Nil, gb.flags,
            gb.flagSpec, Seq.empty))
      case other => Some(other)
    }
  }

  // add Select operator as placeholder on top of subsumer to facilitate matching
  def unifySubsumer1(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      dataMapRelation: ModularPlan): ModularPlan = {
    // Update datamap table relation to the subsumer modular plan
    val updatedSubsumer = subsumer match {
      // In case of order by it adds extra select but that can be ignored while doing selection.
      case s@Select(_, _, _, _, _, Seq(g: GroupBy), _, _, _, _) =>
        s.copy(children = Seq(g.copy(dataMapTableRelation = Some(dataMapRelation))),
          outputList = updateDuplicateColumns(s.outputList))
      case s: Select => s
        .copy(dataMapTableRelation = Some(dataMapRelation),
          outputList = updateDuplicateColumns(s.outputList))
      case g: GroupBy => g
        .copy(dataMapTableRelation = Some(dataMapRelation),
          outputList = updateDuplicateColumns(g.outputList))
      case other => other
    }
    (updatedSubsumer, subsumee) match {
      case (r @
        modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _),
        modular.Select(_, _, _, _, _,
          Seq(modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)),
          _, _, _, _)
        ) =>
        modular.Select(
          r.outputList, r.outputList, Nil, Map.empty, Nil, r :: Nil, r.flags,
          r.flagSpec, Seq.empty).setSkip()
      case _ => updatedSubsumer.setSkip()
    }
  }

  private def updateDuplicateColumns(outputList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val duplicateNameCols = outputList.groupBy(_.name).filter(_._2.length > 1).flatMap(_._2)
      .toList
    val updatedOutList = outputList.map { col =>
      val duplicateColumn = duplicateNameCols
        .find(a => a.semanticEquals(col))
      val qualifiedName = col.qualifier.headOption.getOrElse(s"${ col.exprId.id }") + "_" + col.name
      if (duplicateColumn.isDefined) {
        val attributesOfDuplicateCol = duplicateColumn.get.collect {
          case a: AttributeReference => a
        }
        val attributeOfCol = col.collect { case a: AttributeReference => a }
        // here need to check the whether the duplicate columns is of same tables,
        // since query with duplicate columns is valid, we need to make sure, not to change their
        // names with above defined qualifier name, for example in case of some expression like
        // cast((FLOOR((cast(col_name) as double))).., upper layer even exprid will be same,
        // we need to find the attribute ref(col_name) at lower level and check where expid is same
        // or of same tables, so doin the semantic equals
        val isStrictDuplicate = attributesOfDuplicateCol.forall(expr =>
          attributeOfCol.exists(a => a.semanticEquals(expr)))
        if (!isStrictDuplicate) {
          Alias(col, qualifiedName)(exprId = col.exprId)
        } else if (col.qualifier.nonEmpty) {
          Alias(col, qualifiedName)(exprId = col.exprId)
          // this check is added in scenario where the column is direct Attribute reference and
          // since duplicate columns select is allowed, we should just put alias for those columns
          // and update, for this also above isStrictDuplicate will be true so, it will not be
          // updated above
        } else if (duplicateColumn.get.isInstanceOf[AttributeReference] &&
                   col.isInstanceOf[AttributeReference]) {
          Alias(col, qualifiedName)(exprId = col.exprId)
        } else {
          col
        }
      } else {
        col
      }
    }
    updatedOutList
  }

  def unifySubsumer2(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    val rtables = subsumer.collect { case n: modular.LeafNode => n }
    val etables = subsumee.collect { case n: modular.LeafNode => n }
    val pairs = for {
      i <- rtables.indices
      j <- etables.indices
      if rtables(i) == etables(j) && reTablesJoinMatched(
        rtables(i), etables(j), subsumer, subsumee, i, j
      )
    } yield (rtables(i), etables(j))

    pairs.foldLeft(subsumer) {
      case (curSubsumer, pair) =>
        val mappedOperator =
          pair._1 match {
            case relation: HarmonizedRelation if relation.hasTag =>
              pair._2.asInstanceOf[HarmonizedRelation].addTag
            case _ =>
              pair._2
          }
        val nxtSubsumer = curSubsumer.transform {
          case node: ModularRelation if node.fineEquals(pair._1) => mappedOperator
          case pair._1 if !pair._1.isInstanceOf[ModularRelation] => mappedOperator
        }

        // val attributeSet = AttributeSet(pair._1.output)
        // reverse first due to possible tag for left join
        val rewrites = AttributeMap(pair._1.output.zip(mappedOperator.output))
        nxtSubsumer.transformUp {
          case p => p.transformExpressions {
            case a: Attribute if rewrites contains a => rewrites(a).withQualifier(a.qualifier)
          }
        }
    }
  }

  // match the join table of subsumer and subsumee
  // when the table names are the same
  def reTablesJoinMatched(rtable: modular.LeafNode, etable: modular.LeafNode,
                          subsumer: ModularPlan, subsumee: ModularPlan,
                          rIndex: Int, eIndex: Int): Boolean = {
    (rtable, etable) match {
      case _: (ModularRelation, ModularRelation) =>
        val rtableParent = subsumer.find(p => p.children.contains(rtable)).get
        val etableParent = subsumee.find(p => p.children.contains(etable)).get
        (rtableParent, etableParent) match {
          case  (e: Select, r: Select) =>
            val intersetJoinEdges = r.joinEdges intersect e.joinEdges
            if (intersetJoinEdges.nonEmpty) {
              return intersetJoinEdges.exists(j => j.left == rIndex && j.left == eIndex ||
                j.right == rIndex && j.right == eIndex)
            }
          case _ => false
        }
    }
    true
  }

  /**
   * Replace the identified immediate lower level granularity in the modular plan
   * to perform rollup
   *
   * @param plan             to be re-written
   * @param queryGranularity to be replaced
   * @return plan with granularity changed
   */
  private def rewriteGranularityInPlan(plan: ModularPlan, queryGranularity: String) = {
    val newPlan = plan.transformDown {
      case p => p.transformAllExpressions {
        case udf: ScalaUDF =>
          if (udf.function.isInstanceOf[TimeSeriesFunction]) {
            val transformedUdf = udf.transformDown {
              case _: Literal =>
                new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                  DataTypes.StringType)
            }
            transformedUdf
          } else {
            udf
          }
        case alias@Alias(udf: ScalaUDF, name) =>
          if (udf.function.isInstanceOf[TimeSeriesFunction]) {
            var literal: String = null
            val transformedUdf = udf.transformDown {
              case l: Literal =>
                literal = l.value.toString
                new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                  DataTypes.StringType)
            }
            Alias(transformedUdf,
              name.replace(", " + literal, ", " + queryGranularity))(alias.exprId,
              alias.qualifier).asInstanceOf[NamedExpression]
          } else {
            alias
          }
      }
    }
    newPlan
  }

  def getTimeSeriesUdf(outputList: scala.Seq[NamedExpression]): (String, String) = {
    outputList.collect {
      case Alias(udf: ScalaUDF, name) =>
        if (udf.function.isInstanceOf[TimeSeriesFunction]) {
          udf.children.collect {
            case l: Literal =>
              return (name, l.value.toString)
          }
        }
    }
    (null, null)
  }

}
