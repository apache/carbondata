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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.{RuleExecutor, _}
import org.apache.spark.sql.internal.SQLConf

object BirdcageOptimizer extends RuleExecutor[LogicalPlan] {

  val conf = new SQLConf()
    .copy(SQLConf.CASE_SENSITIVE -> true, SQLConf.STARSCHEMA_DETECTION -> true)
  protected val fixedPoint = FixedPoint(conf.optimizerMaxIterations)

  def batches: Seq[Batch] = {
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch(
      "Finish Analysis", Once,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      //      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch(
      "Union", Once,
      CombineUnions) ::
    Batch(
      "Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    Batch(
      "Subquery", Once,
      OptimizeSubqueries) ::
    Batch(
      "Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch(
      "Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) ::
    Batch(
      "Operator Optimizations", fixedPoint, Seq(
        // Operator push down
        PushProjectionThroughUnion,
        ReorderJoin(conf),
        EliminateOuterJoin(conf),
        PushPredicateThroughJoin,
        PushDownPredicate,
        //      LimitPushDown(conf),
        ColumnPruning,
        //      InferFiltersFromConstraints(conf),
        // Operator combine
        CollapseRepartition,
        CollapseProject,
        CollapseWindow,
        CombineFilters,
        CombineLimits,
        CombineUnions,
        // Constant folding and strength reduction
        NullPropagation(conf),
        FoldablePropagation,
        //      OptimizeIn(conf),
        ConstantFolding,
        ReorderAssociativeOperator,
        LikeSimplification,
        BooleanSimplification,
        SimplifyConditionals,
        RemoveDispensableExpressions,
        SimplifyBinaryComparison,
        //      PruneFilters(conf),
        EliminateSorts,
        SimplifyCasts,
        SimplifyCaseConversionExpressions,
        RewriteCorrelatedScalarSubquery,
        EliminateSerialization,
        RemoveRedundantAliases,
        RemoveRedundantProject,
        SimplifyCreateStructOps,
        SimplifyCreateArrayOps,
        SimplifyCreateMapOps) ++
                                            extendedOperatorOptimizationRules: _*) ::
    Batch(
      "Check Cartesian Products", Once,
      CheckCartesianProducts(conf)) ::
    //    Batch("Join Reorder", Once,
    //      CostBasedJoinReorder(conf)) ::
    //    Batch("Decimal Optimizations", fixedPoint,
    //      DecimalAggregates(conf)) ::
    Batch(
      "Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters) ::
    //    Batch("LocalRelation", fixedPoint,
    //      ConvertToLocalRelation,
    //      PropagateEmptyRelation) ::
    Batch(
      "OptimizeCodegen", Once,
      OptimizeCodegen(conf)) ::
    Batch(
      "RewriteSubquery", Once,
      RewritePredicateSubquery,
      CollapseProject) :: Nil
  }

  /**
   * Optimize all the subqueries inside expression.
   */
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan transformAllExpressions {
        case s: SubqueryExpression =>
          val Subquery(newPlan) = BirdcageOptimizer.this.execute(Subquery(s.plan))
          s.withNewPlan(newPlan)
      }
    }
  }

  /**
   * Override to provide additional rules for the operator optimization batch.
   */
  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = {
    Nil
  }
}

/**
 * Push Aggregate through join to fact table.
 * Pushes down [[Aggregate]] operators where the `grouping` and `aggregate` expressions can
 * be evaluated using only the attributes of the fact table, the left or right side of a
 * star-join.
 * Other [[Aggregate]] expressions stay in the original [[Aggregate]].
 *
 * Check 'Aggregate Pushdown Over Join: Design & Preliminary Results' by LiTao for more details
 */
// case class PushAggregateThroughJoin(conf: SQLConf) extends Rule[LogicalPlan] with
// PredicateHelper {
//
//  val tableCluster = {
//    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
//    val tableClusterString = conf.getConfString("spark.mv.tableCluster")
//    mapper.readValue(tableClusterString, classOf[TableCluster])
//  }
//
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//
//    // Push down aggregate expressions through Join
//    case a @ Aggregate(grouping, aggregate, Project(projectList, Join(left, right, jt, cond)))
//        if (left.isInstanceOf[LeafNode] && => {
//      val fTables: Set[String] = tableCluster.getFact
//      val dTables: Set[String] = tableCluster.getDimension
// //      if canPushThrough(left,a)
//
//      if (fTables.contains(s"${left.databaseName}.${left.tableName}")
//          Aggregate(newGrouping, newAggregate, Project(projectList, Join(Aggregate(_,_,Project
// (projectList1, left)), right, jt, cond)))
//      }
//    }
//
//  private def canPushThrough(join: Join): Boolean = join match {
//    case Join(left : LeafNode, right: LeafNode, Inner, EqualTo(l: AttributeReference,
// r: AttributeReference)) => true
//
//
//  }
//
//
// }
