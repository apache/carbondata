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

package org.apache.carbondata.mv

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.{JoinEdge, ModularPlan}
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util._

/**
 * A collection of implicit conversions that create a DSL for constructing data structures
 * for modular plans.
 *
 */
package object dsl {

  object Plans {

    implicit class DslModularPlan(val modularPlan: ModularPlan) {
      def select(outputExprs: NamedExpression*)
        (inputExprs: Expression*)
        (predicateExprs: Expression*)
        (aliasMap: Map[Int, String])
        (joinEdges: JoinEdge*): ModularPlan = {
        modular
          .Select(
            outputExprs,
            inputExprs,
            predicateExprs,
            aliasMap,
            joinEdges,
            Seq(modularPlan),
            NoFlags,
            Seq.empty,
            Seq.empty)
      }

      def groupBy(outputExprs: NamedExpression*)
        (inputExprs: Expression*)
        (predicateExprs: Expression*): ModularPlan = {
        modular
          .GroupBy(outputExprs, inputExprs, predicateExprs, None, modularPlan, NoFlags, Seq.empty)
      }

      def harmonize: ModularPlan = modularPlan.harmonized
    }

    implicit class DslModularPlans(val modularPlans: Seq[ModularPlan]) {
      def select(outputExprs: NamedExpression*)
        (inputExprs: Expression*)
        (predicateList: Expression*)
        (aliasMap: Map[Int, String])
        (joinEdges: JoinEdge*): ModularPlan = {
        modular
          .Select(
            outputExprs,
            inputExprs,
            predicateList,
            aliasMap,
            joinEdges,
            modularPlans,
            NoFlags,
            Seq.empty,
            Seq.empty)
      }

      def union(): ModularPlan = modular.Union(modularPlans, NoFlags, Seq.empty)
    }

    implicit class DslLogical2Modular(val logicalPlan: LogicalPlan) {
      def resolveonly: LogicalPlan = analysis.SimpleAnalyzer.execute(logicalPlan)

      def modularize: ModularPlan = modular.SimpleModularizer.modularize(logicalPlan).next

      def optimize: LogicalPlan = BirdcageOptimizer.execute(logicalPlan)
    }

   }

}
