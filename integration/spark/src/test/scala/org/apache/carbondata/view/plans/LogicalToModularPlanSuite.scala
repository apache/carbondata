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

package org.apache.carbondata.view.plans

import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{LeftOuter, RightOuter, _}
import org.apache.spark.sql.catalyst.plans.logical._

import org.apache.carbondata.mv.dsl.Plans._
import org.apache.carbondata.mv.plans.modular.{JoinEdge, ModularRelation}
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.view.testutil.ModularPlanTest

class LogicalToModularPlanSuite extends ModularPlanTest {

  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int)

  val testRelation2 = LocalRelation('c.int, 'd.int)

  test("select only") {
    val originalQuery =
      testRelation0
        .select('a.attr)
        .analyze
    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj, MatchLocalRelation(tbl, _)) =>
        ModularRelation(null, null, tbl, NoFlags, Seq.empty)
          .select(proj: _*)(tbl: _*)()(Map.empty)()
    }
    comparePlans(modularized, correctAnswer)
  }

  test("select-project-groupby grouping without aggregate function") {
    val originalQuery =
      testRelation0
        .select('a)
        .groupBy('a)('a)
        .select('a).analyze

    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj1,
      logical.Aggregate(grp, agg, logical.Project(proj2, MatchLocalRelation(tbl, _)))) =>
        ModularRelation(null, null, tbl, NoFlags, Seq.empty)
          .select(proj2: _*)(tbl: _*)()(Map.empty)()
          .groupBy(agg: _*)(proj2: _*)(grp: _*)
          .select(proj1: _*)(proj1: _*)()(Map.empty)()
    }
    comparePlans(modularized, correctAnswer)
  }

  test("select-project with filter") {
    val originalQuery =
      testRelation0
        .where('a + 'b === 1)
        .select('a + 'b as 'e)
        .analyze

    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj, logical.Filter(cond, MatchLocalRelation(tbl, _))) =>
        ModularRelation(null, null, tbl, NoFlags, Seq.empty)
          .select(proj: _*)(tbl: _*)(cond)(Map.empty)()
    }
    comparePlans(modularized, correctAnswer)
  }

}
