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

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

import org.apache.carbondata.mv.dsl._
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.testutil.ModularPlanTest

/**
 * Tests for the isSPJGH function of [[ModularPlan]].
 */
class IsSPJGHSuite extends ModularPlanTest {
  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int, 'e.int)

  def assertIsSPJGH(plan: ModularPlan, result: Boolean = true): Unit = {
    if (plan.isSPJGH != result) {
      val ps = plan.toString
      println(s"Plans should return sameResult = $result\n$ps")
    }
  }

  test("project only") {
    assertIsSPJGH(testRelation0.select('a).analyze.modularize)
    assertIsSPJGH(testRelation0.select('a,'b).analyze.modularize)
  }

  test("groupby-project") {
    assertIsSPJGH(testRelation0.select('a).groupBy('a)('a).select('a).analyze.modularize)
    assertIsSPJGH(testRelation0.select('a,'b).groupBy('a,'b)('a,'b).select('a).analyze.modularize)
  }

  test("groupby-project-filter") {
    assertIsSPJGH(testRelation0.where('a === 1).select('a,'b).groupBy('a,'b)('a,'b).select('a).analyze.modularize)   
  }

  test("groupby-project-filter-join") {
    assertIsSPJGH(testRelation0.where('b === 1).join(testRelation1.where('d === 1),condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).groupBy('b,'c)('b,'c).select('b).analyze.modularize)
  }
}
