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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner, _}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

import org.apache.carbondata.mv.dsl.Plans._
import org.apache.carbondata.view.testutil.ModularPlanTest

class ExtractJoinConditionsSuite extends ModularPlanTest {
  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)
  val testRelation2 = LocalRelation('b.int, 'c.int, 'e.int)

}
