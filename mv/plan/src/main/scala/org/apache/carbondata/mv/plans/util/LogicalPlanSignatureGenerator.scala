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

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.mv.plans._

object CheckSPJG {

  def isSPJG(subplan: LogicalPlan): Boolean = {
    subplan match {
      case a: Aggregate =>
        a.child.collect {
          case Join(_, _, _, _) | Project(_, _) | Filter(_, _) |
               HiveTableRelation(_, _, _) => true
          case  l: LogicalRelation => true
          case _ => false
        }.forall(identity)
      case _ => false
    }
  }
}

object LogicalPlanSignatureGenerator extends SignatureGenerator[LogicalPlan] {
  lazy val rule: SignatureRule[LogicalPlan] = LogicalPlanRule

  override def generate(plan: LogicalPlan): Option[Signature] = {
    if ( plan.isSPJG ) {
      super.generate(plan)
    } else {
      None
    }
  }
}

object LogicalPlanRule extends SignatureRule[LogicalPlan] {

  def apply(plan: LogicalPlan, childSignatures: Seq[Option[Signature]]): Option[Signature] = {

    plan match {
      case l: LogicalRelation =>
        // TODO: implement this (link to BaseRelation)
        None
      case HiveTableRelation(tableMeta, _, _) =>
        Some(Signature(false,
          Set(Seq(tableMeta.database, tableMeta.identifier.table).mkString("."))))
      case l : LocalRelation =>
        // LocalRelation is for unit test cases
        Some(Signature(groupby = false, Set(l.toString())))
      case Filter(_, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
          // if (!childSignatures(0).getOrElse(Signature()).groupby) {
          childSignatures(0)
          // }
        } else {
          None
        }
      case Project(_, _) =>
        if ( childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby ) {
          childSignatures(0)
        } else {
          None
        }
      case Join(_, _, _, _) =>
        if ( childSignatures.length == 2 &&
             !childSignatures(0).getOrElse(Signature()).groupby &&
             !childSignatures(1).getOrElse(Signature()).groupby ) {
          Some(Signature(false,
            childSignatures(0).getOrElse(Signature()).datasets
              .union(childSignatures(1).getOrElse(Signature()).datasets)))
        } else {
          None
        }
      case Aggregate(_, _, _) =>
        if ( childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby ) {
          Some(Signature(true, childSignatures(0).getOrElse(Signature()).datasets))
        } else {
          None
        }
      case _ => None
    }
  }
}
