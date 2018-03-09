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

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.util.{Signature, SignatureGenerator, SignatureRule}

object ModularPlanSignatureGenerator extends SignatureGenerator[ModularPlan] {
  lazy val rule: SignatureRule[ModularPlan] = ModularPlanRule

  override def generate(plan: ModularPlan): Option[Signature] = {
    if (plan.isSPJGH) {
      super.generate(plan)
    } else {
      None
    }
  }
}

object ModularPlanRule extends SignatureRule[ModularPlan] {

  def apply(plan: ModularPlan, childSignatures: Seq[Option[Signature]]): Option[Signature] = {

    plan match {
      case modular.Select(_, _, _, _, _, _, _, _, _) =>
        if (childSignatures.map { _.getOrElse(Signature()).groupby }.forall(x => !x)) {
          Some(Signature(
            groupby = false,
            childSignatures.flatMap { _.getOrElse(Signature()).datasets.toSeq }.toSet))
        } else if (childSignatures.length == 1 &&
                   childSignatures(0).getOrElse(Signature()).groupby) {
          childSignatures(0)
        } else {
          None
        }
      case modular.GroupBy(_, _, _, _, _, _, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
          Some(Signature(groupby = true, childSignatures(0).getOrElse(Signature()).datasets))
        } else {
          None
        }
      case HarmonizedRelation(source) =>
        source.signature match {
          case Some(s) =>
            Some(Signature(groupby = false, s.datasets))
          case _ =>
            None
        }
      case modular.ModularRelation(dbname, tblname, _, _, _) =>
        if (dbname != null && tblname != null) {
          Some(Signature(groupby = false, Set(Seq(dbname, tblname).mkString("."))))
        } else {
          Some(Signature(groupby = false, Set(plan.toString())))
        }
      case _ => None
    }
  }
}
