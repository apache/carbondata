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

import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

// mv sql add the group column to projection list
// when projection list do not contain
object MVAddGroupByColsToProjection extends Rule[LogicalPlan]{

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case agg : Aggregate =>
        var newAgg : Option[Aggregate] = None
        if (agg.groupingExpressions.nonEmpty) {
          agg.groupingExpressions.foreach(groupExp => {
            if (!agg.aggregateExpressions.exists {
              case alias: Alias => alias.semanticEquals(groupExp) ||
                alias.child.semanticEquals(groupExp)
              case other => other.semanticEquals(groupExp)
            }) {
              newAgg = Some(agg.copy(aggregateExpressions =
                agg.aggregateExpressions :+ groupExp.asInstanceOf[NamedExpression]))
            }
          })
        }
        newAgg.getOrElse(plan)
      case _ => plan
    }
  }
}
