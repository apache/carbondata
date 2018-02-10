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

package org.apache.spark.sql.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, PredicateHelper,
ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.constants.CarbonCommonConstants

class CarbonUDFTransformRule extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
      pushDownUDFToJoinLeftRelation(plan)
  }

  private def pushDownUDFToJoinLeftRelation(plan: LogicalPlan): LogicalPlan = {
    val output = plan.transform {
      case proj@Project(cols, Join(
      left, right, jointype: org.apache.spark.sql.catalyst.plans.JoinType, condition)) =>
        var projectionToBeAdded: Seq[org.apache.spark.sql.catalyst.expressions.Alias] = Seq.empty
        var udfExists = false
        val newCols = cols.map {
          case a@Alias(s: ScalaUDF, name)
            if name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID) ||
               name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID) =>
            udfExists = true
            projectionToBeAdded :+= a
            AttributeReference(name, StringType, nullable = true)().withExprId(a.exprId)
          case other => other
        }
        if (udfExists) {
          val newLeft = left match {
            case Project(columns, logicalPlan) =>
              Project(columns ++ projectionToBeAdded, logicalPlan)
            case filter: Filter =>
              Project(filter.output ++ projectionToBeAdded, filter)
            case relation: LogicalRelation =>
              Project(relation.output ++ projectionToBeAdded, relation)
            case other => other
          }
          Project(newCols, Join(newLeft, right, jointype, condition))
        } else {
          proj
        }
      case other => other
    }
    output
  }

}
