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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BinaryArithmetic, BinaryExpression, Expression, In, LessThan, Not, PredicateHelper, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

// This class is used to optimize the filter condition
// If all of the conditions have alias on the aggregate expressions
// it will be pull up and exchange the alias to match the MV
object MVPredicateAlias extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform  {
    case aggregate@Aggregate(_, aggregateExpressions, filter: Filter) =>
      // The flag that all conditions are matched
      var allMatched: Boolean = true
      // The flag the condition has matched, it control filter pull
      var matched: Boolean = false
      val condition = filter.condition.transformUp {
        case x: BinaryArithmetic => x
        case in: In =>
          val matchedAlias = aggregateExpressions.find {
            case alias@Alias(_, _) if alias.child.semanticEquals(in.value) => true
            case _ => false
          }
          if (matchedAlias.isDefined) {
            matched = true
            in.makeCopy(Array(matchedAlias.get.toAttribute, in.list))
          } else {
            allMatched = false
            in
          }
        case binaryExp: BinaryExpression if !binaryExp.left.isInstanceOf[BinaryExpression] =>
          val matchedAlias = aggregateExpressions.find {
            case alias@Alias(_, _) if alias.child.semanticEquals(binaryExp.left) => true
            case _ => false
          }
          if (matchedAlias.isDefined) {
            matched = true
            binaryExp.makeCopy(Array(matchedAlias.get.toAttribute, binaryExp.right))
          } else {
            allMatched = false
            binaryExp
          }
      }
      if (allMatched && matched) {
        val newAggregate = aggregate.copy(child = filter.child)
        Filter(condition, newAggregate)
      } else {
        aggregate
      }
  }
}
