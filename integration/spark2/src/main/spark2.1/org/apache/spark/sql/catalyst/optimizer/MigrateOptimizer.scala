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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CaseKeyWhen, CreateArray, CreateMap, CreateNamedStructLike, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.objects.{LambdaVariable, MapObjects}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule

class MigrateOptimizer {

}

/**
  * Replaces logical [[Deduplicate]] operator with an [[Aggregate]] operator.
  */
object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Deduplicate(keys, child, streaming) if !streaming =>
      val keyExprIds = keys.map(_.exprId)
      val aggCols = child.output.map { attr =>
        if (keyExprIds.contains(attr.exprId)) {
          attr
        } else {
          Alias(new First(attr).toAggregateExpression(), attr.name)(attr.exprId)
        }
      }
      Aggregate(keys, aggCols, child)
  }
}

/** A logical plan for `dropDuplicates`. */
case class Deduplicate(
                        keys: Seq[Attribute],
                        child: LogicalPlan,
                        streaming: Boolean) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}

/**
  * Remove projections from the query plan that do not make any modifications.
  */
object RemoveRedundantProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(_, child) if p.output == child.output => child
  }
}

/**
  * push down operations into [[CreateNamedStructLike]].
  */
object SimplifyCreateStructOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // push down field extraction
      case GetStructField(createNamedStructLike: CreateNamedStructLike, ordinal, _) =>
        createNamedStructLike.valExprs(ordinal)
    }
  }
}

/**
  * push down operations into [[CreateArray]].
  */
object SimplifyCreateArrayOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      // push down field selection (array of structs)
      case GetArrayStructFields(CreateArray(elems), field, ordinal, numFields, containsNull) =>
        // instead f selecting the field on the entire array,
        // select it from each member of the array.
        // pushing down the operation this way open other optimizations opportunities
        // (i.e. struct(...,x,...).x)
        CreateArray(elems.map(GetStructField(_, ordinal, Some(field.name))))
      // push down item selection.
      case ga @ GetArrayItem(CreateArray(elems), IntegerLiteral(idx)) =>
        // instead of creating the array and then selecting one row,
        // remove array creation altgether.
        if (idx >= 0 && idx < elems.size) {
          // valid index
          elems(idx)
        } else {
          // out of bounds, mimic the runtime behavior and return null
          Literal(null, ga.dataType)
        }
    }
  }
}

/**
  * push down operations into [[CreateMap]].
  */
object SimplifyCreateMapOps extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformExpressionsUp {
      case GetMapValue(CreateMap(elems), key) => CaseKeyWhen(key, elems)
    }
  }
}


/**
  * Removes MapObjects when the following conditions are satisfied
  *   1. Mapobject(... lambdavariable(..., false) ...), which means types for input and output
  *      are primitive types with non-nullable
  *   2. no custom collection class specified representation of data item.
  */
object EliminateMapObjects extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case MapObjects(_, _, _, LambdaVariable(_, _, _), inputData) => inputData
  }
}
