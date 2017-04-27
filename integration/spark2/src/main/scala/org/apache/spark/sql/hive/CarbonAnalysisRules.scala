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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.core.constants.CarbonCommonConstants


/**
 * Insert into carbon table from other source
 */
object CarbonPreInsertionCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p@InsertIntoTable(relation: LogicalRelation, _, child, _, _)
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        castChildOutput(p, relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation], child)
    }
  }

  def castChildOutput(p: InsertIntoTable,
      relation: CarbonDatasourceHadoopRelation,
      child: LogicalPlan)
  : LogicalPlan = {
    if (relation.carbonRelation.output.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      sys
        .error("Maximum supported column by carbon is:" + CarbonCommonConstants
          .DEFAULT_MAX_NUMBER_OF_COLUMNS
        )
    }
    if (child.output.size >= relation.carbonRelation.output.size) {
      val newChildOutput = child.output.zipWithIndex.map { columnWithIndex =>
        columnWithIndex._1 match {
          case attr: Alias =>
            Alias(attr.child, s"col${ columnWithIndex._2 }")(attr.exprId)
          case attr: Attribute =>
            Alias(attr, s"col${ columnWithIndex._2 }")(NamedExpression.newExprId)
          case attr => attr
        }
      }
      val newChild: LogicalPlan = if (newChildOutput == child.output) {
        p.child
      } else {
        Project(newChildOutput, child)
      }
      InsertIntoCarbonTable(relation, p.partition, newChild, p.overwrite, p.ifNotExists)
    } else {
      sys.error("Cannot insert into target table because column number are different")
    }
  }
}
