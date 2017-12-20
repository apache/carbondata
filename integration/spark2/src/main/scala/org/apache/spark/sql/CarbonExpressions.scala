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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.types.DataType

/**
 * This class contains the wrappers of all the case classes which are common
 * across spark version 2.1 and 2.2 but have change in parameter list.
 * Below are the overriden unapply methods in order to make it work
 * across both the version of spark2.1 and spark 2.2
 */
object CarbonExpressions {

  /**
   * unapply method of Cast class.
   */
  object MatchCast {
    def unapply(expr: Expression): Option[(Attribute, DataType)] = {
      expr match {
        case a: Cast if a.child.isInstanceOf[Attribute] =>
          Some((a.child.asInstanceOf[Attribute], a.dataType))
        case _ => None
      }
    }
  }

  /**
   * unapply method of Cast class with expression.
   */
  object MatchCastExpression {
    def unapply(expr: Expression): Option[(Expression, DataType)] = {
      expr match {
        case a: Cast if a.child.isInstanceOf[Expression] =>
          Some((a.child.asInstanceOf[Expression], a.dataType))
        case _ => None
      }
    }
  }

  /**
   * unapply method of Describe Table format.
   */
  object CarbonDescribeTable {
    def unapply(plan: LogicalPlan): Option[(TableIdentifier, TablePartitionSpec, Boolean)] = {
      plan match {
        case desc: DescribeTableCommand =>
          Some(desc.table, desc.partitionSpec, desc.isExtended)
        case _ => None
      }
    }
  }

  /**
   * unapply method of SubqueryAlias.
   */
  object CarbonSubqueryAlias {
    def unapply(plan: LogicalPlan): Option[(String, LogicalPlan)] = {
      plan match {
        case s: SubqueryAlias =>
          Some(s.asInstanceOf[SubqueryAlias].alias, s.asInstanceOf[SubqueryAlias].child)
        case _ => None
      }
    }
  }

  /**
   * uapply method of UnresolvedRelation
   */
  object CarbonUnresolvedRelation {
    def unapply(plan: LogicalPlan): Option[(TableIdentifier)] = {
      plan match {
        case u: UnresolvedRelation =>
          Some(u.tableIdentifier)
        case _ => None
      }
    }
  }

  /**
   * unapply method of Scala UDF
   */
  object CarbonScalaUDF {
    def unapply(expression: Expression): Option[(ScalaUDF)] = {
      expression match {
        case a: ScalaUDF =>
          Some(a)
        case _ =>
          None
      }
    }
  }
}
