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

import org.apache.spark.sql.ProjectForUpdate
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.mutation.CarbonProjectForUpdateCommand

/**
 * Rule specific for IUD operations
 */
class CarbonIUDRule extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
      processPlan(plan)
  }

  private def processPlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case ProjectForUpdate(table, cols, Seq(updatePlan)) =>
        var isTransformed = false
        val newPlan = updatePlan transform {
          case Project(pList, child) if !isTransformed =>
            val (dest: Seq[NamedExpression], source: Seq[NamedExpression]) = pList
              .splitAt(pList.size - cols.size)
            val diff = cols.diff(dest.map(_.name.toLowerCase))
            cols.foreach { col =>
              val complexExists = "\"name\":\"" + col + "\""
              if (dest.exists(m => m.dataType.json.contains(complexExists))) {
                throw new UnsupportedOperationException(
                  "Unsupported operation on Complex data type")
              }
            }
            if (diff.nonEmpty) {
              sys.error(s"Unknown column(s) ${ diff.mkString(",") } in table ${ table.tableName }")
            }
            isTransformed = true
            Project(dest.filter(a => !cols.contains(a.name.toLowerCase)) ++ source, child)
        }
        CarbonProjectForUpdateCommand(
          newPlan, table.tableIdentifier.database, table.tableIdentifier.table, cols)
    }
  }
}
