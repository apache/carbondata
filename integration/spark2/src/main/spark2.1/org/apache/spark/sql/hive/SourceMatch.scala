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

import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

object SourceMatch {
  type FlagSet = Long

  val NoFlags: FlagSet = 0L

  type ReturnType = (String, String, Seq[NamedExpression], Seq[LogicalPlan], FlagSet, Seq[Seq[Any]])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case m: CatalogRelation =>
        Some(m.catalogTable.database,m.catalogTable.identifier.table, m.output, Nil, NoFlags,
          Seq.empty)
      case l: LogicalRelation =>
        val tableIdentifier = l.catalogTable.map(_.identifier)
        val database = tableIdentifier.map(_.database).flatten.getOrElse(null)
        val table = tableIdentifier.map(_.table).getOrElse(null)
        Some(database, table, l.output, Nil, NoFlags, Seq.empty)
      case l: LocalRelation => // used for unit test
        Some(null, null, l.output, Nil, NoFlags, Seq.empty)
      case _ => None
    }
  }
}
