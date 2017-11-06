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
import org.apache.spark.sql.CarbonExpressions.CarbonUnresolvedRelation
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.mutation.ProjectForDeleteCommand
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

case class CarbonIUDAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private lazy val parser = sparkSession.sessionState.sqlParser

  private def processUpdateQuery(
      table: UnresolvedRelation,
      columns: List[String],
      selectStmt: String,
      alias: Option[String],
      filter: String): LogicalPlan = {
    var includedDestColumns = false
    var includedDestRelation = false
    var addedTupleId = false

    def prepareTargetReleation(relation: UnresolvedRelation): SubqueryAlias = {
      val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
        Seq.empty, isDistinct = false), "tupleId")())

      val projList = Seq(UnresolvedAlias(UnresolvedStar(alias.map(Seq(_)))), tupleId)

      CarbonReflectionUtils.getSubqueryAlias(
        sparkSession,
        alias,
        Project(projList, relation),
        Some(table.tableIdentifier))
    }

    // get the un-analyzed logical plan
    val targetTable = prepareTargetReleation(table)
    val selectPlan = parser.parsePlan(selectStmt) transform {
      case Project(projectList, child) if !includedDestColumns =>
        includedDestColumns = true
        if (projectList.size != columns.size) {
          CarbonException.analysisException(
            "The number of columns in source table and destination table columns mismatch")
        }
        val renamedProjectList = projectList.zip(columns).map { case (attr, col) =>
          attr match {
            case UnresolvedAlias(child22, _) =>
              UnresolvedAlias(Alias(child22, col + "-updatedColumn")())
            case UnresolvedAttribute(param) =>
              UnresolvedAlias(Alias(attr, col + "-updatedColumn")())
            case _ => attr
          }
        }
        val tableName: Option[Seq[String]] = alias match {
          case Some(a) => Some(alias.toSeq)
          case _ => Some(Seq(child.asInstanceOf[UnresolvedRelation].tableIdentifier.table.toString))
        }
        val list = Seq(
          UnresolvedAlias(UnresolvedStar(tableName))) ++ renamedProjectList
        Project(list, child)
      case Filter(cond, child) if !includedDestRelation =>
        includedDestRelation = true
        Filter(cond, Join(child, targetTable, Inner, None))
      case r@CarbonUnresolvedRelation(t) if !includedDestRelation && t != table.tableIdentifier =>
        includedDestRelation = true
        Join(r, targetTable, Inner, None)
    }
    val updatedSelectPlan: LogicalPlan = if (!includedDestRelation) {
      // special case to handle self join queries
      // Eg. update tableName  SET (column1) = (column1+1)
      selectPlan transform {
        case relation: UnresolvedRelation
          if table.tableIdentifier == relation.tableIdentifier && !addedTupleId =>
          addedTupleId = true
          targetTable
      }
    } else {
      selectPlan
    }
    val finalPlan = if (filter.length > 0) {
      var transformed: Boolean = false
      // Create a dummy projection to include filter conditions
      var newPlan: LogicalPlan = null
      if (table.tableIdentifier.database.isDefined) {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.database.getOrElse("") + "." +
           table.tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      else {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      newPlan transform {
        case CarbonUnresolvedRelation(t)
          if !transformed && t == table.tableIdentifier =>
          transformed = true

          CarbonReflectionUtils.getSubqueryAlias(
            sparkSession,
            alias,
            updatedSelectPlan,
            Some(table.tableIdentifier))
      }
    } else {
      updatedSelectPlan
    }
    val tid = CarbonTableIdentifierImplicit.toTableIdentifier(Seq(table.tableIdentifier.toString()))
    val tidSeq = Seq(GetDB.getDatabaseName(tid.database, sparkSession))
    val destinationTable =
      CarbonReflectionUtils.getUnresolvedRelation(
        table.tableIdentifier,
        sparkSession.version,
        alias)

    ProjectForUpdate(destinationTable, columns, Seq(finalPlan))
  }


  def processDeleteRecordsQuery(selectStmt: String,
      alias: Option[String],
      table: UnresolvedRelation): LogicalPlan = {
    val tidSeq = Seq(GetDB.getDatabaseName(table.tableIdentifier.database, sparkSession),
      table.tableIdentifier.table)
    var addedTupleId = false
    val parsePlan = parser.parsePlan(selectStmt)

    val selectPlan = parsePlan transform {
      case relation: UnresolvedRelation
        if table.tableIdentifier == relation.tableIdentifier && !addedTupleId =>
        addedTupleId = true
        val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
          Seq.empty, isDistinct = false), "tupleId")())

        val projList = Seq(UnresolvedAlias(UnresolvedStar(alias.map(Seq(_)))), tupleId)
        // include tuple id in subquery
        Project(projList, relation)
    }
    ProjectForDeleteCommand(
      selectPlan,
      tidSeq,
      System.currentTimeMillis().toString)
  }

  override def apply(logicalplan: LogicalPlan): LogicalPlan = {

    logicalplan transform {
      case UpdateTable(t, cols, sel, alias, where) => processUpdateQuery(t, cols, sel, alias, where)
      case DeleteRecords(statement, alias, table) =>
        processDeleteRecordsQuery(
          statement,
          alias,
          table)
    }
  }
}
