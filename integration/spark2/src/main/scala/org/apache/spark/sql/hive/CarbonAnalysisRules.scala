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
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.{ProjectExec, SparkSqlParser, SubqueryExec}
import org.apache.spark.sql.execution.command.ProjectForDeleteCommand
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

object CarbonIUDAnalysisRule extends Rule[LogicalPlan] {

  var sparkSession: SparkSession = _

  def init(sparkSession: SparkSession) {
     this.sparkSession = sparkSession
  }

  private lazy val parser = new SparkSqlParser(sparkSession.sessionState.conf)

  private def processUpdateQuery(
      table: UnresolvedRelation,
      columns: List[String],
      selectStmt: String,
      filter: String): LogicalPlan = {
    var includedDestColumns = false
    var includedDestRelation = false
    var addedTupleId = false

    def prepareTargetReleation(relation: UnresolvedRelation): SubqueryAlias = {
      val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
        Seq.empty, isDistinct = false), "tupleId")())
      val projList = Seq(
        UnresolvedAlias(UnresolvedStar(Option(table.alias.toSeq))), tupleId)
      // include tuple id and rest of the required columns in subqury
      SubqueryAlias(table.alias.getOrElse(""),
        Project(projList, relation), Option(table.tableIdentifier))
    }
    // get the un-analyzed logical plan
    val targetTable = prepareTargetReleation(table)
    val selectPlan = parser.parsePlan(selectStmt) transform {
      case Project(projectList, child) if !includedDestColumns =>
        includedDestColumns = true
        if (projectList.size != columns.size) {
          sys.error("Number of source and destination columns are not matching")
        }
        val renamedProjectList = projectList.zip(columns).map{ case(attr, col) =>
          attr match {
            case UnresolvedAlias(child22, _) =>
              UnresolvedAlias(Alias(child22, col + "-updatedColumn")())
            case UnresolvedAttribute(param) =>
              UnresolvedAlias(Alias(attr, col + "-updatedColumn")())
             // UnresolvedAttribute(col + "-updatedColumn")
//              UnresolvedAlias(Alias(child, col + "-updatedColumn")())
            case _ => attr
          }
        }
        val list = Seq(
          UnresolvedAlias(UnresolvedStar(Option(table.alias.toSeq)))) ++ renamedProjectList
        Project(list, child)
      case Filter(cond, child) if !includedDestRelation =>
        includedDestRelation = true
        Filter(cond, Join(child, targetTable, Inner, None))
      case r @ UnresolvedRelation(t, a) if !includedDestRelation && t != table.tableIdentifier =>
        includedDestRelation = true
        Join(r, targetTable, Inner, None)
    }
    val updatedSelectPlan : LogicalPlan = if (!includedDestRelation) {
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
      val alias = table.alias.getOrElse("")
      var transformed: Boolean = false
      // Create a dummy projection to include filter conditions
      var newPlan: LogicalPlan = null
      if (table.tableIdentifier.database.isDefined) {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.database.getOrElse("") + "." +
           table.tableIdentifier.table + " " + alias + " " + filter)
      }
      else {
        newPlan = parser.parsePlan("select * from  " +
           table.tableIdentifier.table + " " + alias + " " + filter)
      }
      newPlan transform {
        case UnresolvedRelation(t, Some(a))
          if !transformed && t == table.tableIdentifier && a == alias =>
          transformed = true
          // Add the filter condition of update statement  on destination table
          SubqueryAlias(alias, updatedSelectPlan, Option(table.tableIdentifier))
      }
    } else {
      updatedSelectPlan
    }
    val tid = CarbonTableIdentifierImplicit.toTableIdentifier(Seq(table.tableIdentifier.toString()))
    val tidSeq = Seq(getDB.getDatabaseName(tid.database, sparkSession))
    val destinationTable = UnresolvedRelation(table.tableIdentifier, table.alias)
    ProjectForUpdate(destinationTable, columns, Seq(finalPlan))
  }

  def processDeleteRecordsQuery(selectStmt: String, table: UnresolvedRelation): LogicalPlan = {
   val tidSeq = Seq(getDB.getDatabaseName(table.tableIdentifier.database, sparkSession),
     table.tableIdentifier.table)
    var addedTupleId = false
    val parsePlan = parser.parsePlan(selectStmt)
    val selectPlan = parsePlan transform {
      case relation: UnresolvedRelation
        if table.tableIdentifier == relation.tableIdentifier && !addedTupleId =>
        addedTupleId = true
        val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
          Seq.empty, isDistinct = false), "tupleId")())
        val alias = table.alias match {
          case Some(alias) => Some(table.alias.toSeq)
          case _ => None
        }
        val projList = Seq(
          UnresolvedAlias(UnresolvedStar(alias)), tupleId)
        // include tuple id in subqury
        Project(projList, relation)
    }
    ProjectForDeleteCommand(
      selectPlan,
      tidSeq,
      System.currentTimeMillis().toString)
  }

  override def apply(logicalplan: LogicalPlan): LogicalPlan = {

    logicalplan transform {
      case UpdateTable(t, cols, sel, where) => processUpdateQuery(t, cols, sel, where)
      case DeleteRecords(statement, table) => processDeleteRecordsQuery(statement, table)
    }
  }
}
