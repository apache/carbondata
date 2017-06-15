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
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.ProjectForDeleteCommand
import org.apache.spark.sql.execution.command.ShowCarbonPartitionsCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Insert into carbon table from other source
 */
object CarbonPreInsertionCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    case p @ InsertIntoTable(relation: LogicalRelation, _, child, _, _)
      if relation.relation.isInstanceOf[CarbonDatasourceRelation] =>
      castChildOutput(p, relation.relation.asInstanceOf[CarbonDatasourceRelation], child)
  }

  def castChildOutput(p: InsertIntoTable, relation: CarbonDatasourceRelation, child: LogicalPlan)
  : LogicalPlan = {
    if (relation.carbonRelation.output.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      sys
        .error("Maximum supported column by carbon is:" + CarbonCommonConstants
          .DEFAULT_MAX_NUMBER_OF_COLUMNS
        )
    }
    if (child.output.size >= relation.carbonRelation.output.size ) {
      InsertIntoCarbonTable(relation, p.partition, p.child, p.overwrite, p.ifNotExists)
    } else {
      sys.error("Cannot insert into target table because column number are different")
    }
  }
}


object CarbonIUDAnalysisRule extends Rule[LogicalPlan] {

  var sqlContext: SQLContext = _

  def init(sqlContext: SQLContext) {
    this.sqlContext = sqlContext
  }

  private def processUpdateQuery(
                   table: UnresolvedRelation,
                   columns: List[String],
                   selectStmt: String,
                   filter: String): LogicalPlan = {
    var includedDestColumns = false
    var includedDestRelation = false
    var addedTupleId = false

    def prepareTargetReleation(relation: UnresolvedRelation): Subquery = {
      val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
        Seq.empty, isDistinct = false), "tupleId")())
      val projList = Seq(
        UnresolvedAlias(UnresolvedStar(table.alias)), tupleId)
      // include tuple id and rest of the required columns in subqury
      Subquery(table.alias.getOrElse(""), Project(projList, relation))
    }
    // get the un-analyzed logical plan
    val targetTable = prepareTargetReleation(table)
    val selectPlan = org.apache.spark.sql.SQLParser.parse(selectStmt, sqlContext) transform {
      case Project(projectList, child) if (!includedDestColumns) =>
        includedDestColumns = true
        if (projectList.size != columns.size) {
          sys.error("Number of source and destination columns are not matching")
        }
        val renamedProjectList = projectList.zip(columns).map{ case(attr, col) =>
          attr match {
            case UnresolvedAlias(child) =>
              UnresolvedAlias(Alias(child, col + "-updatedColumn")())
            case _ => attr
          }
        }
        val list = Seq(
          UnresolvedAlias(UnresolvedStar(table.alias))) ++ renamedProjectList
        Project(list, child)
      case Filter(cond, child) if (!includedDestRelation) =>
        includedDestRelation = true
        Filter(cond, Join(child, targetTable, Inner, None))
      case r @ UnresolvedRelation(t, a) if (!includedDestRelation &&
        t != table.tableIdentifier) =>
        includedDestRelation = true
        Join(r, targetTable, Inner, None)
    }
    val updatedSelectPlan = if (!includedDestRelation) {
      // special case to handle self join queries
      // Eg. update tableName  SET (column1) = (column1+1)
      selectPlan transform {
        case relation: UnresolvedRelation if (table.tableIdentifier == relation.tableIdentifier &&
          addedTupleId == false) =>
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
      SQLParser.parse("select * from  " +
        table.tableIdentifier.mkString(".") + " " + alias + " " + filter, sqlContext)  transform {
        case UnresolvedRelation(t, Some(a)) if (
          !transformed && t == table.tableIdentifier && a == alias) =>
          transformed = true
          // Add the filter condition of update statement  on destination table
          Subquery(alias, updatedSelectPlan)
      }
    } else {
      updatedSelectPlan
    }
    val tid = CarbonTableIdentifierImplicit.toTableIdentifier(table.tableIdentifier)
    val tidSeq = Seq(getDB.getDatabaseName(tid.database, sqlContext), tid.table)
    val destinationTable = UnresolvedRelation(tidSeq, table.alias)
    ProjectForUpdate(destinationTable, columns, Seq(finalPlan))
  }

  def processDeleteRecordsQuery(selectStmt: String, table: UnresolvedRelation): LogicalPlan = {
    val tid = CarbonTableIdentifierImplicit.toTableIdentifier(table.tableIdentifier)
    val tidSeq = Seq(getDB.getDatabaseName(tid.database, sqlContext), tid.table)
    var addedTupleId = false
    val selectPlan = SQLParser.parse(selectStmt, sqlContext) transform {
      case relation: UnresolvedRelation if (table.tableIdentifier == relation.tableIdentifier &&
        addedTupleId == false) =>
        addedTupleId = true
        val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
          Seq.empty, isDistinct = false), "tupleId")())
        val projList = Seq(
          UnresolvedAlias(UnresolvedStar(table.alias)), tupleId)
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
      case ShowPartitions(t) => ShowCarbonPartitionsCommand(t)
    }
  }
}
