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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql._
import org.apache.spark.sql.CarbonExpressions.CarbonUnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.mutation.CarbonProjectForDeleteCommand
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileFormat, HadoopFsRelation, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants

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

      val tableRelation = if (SPARK_VERSION.startsWith("2.1")) {
        relation
      } else if (SPARK_VERSION.startsWith("2.2")
        || SPARK_VERSION.startsWith("2.3")) {
        alias match {
          case Some(_) =>
            CarbonReflectionUtils.getSubqueryAlias(
              sparkSession,
              alias,
              relation,
              Some(table.tableIdentifier))
          case _ => relation
        }
      } else {
        throw new UnsupportedOperationException("Unsupported Spark version.")
      }

      CarbonReflectionUtils.getSubqueryAlias(
        sparkSession,
        alias,
        Project(projList, tableRelation),
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
            case UnresolvedAttribute(_) =>
              UnresolvedAlias(Alias(attr, col + "-updatedColumn")())
            case _ => attr
          }
        }
        val tableName: Option[Seq[String]] = alias match {
          case Some(_) => Some(alias.toSeq)
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
    val destinationTable = CarbonReflectionUtils.getUnresolvedRelation(table.tableIdentifier, alias)

    ProjectForUpdate(destinationTable, columns, Seq(finalPlan))
  }


  def processDeleteRecordsQuery(selectStmt: String,
      alias: Option[String],
      table: UnresolvedRelation): LogicalPlan = {
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
        if (SPARK_VERSION.startsWith("2.1")) {
          Project(projList, relation)
        } else if (SPARK_VERSION.startsWith("2.2")
          || SPARK_VERSION.startsWith("2.3")) {
          alias match {
            case Some(_) =>
              val subqueryAlias = CarbonReflectionUtils.getSubqueryAlias(
                sparkSession,
                alias,
                relation,
                Some(table.tableIdentifier))
              Project(projList, subqueryAlias)
            case _ => Project(projList, relation)
          }
        } else {
          throw new UnsupportedOperationException("Unsupported Spark version.")
        }
    }
    CarbonProjectForDeleteCommand(
      selectPlan,
      table.tableIdentifier.database,
      table.tableIdentifier.table,
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

/**
 * Insert into carbon table from other source
 */
case class CarbonPreInsertionCasts(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p@InsertIntoTable(relation: LogicalRelation, _, child, _, _)
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        castChildOutput(p, relation, child)
    }
  }

  def castChildOutput(p: InsertIntoTable,
      relation: LogicalRelation,
      child: LogicalPlan): LogicalPlan = {
    val carbonDSRelation = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    if (carbonDSRelation.carbonRelation.output.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      CarbonException.analysisException(
        s"Maximum number of columns supported: " +
          s"${CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS}")
    }
    if (child.output.size >= carbonDSRelation.carbonRelation.output.size ||
        carbonDSRelation.carbonTable.isHivePartitionTable) {
      val newChildOutput = child.output.zipWithIndex.map { columnWithIndex =>
        columnWithIndex._1 match {
          case attr: Alias =>
            Alias(attr.child, s"col${ columnWithIndex._2 }")(attr.exprId)
          case attr: Attribute =>
            Alias(attr, s"col${ columnWithIndex._2 }")(NamedExpression.newExprId)
          case attr => attr
        }
      }
      val version = SPARK_VERSION
      val newChild: LogicalPlan = if (newChildOutput == child.output) {
        if (version.startsWith("2.1")) {
          CarbonReflectionUtils.getField("child", p).asInstanceOf[LogicalPlan]
        } else if (version.startsWith("2.2")) {
          CarbonReflectionUtils.getField("query", p).asInstanceOf[LogicalPlan]
        } else {
          throw new UnsupportedOperationException(s"Spark version $version is not supported")
        }
      } else {
        Project(newChildOutput, child)
      }

      val overwrite = CarbonReflectionUtils.getOverWriteOption("overwrite", p)

      InsertIntoCarbonTable(carbonDSRelation, p.partition, newChild, overwrite, true)
    } else {
      CarbonException.analysisException(
        "Cannot insert into target table because number of columns mismatch")
    }
  }
}

