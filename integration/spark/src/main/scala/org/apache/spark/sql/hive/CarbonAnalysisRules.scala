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

import org.apache.spark
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql._
import org.apache.spark.sql.CarbonExpressions.CarbonUnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.execution.command.mutation.CarbonProjectForDeleteCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.{CarbonPlanHelper, DMLHelper}
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.view.MVManagerInSpark

case class CarbonIUDAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private lazy val parser = sparkSession.sessionState.sqlParser
  private lazy val optimizer = sparkSession.sessionState.optimizer
  private lazy val analyzer = sparkSession.sessionState.analyzer

  private def projectionWithTupleId(alias: Option[String]): Seq[UnresolvedAlias] = {
    val tupleId = UnresolvedAlias(Alias(UnresolvedFunction("getTupleId",
      Seq.empty, isDistinct = false), "tupleId")())
    Seq(UnresolvedAlias(UnresolvedStar(alias.map(Seq(_)))), tupleId)
  }

  private def processUpdateQuery(
      table: UnresolvedRelation,
      columns: List[String],
      selectStmt: String,
      alias: Option[String],
      filter: String): LogicalPlan = {
    val tableIdentifier = CarbonToSparkAdapter.getTableIdentifier(table).get
    CarbonPlanHelper.validateCarbonTable(
      tableIdentifier,
      sparkSession,
      "only CarbonData table support update operation"
    )
    var includedDestColumns = false
    var includedDestRelation = false
    var addedTupleId = false

    def prepareTargetRelation(relation: UnresolvedRelation): SubqueryAlias = {
      val projList = projectionWithTupleId(alias)
      val carbonTable = CarbonEnv.
        getCarbonTable(tableIdentifier)(sparkSession)
      MVManagerInSpark.disableMVOnTable(sparkSession, carbonTable)
      val tableRelation =
        alias match {
          case Some(_) =>
            CarbonReflectionUtils.getSubqueryAlias(
              sparkSession,
              alias,
              relation,
              Some(tableIdentifier))
          case _ => relation
        }

      CarbonReflectionUtils.getSubqueryAlias(
        sparkSession,
        alias,
        Project(projList, tableRelation),
        Some(tableIdentifier))
    }

    // get the un-analyzed logical plan
    val targetTable = prepareTargetRelation(table)
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
              UnresolvedAlias(Alias(child22, col + CarbonCommonConstants.UPDATED_COL_EXTENSION)())
            case UnresolvedAttribute(_) =>
              UnresolvedAlias(Alias(attr, col + CarbonCommonConstants.UPDATED_COL_EXTENSION)())
            case _ => attr
          }
        }
        val tableName: Option[Seq[String]] = alias match {
          case Some(_) => Some(alias.toSeq)
          case _ => Some(Seq(CarbonToSparkAdapter.getTableIdentifier(
            child.asInstanceOf[UnresolvedRelation]).get.table))
        }
        val list = Seq(
          UnresolvedAlias(UnresolvedStar(tableName))) ++ renamedProjectList
        Project(list, child)
      case Filter(cond, child) if !includedDestRelation =>
        includedDestRelation = true
        Filter(cond, CarbonToSparkAdapter.createJoinNode(child, targetTable, Inner, None))
      case r@CarbonUnresolvedRelation(t) if !includedDestRelation && t != tableIdentifier =>
        includedDestRelation = true
        CarbonToSparkAdapter.createJoinNode(r, targetTable, Inner, None)
    }
    val updatedSelectPlan: LogicalPlan = if (!includedDestRelation) {
      // special case to handle self join queries
      // Eg. update tableName  SET (column1) = (column1+1)
      selectPlan transform {
        case relation: UnresolvedRelation
          if tableIdentifier == CarbonToSparkAdapter.getTableIdentifier(relation).get &&
             !addedTupleId =>
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
      if (tableIdentifier.database.isDefined) {
        newPlan = parser.parsePlan("select * from  " +
           tableIdentifier.database.getOrElse("") + CarbonCommonConstants.POINT +
           tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      else {
        newPlan = parser.parsePlan("select * from  " +
           tableIdentifier.table + " " + alias.getOrElse("") + " " + filter)
      }
      newPlan transform {
        case CarbonUnresolvedRelation(t)
          if !transformed && t == tableIdentifier =>
          transformed = true

          CarbonReflectionUtils.getSubqueryAlias(
            sparkSession,
            alias,
            updatedSelectPlan,
            Some(tableIdentifier))
      }
    } else {
      updatedSelectPlan
    }
    val destinationTable = CarbonReflectionUtils.getUnresolvedRelation(tableIdentifier, alias)

    // In Spark 2.1 and 2.2, it uses Analyzer.execute method to transform LogicalPlan
    // but in Spark 2.3, it uses Analyzer.executeAndCheck method
    val analyzedPlan = CarbonToSparkAdapter.invokeAnalyzerExecute(
        analyzer, ProjectForUpdate(destinationTable, columns, Seq(finalPlan)))
    // For all commands, they execute eagerly, and will be transformed to
    // logical plan 'LocalRelation' in analyze phase(please see the code in 'Dataset.logicalPlan'),
    // so it needs to return logical plan 'CarbonProjectForUpdateCommand' here
    // instead of 'ProjectForUpdate'
    optimizer.execute(analyzedPlan)
  }


  def processDeleteRecordsQuery(selectStmt: String,
      alias: Option[String],
      table: UnresolvedRelation): LogicalPlan = {
    val tableIdentifier = CarbonToSparkAdapter.getTableIdentifier(table).get
    CarbonPlanHelper.validateCarbonTable(
      tableIdentifier,
      sparkSession,
      "only CarbonData table support delete operation"
    )
    var addedTupleId = false
    val parsePlan = parser.parsePlan(selectStmt)

    val selectPlan = parsePlan transform {
      case relation: UnresolvedRelation
        if tableIdentifier == CarbonToSparkAdapter.getTableIdentifier(relation).get &&
           !addedTupleId =>
        addedTupleId = true
        val projList = projectionWithTupleId(alias)
        val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
        if (carbonTable != null) {
          if (carbonTable.isMV) {
            throw new UnsupportedOperationException(
              "Delete operation is not supported for indexSchema table")
          }
          MVManagerInSpark.disableMVOnTable(sparkSession, carbonTable)
        }
        // include tuple id in subquery
        alias match {
          case Some(_) =>
            val subqueryAlias = CarbonReflectionUtils.getSubqueryAlias(
              sparkSession,
              alias,
              relation,
              Some(tableIdentifier))
            Project(projList, subqueryAlias)
          case _ => Project(projList, relation)
        }
    }
    CarbonProjectForDeleteCommand(
      selectPlan,
      tableIdentifier.database,
      tableIdentifier.table,
      System.currentTimeMillis().toString)
  }

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = {

    logicalPlan transform {
      case spark.sql.UpdateTable(t, cols, sel, alias, where) =>
        processUpdateQuery(t, cols, sel, alias, where)
      case DeleteRecords(statement, alias, table) =>
        processDeleteRecordsQuery(
          statement,
          alias,
          table)
    }
  }
}

case class CarbonLoadDataAnalyzeRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case p: LogicalPlan if !p.childrenResolved => p
      case loadData: LoadDataCommand
        if CarbonPlanHelper.isCarbonTable(loadData.table, sparkSession) =>
        DMLHelper.loadData(loadData)
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

      case p: CarbonToSparkAdapter.InsertIntoStatementWrapper if p.table
        .isInstanceOf[LogicalRelation] && p.table.asInstanceOf[LogicalRelation].relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        // when plan contains Union, it can have multiple insert statements as its children
        castChildOutput(p, p.table.asInstanceOf[LogicalRelation], p.children.head,
          plan.isInstanceOf[Union])
    }
  }

  def castChildOutput(p: CarbonToSparkAdapter.InsertIntoStatementWrapper,
      relation: LogicalRelation,
      child: LogicalPlan,
      containsMultipleInserts: Boolean): LogicalPlan = {
    val carbonDSRelation = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val carbonTable = carbonDSRelation.carbonRelation.carbonTable
    val tableProperties = carbonTable.getTableInfo.getFactTable.getTableProperties
    val spatialProperty = tableProperties.get(CarbonCommonConstants.SPATIAL_INDEX)
    val staticParCols = CarbonToSparkAdapter.getPartitionsFromInsert(p)
      .filter(_._2.isDefined).keySet.map(_.toLowerCase())
    val expectedOutput = carbonDSRelation.carbonRelation.output.filterNot(
      a => staticParCols.contains(a.name.toLowerCase()))
    if (expectedOutput.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      CarbonException.analysisException(
        s"Maximum number of columns supported: " +
          s"${CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS}")
    }
    var newLogicalPlan = child
    if (spatialProperty != null && !spatialProperty.isEmpty &&
        child.output.size + 1 == expectedOutput.size) {
      newLogicalPlan = child.transform {
        // To support insert sql to automatically generate GeoId if customized input is not given.
        case p: Project =>
          val geoId = Alias(Literal(null, NullType).asInstanceOf[Expression], "NULL")()
          val list = Seq(geoId) ++ p.projectList
          Project(list, p.child)
      }
    }
    // In spark, PreprocessTableInsertion rule has below cast logic.
    // It was missed in carbon when implemented insert into rules.
    if (newLogicalPlan.output.size != expectedOutput.size) {
      CarbonException.analysisException(
        s"${carbonTable.getTableName} requires that the data to be inserted " +
        s"have the same number of columns as the target table: " +
        s"target table has ${p.table.output.size} column(s) but the " +
        s"inserted data has ${p.query.output.length + staticParCols.size} column(s), " +
        s"including ${staticParCols.size} partition column(s) having constant value(s)."
      )
    }
    var newChildOutput = newLogicalPlan.output.zip(expectedOutput)
      .map {
        case (actual, expected) =>
          if (expected.dataType.sameType(actual.dataType) &&
              expected.name == actual.name &&
              expected.metadata == actual.metadata) {
            actual
          } else {
            // Renaming is needed for handling the following cases like
            // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
            // 2) Target tables have column metadata
            Alias(Cast(actual, expected.dataType), expected.name)(
              explicitMetadata = Option(expected.metadata))
          }
      }
    newChildOutput = newChildOutput.zipWithIndex.map { columnWithIndex =>
      columnWithIndex._1 match {
        case attr: Attribute =>
          Alias(attr, s"col${ columnWithIndex._2 }")(NamedExpression.newExprId)
        case attr => attr
      }
    }
    val newChild: LogicalPlan = if (newChildOutput == newLogicalPlan.output) {
      throw new UnsupportedOperationException(s"Spark version $SPARK_VERSION is not supported")
    } else {
      Project(newChildOutput, newLogicalPlan)
    }

    val overwrite = CarbonReflectionUtils.getOverWriteOption("overwrite", p)

    InsertIntoCarbonTable(carbonDSRelation, CarbonToSparkAdapter.getPartitionsFromInsert(p),
      newChild, overwrite, ifNotExists = true, containsMultipleInserts = containsMultipleInserts)
  }
}

