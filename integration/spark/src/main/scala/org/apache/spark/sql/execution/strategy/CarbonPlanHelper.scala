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

package org.apache.spark.sql.execution.strategy

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonEnv, CarbonToSparkAdapter, CustomDeterministicExpression, InsertIntoCarbonTable, SparkSession, SQLContext}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{AtomicType, StructField}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil}

object CarbonPlanHelper {

  def insertInto(insertInto: InsertIntoCarbonTable): CarbonInsertIntoCommand = {
    CarbonInsertIntoCommand(
      databaseNameOp = Some(insertInto.table.carbonRelation.databaseName),
      tableName = insertInto.table.carbonRelation.tableName,
      options = scala.collection.immutable
        .Map("fileheader" -> insertInto.table.getTableSchema.get.fields.map(_.name).mkString(",")),
      isOverwriteTable = insertInto.overwrite,
      logicalPlan = insertInto.child,
      tableInfo = insertInto.table.carbonRelation.carbonTable.getTableInfo,
      partition = insertInto.partition.map(entry => (entry._1.toLowerCase, entry._2)))
  }

  def addColumn(
      addColumnCommand: CarbonAlterTableAddColumnCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableAddColumnsModel = addColumnCommand.alterTableAddColumnsModel
    if (isCarbonTable(TableIdentifier(alterTableAddColumnsModel.tableName,
      alterTableAddColumnsModel.databaseName), sparkSession)) {
      requireTransactionalTable(alterTableAddColumnsModel.databaseName,
        alterTableAddColumnsModel.tableName, sparkSession)
      ExecutedCommandExec(addColumnCommand) :: Nil
      // TODO: remove this else if check once the 2.1 version is unsupported by carbon
    } else if (SparkUtil.isSparkVersionXAndAbove("2.2")) {
      val structField = (alterTableAddColumnsModel.dimCols ++ alterTableAddColumnsModel.msrCols)
        .map { f =>
          val structField = StructField(f.column,
            CarbonSparkDataSourceUtil
              .convertCarbonToSparkDataType(DataTypeUtil.valueOf(f.dataType.get)))
          if (StringUtils.isNotEmpty(f.columnComment)) {
            structField.withComment(f.columnComment)
          } else {
            structField
          }
        }
      val identifier = TableIdentifier(
        alterTableAddColumnsModel.tableName,
        alterTableAddColumnsModel.databaseName)
      ExecutedCommandExec(CarbonReflectionUtils
        .invokeAlterTableAddColumn(identifier, structField).asInstanceOf[RunnableCommand]) ::
      Nil
      // TODO: remove this else check once the 2.1 version is unsupported by carbon
    } else {
      throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
    }
  }

  def changeColumn(
      changeColumnCommand: CarbonAlterTableColRenameDataTypeChangeCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val model = changeColumnCommand.alterTableColRenameAndDataTypeChangeModel
    if (isCarbonTable(TableIdentifier(model.tableName, model.databaseName), sparkSession)) {
      requireTransactionalTable(model.databaseName, model.tableName, sparkSession)
      ExecutedCommandExec(changeColumnCommand) :: Nil
    } else {
      throw new MalformedCarbonCommandException(
        String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
          model.tableName,
          model.databaseName.getOrElse("default")))
    }
  }

  def dropColumn(
      dropColumnCommand: CarbonAlterTableDropColumnCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableDropColumnModel = dropColumnCommand.alterTableDropColumnModel
    requireTransactionalTable(alterTableDropColumnModel.databaseName,
      alterTableDropColumnModel.tableName, sparkSession)
    ExecutedCommandExec(dropColumnCommand) :: Nil
  }

  def compact(
      compactionCommand: CarbonAlterTableCompactionCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableModel = compactionCommand.alterTableModel
    if (isCarbonTable(TableIdentifier(alterTableModel.tableName, alterTableModel.dbName),
      sparkSession)) {
      ExecutedCommandExec(compactionCommand) :: Nil
    } else {
      throw new MalformedCarbonCommandException(
        String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
          alterTableModel.tableName,
          alterTableModel.dbName.getOrElse("default")))
    }
  }

  def isCarbonTable(tableIdent: TableIdentifier): Boolean = {
    isCarbonTable(tableIdent, SparkSQLUtil.getSparkSession)
  }

  def isCarbonTable(tableIdent: TableIdentifier, sparkSession: SparkSession): Boolean = {
    val dbOption = tableIdent.database.map(_.toLowerCase)
    val tableIdentifier = TableIdentifier(tableIdent.table.toLowerCase(), dbOption)
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .tableExists(tableIdentifier)(sparkSession)
  }

  def isTableExists(tableIdent: TableIdentifier, sparkSession: SparkSession): Boolean = {
    val dbOption = tableIdent.database.map(_.toLowerCase)
    val tableIdentifier = TableIdentifier(tableIdent.table.toLowerCase(), dbOption)
    sparkSession.sessionState.catalog.tableExists(tableIdentifier)
  }

  /**
   * only carbon transaction table support: drop column, add column, change column
   */
  def requireTransactionalTable(databaseName: Option[String], tableName: String,
      sparkSession: SparkSession): Unit = {
    val carbonTable = CarbonEnv.getCarbonTable(databaseName, tableName)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external fileformat table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    }
  }

  def validateCarbonTable(
      tableIdentifier: TableIdentifier,
      sparkSession: SparkSession,
      message: String
  ): Unit = {
    if (!CarbonPlanHelper.isTableExists(tableIdentifier, sparkSession)) {
      throw new NoSuchTableException(
        tableIdentifier.database.getOrElse(
          CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession)),
        tableIdentifier.table)
    }
    if (!CarbonPlanHelper.isCarbonTable(tableIdentifier, sparkSession)) {
      throw new UnsupportedOperationException(message)
    }
  }

  /**
   * get deterministic expression for NamedExpression
   */
  private def makeDeterministicExp(exp: NamedExpression): Expression = {
    exp match {
      case alias: Alias if alias.child.isInstanceOf[CustomDeterministicExpression] =>
        alias
      case _ =>
        CustomDeterministicExpression(exp)
    }
  }

  /**
   * Convert all Expression to deterministic Expression
   */
  def makeDeterministic(plan: LogicalPlan): LogicalPlan = {
    val transformedPlan = plan transform {
      case p@Project(projectList: Seq[NamedExpression], cd) =>
        if (cd.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Filter] ||
          cd.isInstanceOf[LogicalRelation]) {
          p.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              if (SparkUtil.isSparkVersionXAndAbove("3")) {
                // create custom deterministic expression for Rand function
                a.transform {
                  case rand: Rand =>
                    CustomDeterministicExpression(rand)
                }
              } else {
                CarbonToSparkAdapter.createAliasRef(
                  CustomDeterministicExpression(exp),
                  a.name,
                  a.exprId,
                  a.qualifier,
                  a.explicitMetadata)
              }
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              makeDeterministicExp(exp)
          }
        } else {
          p
        }
      case f@org.apache.spark.sql.catalyst.plans.logical.Filter(condition: Expression, cd) =>
        if (cd.isInstanceOf[Project] || cd.isInstanceOf[LogicalRelation]) {
          f.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              if (SparkUtil.isSparkVersionXAndAbove("3")) {
                // create custom deterministic expression for Rand function
                a.transform {
                  case rand: Rand =>
                    CustomDeterministicExpression(rand)
                }
              } else {
                CarbonToSparkAdapter.createAliasRef(
                  CustomDeterministicExpression(exp),
                  a.name,
                  a.exprId,
                  a.qualifier,
                  a.explicitMetadata)
              }
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              makeDeterministicExp(exp)
          }
        } else {
          f
        }
    }
    transformedPlan
  }

  def vectorReaderEnabled(): Boolean = {
    val vectorizedReader =
      if (SparkSQLUtil.getSparkSession.conf.contains(CarbonCommonConstants.ENABLE_VECTOR_READER)) {
        SparkSQLUtil.getSparkSession.conf.get(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else if (System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER) != null) {
        System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else {
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
      }
    vectorizedReader.toBoolean
  }

  def supportBatchedDataSource(sqlContext: SQLContext,
      cols: Seq[Attribute],
      extraRDD: Option[(RDD[InternalRow], Boolean)]): Boolean = {
    vectorReaderEnabled() &&
      extraRDD.getOrElse((null, true))._2 &&
      sqlContext.conf.wholeStageEnabled &&
      sqlContext.conf.wholeStageMaxNumFields >= cols.size &&
      cols.forall(_.dataType.isInstanceOf[AtomicType])
  }
}
