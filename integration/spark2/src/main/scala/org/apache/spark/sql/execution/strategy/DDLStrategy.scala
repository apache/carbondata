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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{AlterTableCompactionCommand, CarbonShowLoadsCommand, LoadTableByInsertCommand, LoadTableCommand}
import org.apache.spark.sql.execution.command.partition.ShowCarbonPartitionsCommand
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand}

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Carbon strategies for ddl commands
 */
class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LoadDataCommand(identifier, path, isLocal, isOverwrite, partition)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          LoadTableCommand(
            identifier.database,
            identifier.table.toLowerCase,
            path,
            Seq(),
            Map(),
            isOverwrite)) :: Nil
      case alter@AlterTableRenameCommand(oldTableIdentifier, newTableIdentifier, _) =>
        val dbOption = oldTableIdentifier.database.map(_.toLowerCase)
        val tableIdentifier = TableIdentifier(oldTableIdentifier.table.toLowerCase(), dbOption)
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableIdentifier)(sparkSession)
        if (isCarbonTable) {
          val renameModel = AlterTableRenameModel(tableIdentifier, newTableIdentifier)
          ExecutedCommandExec(CarbonAlterTableRenameCommand(renameModel)) :: Nil
        } else {
          ExecutedCommandExec(alter) :: Nil
        }
      case DropTableCommand(identifier, ifNotExists, isView, _)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .isTablePathExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database,
            identifier.table.toLowerCase)) :: Nil
      case ShowLoadsCommand(databaseName, table, limit) =>
        ExecutedCommandExec(
          CarbonShowLoadsCommand(
            databaseName,
            table.toLowerCase,
            limit,
            plan.output)) :: Nil
      case InsertIntoCarbonTable(relation: CarbonDatasourceHadoopRelation,
      _, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(LoadTableByInsertCommand(relation, child, overwrite.enabled)) :: Nil
      case createDb@CreateDatabaseCommand(dbName, ifNotExists, _, _, _) =>
        ExecutedCommandExec(createDb) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, isCascade) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      case alterTable@AlterTableCompactionCommand(altertablemodel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(altertablemodel.tableName,
            altertablemodel.dbName))(sparkSession)
        if (isCarbonTable) {
          if (altertablemodel.compactionType.equalsIgnoreCase("minor") ||
              altertablemodel.compactionType.equalsIgnoreCase("major") ||
              altertablemodel.compactionType.equalsIgnoreCase("SEGMENT_INDEX_COMPACTION")) {
            ExecutedCommandExec(alterTable) :: Nil
          } else {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on carbon table")
          }
        } else {
          throw new MalformedCarbonCommandException(
            "Operation not allowed : " + altertablemodel.alterSql)
        }
      case dataTypeChange@CarbonAlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(alterTableChangeDataTypeModel.tableName,
            alterTableChangeDataTypeModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(dataTypeChange) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case addColumn@CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(alterTableAddColumnsModel.tableName,
            alterTableAddColumnsModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(addColumn) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case dropColumn@CarbonAlterTableDropColumnCommand(alterTableDropColumnModel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(alterTableDropColumnModel.tableName,
            alterTableDropColumnModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(dropColumn) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case desc@DescribeTableCommand(identifier, partitionSpec, isExtended, isFormatted)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(identifier)(sparkSession) && isFormatted =>
        val resolvedTable =
          sparkSession.sessionState.executePlan(UnresolvedRelation(identifier, None)).analyzed
        val resultPlan = sparkSession.sessionState.executePlan(resolvedTable).executedPlan
        ExecutedCommandExec(
          CarbonDescribeFormattedCommand(
            resultPlan,
            plan.output,
            identifier)) :: Nil
      case ShowPartitionsCommand(t, cols) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(t)(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(ShowCarbonPartitionsCommand(t)) :: Nil
        } else {
          ExecutedCommandExec(ShowPartitionsCommand(t, cols)) :: Nil
        }
      case set@SetCommand(kv) =>
        ExecutedCommandExec(CarbonSetCommand(set)) :: Nil
      case reset@ResetCommand =>
        ExecutedCommandExec(CarbonResetCommand()) :: Nil
      case org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, None)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource") =>
        val updatedCatalog =
          CarbonSource.updateCatalogTableWithCarbonSchema(tableDesc, sparkSession)
        val cmd =
          CreateDataSourceTableCommand(updatedCatalog, ignoreIfExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil
      case AlterTableSetPropertiesCommand(tableName, properties, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableName)(sparkSession) => {
        ExecutedCommandExec(AlterTableSetCommand(tableName, properties, isView)) :: Nil
      }
      case AlterTableUnsetPropertiesCommand(tableName, propKeys, ifExists, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableName)(sparkSession) => {
        ExecutedCommandExec(AlterTableUnsetCommand(tableName, propKeys, ifExists, isView)) :: Nil
      }
      case _ => Nil
    }
  }

}
