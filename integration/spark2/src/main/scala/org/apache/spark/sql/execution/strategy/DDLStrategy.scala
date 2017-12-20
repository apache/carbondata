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
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand, CarbonLoadDataCommand, RefreshCarbonTableCommand}
import org.apache.spark.sql.execution.command.partition.{CarbonShowCarbonPartitionsCommand, CarbonStandardAlterTableDropPartition}
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonDescribeFormattedCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand}
import org.apache.spark.sql.CarbonExpressions.{CarbonDescribeTable => DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.RefreshTable
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CarbonReflectionUtils, FileUtils}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.merger.CompactionType
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Carbon strategies for ddl commands
 */

class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  val LOGGER: LogService =
    LogServiceFactory.getLogService(this.getClass.getName)
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LoadDataCommand(identifier, path, isLocal, isOverwrite, partition)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          CarbonLoadDataCommand(
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
      case InsertIntoCarbonTable(relation: CarbonDatasourceHadoopRelation,
      partition, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(CarbonInsertIntoCommand(relation, child, overwrite, partition)) :: Nil
      case createDb@CreateDatabaseCommand(dbName, ifNotExists, _, _, _) =>
        FileUtils.createDatabaseDirectory(dbName, CarbonProperties.getStorePath)
        ExecutedCommandExec(createDb) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, isCascade) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      case alterTable@CarbonAlterTableCompactionCommand(altertablemodel, _) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(altertablemodel.tableName,
            altertablemodel.dbName))(sparkSession)
        if (isCarbonTable) {
          var compactionType: CompactionType = null
          try {
            compactionType = CompactionType.valueOf(altertablemodel.compactionType.toUpperCase)
          } catch {
            case _ =>
              throw new MalformedCarbonCommandException(
                "Unsupported alter operation on carbon table")
          }
          if (CompactionType.MINOR == compactionType ||
              CompactionType.MAJOR == compactionType ||
              CompactionType.SEGMENT_INDEX == compactionType ||
              CompactionType.STREAMING == compactionType) {
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
      case desc@DescribeTableCommand(identifier, partitionSpec, isExtended) =>
        val isFormatted: Boolean = if (sparkSession.version.startsWith("2.1")) {
          CarbonReflectionUtils
            .getDescribeTableFormattedField(desc.asInstanceOf[DescribeTableCommand])
        } else {
          false
        }
        if (CarbonEnv.getInstance(sparkSession).carbonMetastore
              .tableExists(identifier)(sparkSession) && (isExtended || isFormatted)) {
          val resolvedTable =
            sparkSession.sessionState.executePlan(UnresolvedRelation(identifier)).analyzed
          val resultPlan = sparkSession.sessionState.executePlan(resolvedTable).executedPlan
          ExecutedCommandExec(
            CarbonDescribeFormattedCommand(
              resultPlan,
              plan.output,
              identifier)) :: Nil
        } else {
          Nil
        }
      case ShowPartitionsCommand(t, cols) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(t)(sparkSession)
        if (isCarbonTable) {
          val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
            .lookupRelation(t)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
          if (!carbonTable.isHivePartitionTable) {
            ExecutedCommandExec(CarbonShowCarbonPartitionsCommand(t)) :: Nil
          } else {
            ExecutedCommandExec(ShowPartitionsCommand(t, cols)) :: Nil
          }
        } else {
          ExecutedCommandExec(ShowPartitionsCommand(t, cols)) :: Nil
        }
      case adp@AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableName)(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(
            CarbonStandardAlterTableDropPartition(
              tableName,
              specs,
              ifExists,
              purge,
              retainData)) :: Nil
        } else {
          ExecutedCommandExec(adp) :: Nil
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
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if table.provider.get != DDLUtils.HIVE_PROVIDER
           && table.provider.get.equals("org.apache.spark.sql.CarbonSource") =>
        val updatedCatalog = CarbonSource.updateCatalogTableWithCarbonSchema(table, sparkSession)
        val cmd = CreateDataSourceTableCommand(updatedCatalog, ignoreIfExists)
        ExecutedCommandExec(cmd) :: Nil
      case AlterTableSetPropertiesCommand(tableName, properties, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableName)(sparkSession) => {
        // TODO remove this limitation later
        val property = properties.find(_._1.equalsIgnoreCase("streaming"))
        if (property.isDefined) {
          if (!property.get._2.trim.equalsIgnoreCase("true")) {
            throw new MalformedCarbonCommandException(
              "Streaming property can not be changed to 'false' once it is 'true'")
          }
        }
        ExecutedCommandExec(CarbonAlterTableSetCommand(tableName, properties, isView)) :: Nil
      }
      case AlterTableUnsetPropertiesCommand(tableName, propKeys, ifExists, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(tableName)(sparkSession) => {
        // TODO remove this limitation later
        if (propKeys.exists(_.equalsIgnoreCase("streaming"))) {
          throw new MalformedCarbonCommandException(
            "Streaming property can not be removed")
        }
        ExecutedCommandExec(
          CarbonAlterTableUnsetCommand(tableName, propKeys, ifExists, isView)) :: Nil
      }
      case RefreshTable(tableIdentifier) =>
        RefreshCarbonTableCommand(tableIdentifier.database,
          tableIdentifier.table).run(sparkSession)
        ExecutedCommandExec(RefreshTable(tableIdentifier)) :: Nil
      case _ => Nil
    }
  }

}
