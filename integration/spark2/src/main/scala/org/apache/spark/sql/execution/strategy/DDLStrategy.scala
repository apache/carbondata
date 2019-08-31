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
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand, CarbonLoadDataCommand, RefreshCarbonTableCommand}
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand, CarbonShowCarbonPartitionsCommand}
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonCreateDataSourceTableCommand, CarbonCreateTableLikeCommand, CarbonDescribeFormattedCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand}
import org.apache.spark.sql.CarbonExpressions.{CarbonDescribeTable => DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.{RefreshResource, RefreshTable}
import org.apache.spark.sql.hive.{CarbonRelation, CreateCarbonSourceTableAsSelectCommand}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.{CarbonReflectionUtils, DataMapUtil, FileUtils, SparkUtil}
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.util.Util

  /**
   * Carbon strategies for ddl commands
   * CreateDataSourceTableAsSelectCommand class has extra argument in
   * 2.3, so need to add wrapper to match the case
   */
object MatchCreateDataSourceTable {
  def unapply(plan: LogicalPlan): Option[(CatalogTable, SaveMode, LogicalPlan)] = plan match {
    case t: CreateDataSourceTableAsSelectCommand => Some(t.table, t.mode, t.query)
    case _ => None
  }
}

class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LoadDataCommand(identifier, path, isLocal, isOverwrite, partition)
        if CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          CarbonLoadDataCommand(
            databaseNameOp = identifier.database,
            tableName = identifier.table.toLowerCase,
            factPathFromUser = path,
            dimFilesPath = Seq(),
            options = Map(),
            isOverwriteTable = isOverwrite,
            inputSqlString = null,
            dataFrame = None,
            updateModel = None,
            tableInfoOp = None,
            internalOptions = Map.empty,
            partition = partition.getOrElse(Map.empty).map { case (col, value) =>
              (col, Some(value))})) :: Nil
      case alter@AlterTableRenameCommand(oldTableIdentifier, newTableIdentifier, _) =>
        val dbOption = oldTableIdentifier.database.map(_.toLowerCase)
        val tableIdentifier = TableIdentifier(oldTableIdentifier.table.toLowerCase(), dbOption)
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableIdentifier)(sparkSession)
        if (isCarbonTable) {
          val renameModel = AlterTableRenameModel(tableIdentifier, newTableIdentifier)
          ExecutedCommandExec(CarbonAlterTableRenameCommand(renameModel)) :: Nil
        } else {
          ExecutedCommandExec(alter) :: Nil
        }
      case DropTableCommand(identifier, ifNotExists, isView, _)
        if CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .isTablePathExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database,
            identifier.table.toLowerCase)) :: Nil
      case createLikeTable: CreateTableLikeCommand =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(createLikeTable.sourceTable)(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(CarbonCreateTableLikeCommand(createLikeTable.sourceTable,
            createLikeTable.targetTable, createLikeTable.ifNotExists)) :: Nil
        } else {
          ExecutedCommandExec(createLikeTable) :: Nil
        }
      case InsertIntoCarbonTable(relation: CarbonDatasourceHadoopRelation,
      partition, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(CarbonInsertIntoCommand(relation, child, overwrite, partition)) :: Nil
      case createDb@CreateDatabaseCommand(dbName, ifNotExists, _, _, _) =>
        val dbLocation = try {
          CarbonEnv.getDatabaseLocation(dbName, sparkSession)
        } catch {
          case e: NoSuchDatabaseException =>
            CarbonProperties.getStorePath
        }
        ThreadLocalSessionInfo
          .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
        FileUtils.createDatabaseDirectory(dbName, dbLocation, sparkSession.sparkContext)
        ExecutedCommandExec(createDb) :: Nil
      case drop@DropDatabaseCommand(dbName,
      ifExists, isCascade) if CarbonEnv.databaseLocationExists(dbName, sparkSession, ifExists) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      case alterTable@CarbonAlterTableCompactionCommand(altertablemodel, _, _) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(altertablemodel.tableName,
            altertablemodel.dbName))(sparkSession)
        if (isCarbonTable) {
            ExecutedCommandExec(alterTable) :: Nil
        } else {
          throw new MalformedCarbonCommandException(
            String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
            altertablemodel.tableName,
            altertablemodel.dbName.getOrElse("default")))
        }
      case colRenameDataTypeChange@CarbonAlterTableColRenameDataTypeChangeCommand(
      alterTableColRenameAndDataTypeChangeModel, _) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(alterTableColRenameAndDataTypeChangeModel.tableName,
            alterTableColRenameAndDataTypeChangeModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          val carbonTable = CarbonEnv
            .getCarbonTable(alterTableColRenameAndDataTypeChangeModel.databaseName,
              alterTableColRenameAndDataTypeChangeModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(colRenameDataTypeChange) :: Nil
          }
        } else {
          throw new MalformedCarbonCommandException(
            String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
              alterTableColRenameAndDataTypeChangeModel.tableName,
              alterTableColRenameAndDataTypeChangeModel.
                databaseName.getOrElse("default")))
        }
      case addColumn@CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(alterTableAddColumnsModel.tableName,
            alterTableAddColumnsModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          val carbonTable = CarbonEnv.getCarbonTable(alterTableAddColumnsModel.databaseName,
            alterTableAddColumnsModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(addColumn) :: Nil
          }
          // TODO: remove this else if check once the 2.1 version is unsupported by carbon
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          val structField = (alterTableAddColumnsModel.dimCols ++ alterTableAddColumnsModel.msrCols)
            .map {
              a =>
                StructField(a.column,
                  Util.convertCarbonToSparkDataType(DataTypeUtil.valueOf(a.dataType.get)))
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
      case dropColumn@CarbonAlterTableDropColumnCommand(alterTableDropColumnModel) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(alterTableDropColumnModel.tableName,
            alterTableDropColumnModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          val carbonTable = CarbonEnv.getCarbonTable(alterTableDropColumnModel.databaseName,
            alterTableDropColumnModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(dropColumn) :: Nil
          }
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
        if (CarbonEnv.getInstance(sparkSession).carbonMetaStore
              .tableExists(identifier)(sparkSession) && (isExtended || isFormatted)) {
          val resolvedTable =
            sparkSession.sessionState.executePlan(UnresolvedRelation(identifier)).analyzed
          val resultPlan = sparkSession.sessionState.executePlan(resolvedTable).executedPlan
          ExecutedCommandExec(
            CarbonDescribeFormattedCommand(
              resultPlan,
              plan.output,
              partitionSpec,
              identifier)) :: Nil
        } else {
          Nil
        }
      case ShowPartitionsCommand(t, cols) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(t)(sparkSession)
        if (isCarbonTable) {
          val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .lookupRelation(t)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
          if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          }
          if (!carbonTable.isHivePartitionTable) {
            ExecutedCommandExec(CarbonShowCarbonPartitionsCommand(t)) :: Nil
          } else {
            ExecutedCommandExec(ShowPartitionsCommand(t, cols)) :: Nil
          }
        } else {
          ExecutedCommandExec(ShowPartitionsCommand(t, cols)) :: Nil
        }
      case adp@AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableName)(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(
            CarbonAlterTableDropHivePartitionCommand(
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
          && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog =
          CarbonSource.updateCatalogTableWithCarbonSchema(tableDesc, sparkSession)
        val cmd =
          CreateDataSourceTableCommand(updatedCatalog, ignoreIfExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil
      case MatchCreateDataSourceTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(tableDesc, sparkSession, Option(query))
        val cmd = CreateCarbonSourceTableAsSelectCommand(updatedCatalog, SaveMode.Ignore, query)
        ExecutedCommandExec(cmd) :: Nil
      case cmd@org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(tableDesc, sparkSession, query)
        val cmd = CreateCarbonSourceTableAsSelectCommand(updatedCatalog, SaveMode.Ignore, query.get)
        ExecutedCommandExec(cmd) :: Nil
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if table.provider.get != DDLUtils.HIVE_PROVIDER
          && (table.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || table.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(table, sparkSession)
        val cmd = new CarbonCreateDataSourceTableCommand(updatedCatalog, ignoreIfExists)
        ExecutedCommandExec(cmd) :: Nil
      case AlterTableSetPropertiesCommand(tableName, properties, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableName)(sparkSession) => {

        val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(tableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
        if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
          throw new MalformedCarbonCommandException(
            "Unsupported operation on non transactional table")
        }

        // TODO remove this limitation later
        val property = properties.find(_._1.equalsIgnoreCase("streaming"))
        if (property.isDefined) {
          if (carbonTable.getTablePath.startsWith("s3") && property.get._2.equalsIgnoreCase("s3")) {
            throw new UnsupportedOperationException("streaming is not supported with s3 store")
          }
          if (carbonTable.isStreamingSink) {
            throw new MalformedCarbonCommandException(
              "Streaming property can not be changed once it is 'true'")
          } else {
            if (!property.get._2.trim.equalsIgnoreCase("true")) {
              throw new MalformedCarbonCommandException(
                "Streaming property value is incorrect")
            }
            if (DataMapUtil.hasMVDataMap(carbonTable)) {
              throw new MalformedCarbonCommandException(
                "The table which has MV datamap does not support set streaming property")
            }
            if (carbonTable.isChildTable) {
              throw new MalformedCarbonCommandException(
                "Datamap table does not support set streaming property")
            }
          }
        }
        ExecutedCommandExec(CarbonAlterTableSetCommand(tableName, properties, isView)) :: Nil
      }
      case AlterTableUnsetPropertiesCommand(tableName, propKeys, ifExists, isView)
        if CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableName)(sparkSession) => {
        // TODO remove this limitation later
        if (propKeys.exists(_.equalsIgnoreCase("streaming"))) {
          throw new MalformedCarbonCommandException(
            "Streaming property can not be removed")
        }
        ExecutedCommandExec(
          CarbonAlterTableUnsetCommand(tableName, propKeys, ifExists, isView)) :: Nil
      }
      case rename@AlterTableRenamePartitionCommand(tableName, oldPartition, newPartition) =>
        val dbOption = tableName.database.map(_.toLowerCase)
        val tableIdentifier = TableIdentifier(tableName.table.toLowerCase(), dbOption)
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableIdentifier)(sparkSession)
        if (isCarbonTable) {
          throw new UnsupportedOperationException("Renaming partition on table is not supported")
        } else {
          ExecutedCommandExec(rename) :: Nil
        }
      case addPart@AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists) =>
        val dbOption = tableName.database.map(_.toLowerCase)
        val tableIdentifier = TableIdentifier(tableName.table.toLowerCase(), dbOption)
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableIdentifier)(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(
            CarbonAlterTableAddHivePartitionCommand(
              tableName,
              partitionSpecsAndLocs,
              ifNotExists)) :: Nil
        } else {
          ExecutedCommandExec(addPart) :: Nil
        }
      case RefreshTable(tableIdentifier) =>
        RefreshCarbonTableCommand(tableIdentifier.database,
          tableIdentifier.table).run(sparkSession)
        ExecutedCommandExec(RefreshTable(tableIdentifier)) :: Nil
      case refresh@RefreshResource(path : String) =>
        val plan = try {
          new CarbonSpark2SqlParser().parse(s"REFRESH $path")
        } catch {
          case e: Exception =>
            LOGGER.error(e.getMessage)
            refresh
        }
        ExecutedCommandExec(plan.asInstanceOf[RunnableCommand]) :: Nil
      case alterSetLoc@AlterTableSetLocationCommand(tableName, _, _) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(tableName)(sparkSession)
        if (isCarbonTable) {
          throw new UnsupportedOperationException("Set partition location is not supported")
        } else {
          ExecutedCommandExec(alterSetLoc) :: Nil
        }
      case _ => Nil
    }
  }

}
