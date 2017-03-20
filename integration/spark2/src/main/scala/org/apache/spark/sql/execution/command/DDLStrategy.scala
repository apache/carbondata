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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{CarbonEnv, ShowLoadsCommand, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.hive.execution.command.CarbonDropDatabaseCommand

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Carbon strategies for ddl commands
 */
class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LoadDataCommand(identifier, path, isLocal, isOverwrite, partition)
        if CarbonEnv.get.carbonMetastore.tableExists(identifier)(sparkSession) =>
        ExecutedCommandExec(LoadTable(identifier.database, identifier.table.toLowerCase, path,
          Seq(), Map(), isOverwrite)) :: Nil
      case alter@AlterTableRenameCommand(oldTableIdentifier, newTableIdentifier, _) =>
        val isCarbonTable = CarbonEnv.get.carbonMetastore
          .tableExists(oldTableIdentifier)(
            sparkSession)
        if (isCarbonTable) {
          val renameModel = AlterTableRenameModel(oldTableIdentifier, newTableIdentifier)
          ExecutedCommandExec(AlterTableRenameTable(renameModel)) :: Nil
        } else {
          ExecutedCommandExec(alter) :: Nil
        }
      case DropTableCommand(identifier, ifNotExists, isView, _)
        if CarbonEnv.get.carbonMetastore
          .isTablePathExists(identifier)(sparkSession) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database,
            identifier.table.toLowerCase)) :: Nil
      case ShowLoadsCommand(databaseName, table, limit) =>
        ExecutedCommandExec(ShowLoads(databaseName, table.toLowerCase, limit, plan.output)) :: Nil
      case createDb@CreateDatabaseCommand(dbName, ifNotExists, _, _, _) =>
        CarbonEnv.get.carbonMetastore.createDatabaseDirectory(dbName)
        ExecutedCommandExec(createDb) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, isCascade) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      case alterTable@AlterTableCompaction(altertablemodel) =>
        val isCarbonTable = CarbonEnv.get.carbonMetastore
          .tableExists(TableIdentifier(altertablemodel.tableName,
            altertablemodel.dbName))(sparkSession)
        if (isCarbonTable) {
          if (altertablemodel.compactionType.equalsIgnoreCase("minor") ||
              altertablemodel.compactionType.equalsIgnoreCase("major")) {
            ExecutedCommandExec(alterTable) :: Nil
          } else {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on carbon table")
          }
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case dataTypeChange@AlterTableDataTypeChange(alterTableChangeDataTypeModel) =>
        val isCarbonTable = CarbonEnv.get.carbonMetastore
          .tableExists(TableIdentifier(alterTableChangeDataTypeModel.tableName,
            alterTableChangeDataTypeModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(dataTypeChange) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case addColumn@AlterTableAddColumns(alterTableAddColumnsModel) =>
        val isCarbonTable = CarbonEnv.get.carbonMetastore
          .tableExists(TableIdentifier(alterTableAddColumnsModel.tableName,
            alterTableAddColumnsModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(addColumn) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case dropColumn@AlterTableDropColumns(alterTableDropColumnModel) =>
        val isCarbonTable = CarbonEnv.get.carbonMetastore
          .tableExists(TableIdentifier(alterTableDropColumnModel.tableName,
            alterTableDropColumnModel.databaseName))(sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(dropColumn) :: Nil
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case desc@DescribeTableCommand(identifier, partitionSpec, isExtended, isFormatted)
        if CarbonEnv.get.carbonMetastore.tableExists(identifier)(sparkSession) && isFormatted =>
        val resolvedTable =
          sparkSession.sessionState.executePlan(UnresolvedRelation(identifier, None)).analyzed
        val resultPlan = sparkSession.sessionState.executePlan(resolvedTable).executedPlan
        ExecutedCommandExec(DescribeCommandFormatted(resultPlan, plan.output, identifier)) :: Nil
      case _ => Nil
    }
  }

}
