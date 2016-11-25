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
import org.apache.spark.sql.catalyst.CarbonTableIdentifierImplicit._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand}
import org.apache.spark.sql.hive.execution.{DropTable, HiveNativeCommand}
import org.apache.spark.sql.hive.execution.command._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class CarbonStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonLateDecodeStrategy
    total
  }

  /**
   * Carbon strategies for performing late decode (converting dictionary key to value
   * as late as possible)
   */
  private[sql] object CarbonLateDecodeStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
          CarbonDictionaryDecoder(relations,
            profile,
            aliasMap,
            planLater(child))(sqlContext) :: Nil
        case _ =>
          Nil
      }
    }
  }

  object DDLStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case DropTable(tableName, ifNotExists)
        if CarbonEnv.getInstance(sqlContext).carbonCatalog
            .isTablePathExists(toTableIdentifier(tableName.toLowerCase))(sqlContext) =>
        val identifier = toTableIdentifier(tableName.toLowerCase)
        ExecutedCommand(DropTableCommand(ifNotExists, identifier.database, identifier.table)) :: Nil
      case ShowLoadsCommand(databaseName, table, limit) =>
        ExecutedCommand(ShowLoads(databaseName, table, limit, plan.output)) :: Nil
      case LoadTable(databaseNameOp, tableName, factPathFromUser, dimFilesPath,
      options, isOverwriteExist, inputSqlString, dataFrame) =>
        val isCarbonTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(TableIdentifier(tableName, databaseNameOp))(sqlContext)
        if (isCarbonTable || options.nonEmpty) {
          ExecutedCommand(LoadTable(databaseNameOp, tableName, factPathFromUser, dimFilesPath,
            options, isOverwriteExist, inputSqlString, dataFrame)) :: Nil
        } else {
          ExecutedCommand(HiveNativeCommand(inputSqlString)) :: Nil
        }
      case alterTable@AlterTableCompaction(altertablemodel) =>
        val isCarbonTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(TableIdentifier(altertablemodel.tableName,
                 altertablemodel.dbName))(sqlContext)
        if (isCarbonTable) {
          if (altertablemodel.compactionType.equalsIgnoreCase("minor") ||
              altertablemodel.compactionType.equalsIgnoreCase("major")) {
            ExecutedCommand(alterTable) :: Nil
          } else {
            throw new MalformedCarbonCommandException(
                "Unsupported alter operation on carbon table")
          }
        } else {
          ExecutedCommand(HiveNativeCommand(altertablemodel.alterSql)) :: Nil
        }
      case CreateDatabase(dbName, sql) =>
        ExecutedCommand(CreateDatabaseCommand(dbName, HiveNativeCommand(sql))) :: Nil
      case DropDatabase(dbName, isCascade, sql) =>
        if (isCascade) {
          ExecutedCommand(DropDatabaseCascadeCommand(dbName, HiveNativeCommand(sql))) :: Nil
        } else {
          ExecutedCommand(DropDatabaseCommand(dbName, HiveNativeCommand(sql))) :: Nil
        }
      case d: HiveNativeCommand =>
        try {
          val resolvedTable = sqlContext.executePlan(CarbonHiveSyntax.parse(d.sql)).optimizedPlan
          planLater(resolvedTable) :: Nil
        } catch {
          case ce: MalformedCarbonCommandException =>
            throw ce
          case ae: AnalysisException =>
            throw ae
          case e: Exception => ExecutedCommand(d) :: Nil
        }
      case DescribeFormattedCommand(sql, tblIdentifier) =>
        val isTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
            .tableExists(tblIdentifier)(sqlContext)
        if (isTable) {
          val describe =
            LogicalDescribeCommand(UnresolvedRelation(tblIdentifier, None), isExtended = false)
          val resolvedTable = sqlContext.executePlan(describe.table).analyzed
          val resultPlan = sqlContext.executePlan(resolvedTable).executedPlan
          ExecutedCommand(DescribeCommandFormatted(resultPlan, plan.output, tblIdentifier)) :: Nil
        } else {
          ExecutedCommand(HiveNativeCommand(sql)) :: Nil
        }
      case _ =>
        Nil
    }

    def toTableIdentifier(name: String): TableIdentifier = {
      val identifier = name.split("\\.")
      identifier match {
        case Array(tableName) => TableIdentifier(tableName, None)
        case Array(dbName, tableName) => TableIdentifier(tableName, Some(dbName))
      }
    }
  }

}

object CarbonHiveSyntax {

  @transient
  protected val sqlParser = new CarbonSqlParser

  def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}
